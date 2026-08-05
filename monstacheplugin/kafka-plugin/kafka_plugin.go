// Package main implements a Monstache plugin that synchronizes MongoDB data to both
// Elasticsearch and Kafka, with support for custom document transformation and enrichment.
//
// Usage:
//   1. Customize collection names, field filtering, and enrichment logic below
//   2. Configure MongoDB/Elasticsearch/Kafka connection via environment variables
//   3. Build: go build -buildmode=plugin -o kafka_plugin.so kafka_plugin.go
//   4. Use with Monstache: mapper-plugin-path = "./kafka_plugin.so"
//
// Environment Variables:
//   KAFKA_ALLOW_SYNC=true          - Enable Kafka publishing
//   KAFKA_BROKERS=localhost:9092   - Comma-separated Kafka brokers
//   KAFKA_TOPIC_COLLECTIONS=col1,col2 - Collections to sync (must match allowed collections)
//
// Architecture:
//   - filterFields(): Whitelist document fields before sync
//   - attachRelatedDocuments(): Join data from related collections (optional)
//   - enrichDocumentWithRelated(): Compute aggregates on related data (optional)
//   - Map(): Entry point - applies transformations and publishes to Kafka/ES
//
// Customization guide: https://docs.monstache.io/v6/advanced
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/rwynn/monstache/v6/monstachemap"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	kafkaWriter    *kafka.Writer
	kafkaAdmin     *kafka.Conn
	kafkaAllowSync = os.Getenv("KAFKA_ALLOW_SYNC") == "true"
	allowedColl    []string
	logger         = log.New(os.Stdout, "[KafkaPlugin] ", log.LstdFlags)

	// Example collections - customize per use case
	primaryCollection = "example_primary"
	relatedCollection = "example_related"
	detailCollection  = "example_detail"

	// Timeout configurations
	defaultQueryTimeout = 5 * time.Second
	kafkaWriteTimeout   = 3 * time.Second
)

type KafkaMessage struct {
	Collection string      `json:"collection"`
	Namespace  string      `json:"namespace"`
	Operation  string      `json:"operation"` // insert, update, delete, replace
	Data       interface{} `json:"data"`
}

// ====================== FIELD LISTS ===============================
// Example field filtering - customize for your collections
// Omit this map to include all fields, or define fields to whitelist
var collectionFields = map[string][]string{
	primaryCollection: {
		"_id",
		"userId",
		"name",
		"email",
		"status",
		"createdAt",
		"updatedAt",
		"metadata.region",
	},
	relatedCollection: {
		"_id",
		"primaryId",
		"action",
		"value",
		"createdAt",
	},
	detailCollection: {
		"_id",
		"primaryId",
		"detail",
		"amount",
		"currency",
		"createdAt",
	},
}

// Example projection for related document queries
var relatedDocProjection = map[string]interface{}{
	"action": 1, "value": 1, "createdAt": 1,
}
var relatedIdProjection = map[string]interface{}{"primaryId": 1}

// ====================== HELPERS ===============================
func getEnvList(key string) []string {
	if v := os.Getenv(key); v != "" {
		return strings.Split(v, ",")
	}
	logger.Fatalf("Missing env: %s", key)
	return nil
}

func newKafkaWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 20 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
}

func buildTopic(col string) string { return "monstache." + col }

func getStringSlice(v interface{}) []string {
	switch arr := v.(type) {
	case []string:
		return arr
	case []interface{}:
		out := make([]string, 0, len(arr))
		for _, e := range arr {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func parseMonth(v interface{}) string {
	switch t := v.(type) {
	case time.Time:
		return t.Format("200601")
	case string:
		if tm, err := time.Parse(time.RFC3339, t); err == nil {
			return tm.Format("200601")
		}
	}
	return time.Now().Format("200601")
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	case primitive.Decimal128:
		var f float64
		fmt.Sscanf(val.String(), "%f", &f)
		return f
	}
	return 0
}

// ====================== FILTER LOGIC ===============================
func filterFields(doc map[string]interface{}, coll string) map[string]interface{} {
	fields, ok := collectionFields[coll]
	if !ok {
		return doc
	}

	out := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		if !strings.Contains(f, ".") {
			if val, ok := doc[f]; ok {
				out[f] = val
			}
			continue
		}
		parts := strings.SplitN(f, ".", 2)
		if root, ok := doc[parts[0]].(map[string]interface{}); ok {
			if val, ok := root[parts[1]]; ok {
				out[f] = val
			}
		}
	}
	return out
}

// ====================== DOCUMENT ENRICHMENT EXAMPLE ===============================
// Example: attach related documents from another collection
func attachRelatedDocuments(ctx context.Context, input *monstachemap.MapperPluginInput, doc map[string]interface{}, relatedCollName string) []map[string]interface{} {
	if input.MongoClient == nil {
		return nil
	}

	// Get related document IDs - customize field name per use case
	relatedIDs := getStringSlice(doc["relatedIds"])
	if len(relatedIDs) == 0 {
		return nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	cursor, err := input.MongoClient.Database(input.Database).
		Collection(relatedCollName).
		Find(queryCtx,
			map[string]interface{}{"_id": map[string]interface{}{"$in": relatedIDs}},
			options.Find().SetProjection(relatedDocProjection),
		)
	if err != nil {
		logger.Printf("Related query error: %v", err)
		return nil
	}
	defer cursor.Close(queryCtx)

	var results []map[string]interface{}
	if err := cursor.All(queryCtx, &results); err != nil {
		logger.Printf("Related decode error: %v", err)
		return nil
	}

	return results
}

// Example: enrich document with aggregated data from related documents
func enrichDocumentWithRelated(doc map[string]interface{}, related []map[string]interface{}) {
	if len(related) == 0 {
		return
	}

	// Example: aggregate values from related documents
	totalValue := 0.0
	count := 0
	for _, r := range related {
		if val, ok := r["value"].(float64); ok {
			totalValue += val
			count++
		}
	}

	if count > 0 {
		doc["aggregatedValue"] = totalValue
		doc["relatedCount"] = count
	}
}

// ====================== INIT ===============================
func init() {
	brokers := getEnvList("KAFKA_BROKERS")
	allowedColl = getEnvList("KAFKA_TOPIC_COLLECTIONS")

	kafkaWriter = newKafkaWriter(brokers)
	logger.Printf("Kafka plugin init brokers=%v topics=%v", brokers, allowedColl)

	// Kafka Admin
	admin, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		logger.Fatalf("Kafka admin connect failed: %v", err)
	}
	kafkaAdmin = admin

	// auto-create topics
	for _, col := range allowedColl {
		topic := buildTopic(col)
		if parts, _ := admin.ReadPartitions(topic); len(parts) == 0 {
			if err := admin.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1}); err != nil {
				logger.Fatalf("Create topic failed: %v", err)
			}
			logger.Println("Created topic:", topic)
		}
	}
}

// ====================== MAP PLUGIN ===============================
func Map(input *monstachemap.MapperPluginInput) (output *monstachemap.MapperPluginOutput, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Printf("Plugin Map panic recovered: %v", r)
			err = fmt.Errorf("panic in Plugin Map: %v", r)
			output = nil
		}
	}()

	if kafkaWriter == nil || !slices.Contains(allowedColl, input.Collection) {
		return nil, nil
	}

	// Example: skip documents matching certain criteria - customize per use case
	// if skipFlag, ok := input.Document["skipSync"].(bool); ok && skipFlag {
	// 	return nil, nil
	// }

	ctx, cancel := context.WithTimeout(context.Background(), defaultQueryTimeout)
	defer cancel()

	doc := filterFields(input.Document, input.Collection)

	// Time-based index (monthly) - customize naming per use case
	index := fmt.Sprintf("%s-%s", strings.ReplaceAll(input.Collection, "_", "-"), parseMonth(doc["createdAt"]))

	// Example: enrich primary collection with related data
	if input.Collection == primaryCollection {
		related := attachRelatedDocuments(ctx, input, doc, relatedCollection)
		enrichDocumentWithRelated(doc, related)
	}

	if kafkaAllowSync {
		msg, marshalErr := json.Marshal(KafkaMessage{
			Collection: input.Collection,
			Namespace:  input.Namespace,
			Operation:  input.Operation,
			Data:       doc,
		})
		if marshalErr != nil {
			logger.Printf("JSON marshal error: %v", marshalErr)
			return nil, fmt.Errorf("failed to marshal kafka message: %w", marshalErr)
		}

		kafkaCtx, kafkaCancel := context.WithTimeout(context.Background(), kafkaWriteTimeout)
		defer kafkaCancel()

		// Handle Kafka write error
		if writeErr := kafkaWriter.WriteMessages(kafkaCtx, kafka.Message{
			Key:   []byte(input.Document["_id"].(primitive.ObjectID).Hex()),
			Value: msg,
			Topic: buildTopic(input.Collection),
		}); writeErr != nil {
			logger.Printf("Kafka write error for %s/%s: %v", input.Collection, input.Document["_id"], writeErr)
			// Option 1: Return error to let Monstache retry
			return nil, fmt.Errorf("kafka write failed: %w", writeErr)
			// Option 2: Log and continue (uncomment if want ES sync even if Kafka fails)
			// logger.Printf("WARNING: Kafka write failed but continuing ES sync: %v", writeErr)
		}
	}

	// Use updatedAt as external version for ES ordering
	var version int64
	if updatedAt, ok := doc["updatedAt"].(primitive.DateTime); ok {
		version = int64(updatedAt)
	} else if updatedAt, ok := doc["updatedAt"].(time.Time); ok {
		version = updatedAt.UnixMilli()
	}

	output = &monstachemap.MapperPluginOutput{
		Document: doc,
		Index:    index,
	}

	// Only set version if we have a valid timestamp
	if version > 0 {
		output.Version = version
		output.VersionType = "external_gte"
	}

	return output, nil
}

func OnShutdown() {
	if kafkaWriter != nil {
		_ = kafkaWriter.Close()
		logger.Println("Kafka writer closed")
	}
}

func main() {}
