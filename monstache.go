// package main provides the monstache binary
package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/coreos/go-systemd/daemon"
	"github.com/evanphx/json-patch"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"github.com/rwynn/gtm"
	"github.com/rwynn/gtm/consistent"
	"github.com/rwynn/monstache/monstachemap"
	"golang.org/x/net/context"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/natefinch/lumberjack.v2"
	elastic "gopkg.in/olivere/elastic.v5"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"plugin"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var gridByteBuffer bytes.Buffer
var infoLog = log.New(os.Stdout, "INFO ", log.Flags())
var statsLog = log.New(os.Stdout, "STATS ", log.Flags())
var traceLog = log.New(os.Stdout, "TRACE ", log.Flags())
var errorLog = log.New(os.Stderr, "ERROR ", log.Flags())

var mapperPlugin func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error)
var mapEnvs map[string]*executionEnv
var mapIndexTypes map[string]*indexTypeMapping
var fileNamespaces map[string]bool
var patchNamespaces map[string]bool

var chunksRegex = regexp.MustCompile("\\.chunks$")
var systemsRegex = regexp.MustCompile("system\\..+$")
var lastTimestamp bson.MongoTimestamp

const version = "3.2.0"
const mongoURLDefault string = "localhost"
const resumeNameDefault string = "default"
const elasticMaxConnsDefault int = 10
const elasticClientTimeoutDefault int = 60
const elasticMaxDocsDefault int = 1000
const gtmChannelSizeDefault int = 512

type stringargs []string

type executionEnv struct {
	VM      *otto.Otto
	Script  string
	Routing bool
}

type javascript struct {
	Namespace string
	Script    string
	Routing   bool
}

type indexTypeMapping struct {
	Namespace string
	Index     string
	Type      string
}

type logFiles struct {
	Info  string
	Error string
	Trace string
	Stats string
}

type indexingMeta struct {
	Routing string
	Index   string
	Type    string
}

type mongoDialSettings struct {
	Timeout int
	Ssl     bool
}

type mongoSessionSettings struct {
	SocketTimeout int `toml:"socket-timeout"`
	SyncTimeout   int `toml:"sync-timeout"`
}

type gtmSettings struct {
	ChannelSize    int    `toml:"channel-size"`
	BufferSize     int    `toml:"buffer-size"`
	BufferDuration string `toml:"buffer-duration"`
}

type configOptions struct {
	MongoURL                 string               `toml:"mongo-url"`
	MongoPemFile             string               `toml:"mongo-pem-file"`
	MongoValidatePemFile     bool                 `toml:"mongo-validate-pem-file"`
	MongoOpLogDatabaseName   string               `toml:"mongo-oplog-database-name"`
	MongoOpLogCollectionName string               `toml:"mongo-oplog-collection-name"`
	MongoCursorTimeout       string               `toml:"mongo-cursor-timeout"`
	MongoDialSettings        mongoDialSettings    `toml:"mongo-dial-settings"`
	MongoSessionSettings     mongoSessionSettings `toml:"mongo-session-settings"`
	GtmSettings              gtmSettings          `toml:"gtm-settings"`
	Logs                     logFiles             `toml:"logs"`
	ElasticUrls              stringargs           `toml:"elasticsearch-urls"`
	ElasticUser              string               `toml:"elasticsearch-user"`
	ElasticPassword          string               `toml:"elasticsearch-password"`
	ElasticPemFile           string               `toml:"elasticsearch-pem-file"`
	ElasticValidatePemFile   bool                 `toml:"elasticsearch-validate-pem-file"`
	ElasticVersion           string               `toml:"elasticsearch-version"`
	ResumeName               string               `toml:"resume-name"`
	NsRegex                  string               `toml:"namespace-regex"`
	NsExcludeRegex           string               `toml:"namespace-exclude-regex"`
	ClusterName              string               `toml:"cluster-name"`
	Print                    bool                 `toml:"print-config"`
	Version                  bool
	Stats                    bool
	IndexStats               bool   `toml:"index-stats"`
	StatsDuration            string `toml:"stats-duration"`
	Gzip                     bool
	Verbose                  bool
	Resume                   bool
	ResumeWriteUnsafe        bool  `toml:"resume-write-unsafe"`
	ResumeFromTimestamp      int64 `toml:"resume-from-timestamp"`
	Replay                   bool
	DroppedDatabases         bool   `toml:"dropped-databases"`
	DroppedCollections       bool   `toml:"dropped-collections"`
	IndexFiles               bool   `toml:"index-files"`
	FileHighlighting         bool   `toml:"file-highlighting"`
	EnablePatches            bool   `toml:"enable-patches"`
	FailFast                 bool   `toml:"fail-fast"`
	IndexOplogTime           bool   `toml:"index-oplog-time"`
	ExitAfterDirectReads     bool   `toml:"exit-after-direct-reads"`
	MergePatchAttr           string `toml:"merge-patch-attribute"`
	ElasticMaxConns          int    `toml:"elasticsearch-max-conns"`
	ElasticRetry             bool   `toml:"elasticsearch-retry"`
	ElasticMaxDocs           int    `toml:"elasticsearch-max-docs"`
	ElasticMaxBytes          int    `toml:"elasticsearch-max-bytes"`
	ElasticMaxSeconds        int    `toml:"elasticsearch-max-seconds"`
	ElasticClientTimeout     int    `toml:"elasticsearch-client-timeout"`
	ElasticMajorVersion      int
	MaxFileSize              int64 `toml:"max-file-size"`
	ConfigFile               string
	Script                   []javascript
	Mapping                  []indexTypeMapping
	FileNamespaces           stringargs `toml:"file-namespaces"`
	PatchNamespaces          stringargs `toml:"patch-namespaces"`
	Workers                  stringargs
	Worker                   string
	DirectReadNs             stringargs `toml:"direct-read-namespaces"`
	DirectReadLimit          int        `toml:"direct-read-limit"`
	DirectReadBatchSize      int        `toml:"direct-read-batch-size"`
	DirectReadersPerCol      int        `toml:"direct-readers-per-col"`
	MapperPluginPath         string     `toml:"mapper-plugin-path"`
}

func (args *stringargs) String() string {
	return fmt.Sprintf("%s", *args)
}

func (args *stringargs) Set(value string) error {
	*args = append(*args, value)
	return nil
}

func (config *configOptions) parseElasticsearchVersion(number string) (err error) {
	if number == "" {
		err = errors.New("elasticsearch version cannot be blank")
	} else {
		versionParts := strings.Split(number, ".")
		var majorVersion int
		majorVersion, err = strconv.Atoi(versionParts[0])
		if err == nil {
			config.ElasticMajorVersion = majorVersion
			if majorVersion == 0 {
				err = errors.New("invalid elasticsearch major version 0")
			}
		}
	}
	return
}

func (config *configOptions) newBulkProcessor(client *elastic.Client, mongo *mgo.Session) (bulk *elastic.BulkProcessor, err error) {
	bulkService := client.BulkProcessor().Name("monstache")
	bulkService.Workers(config.ElasticMaxConns)
	bulkService.Stats(true)
	if config.ElasticMaxDocs != 0 {
		bulkService.BulkActions(config.ElasticMaxDocs)
	}
	if config.ElasticMaxBytes != 0 {
		bulkService.BulkSize(config.ElasticMaxBytes)
	}
	bulkService.FlushInterval(time.Duration(config.ElasticMaxSeconds) * time.Second)
	bulkService.After(createAfterBulk(mongo, config))
	return bulkService.Do(context.Background())
}

func (config *configOptions) newStatsBulkProcessor(client *elastic.Client) (bulk *elastic.BulkProcessor, err error) {
	bulkService := client.BulkProcessor().Name("monstache-stats")
	bulkService.Workers(1)
	bulkService.Stats(false)
	bulkService.BulkActions(1)
	return bulkService.Do(context.Background())
}

func (config *configOptions) needsSecureScheme() bool {
	if len(config.ElasticUrls) > 0 {
		for _, url := range config.ElasticUrls {
			if strings.HasPrefix(url, "https") {
				return true
			}
		}
	}
	return false

}

func (config *configOptions) newElasticClient() (client *elastic.Client, err error) {
	var clientOptions []elastic.ClientOptionFunc
	var httpClient *http.Client
	clientOptions = append(clientOptions, elastic.SetErrorLog(errorLog))
	clientOptions = append(clientOptions, elastic.SetSniff(false))
	if config.needsSecureScheme() {
		clientOptions = append(clientOptions, elastic.SetScheme("https"))
	}
	if len(config.ElasticUrls) > 0 {
		clientOptions = append(clientOptions, elastic.SetURL(config.ElasticUrls...))
	} else {
		config.ElasticUrls = append(config.ElasticUrls, elastic.DefaultURL)
	}
	if config.Verbose {
		clientOptions = append(clientOptions, elastic.SetTraceLog(traceLog))
	}
	if config.Gzip {
		clientOptions = append(clientOptions, elastic.SetGzip(true))
	}
	if config.ElasticUser != "" {
		clientOptions = append(clientOptions, elastic.SetBasicAuth(config.ElasticUser, config.ElasticPassword))
	}
	if config.ElasticRetry {
		d1, d2 := time.Duration(50)*time.Millisecond, time.Duration(20)*time.Second
		retrier := elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(d1, d2))
		clientOptions = append(clientOptions, elastic.SetRetrier(retrier))
	}
	httpClient, err = config.NewHTTPClient()
	if err != nil {
		return client, err
	}
	clientOptions = append(clientOptions, elastic.SetHttpClient(httpClient))
	return elastic.NewClient(clientOptions...)
}

func (config *configOptions) testElasticsearchConn(client *elastic.Client) (err error) {
	var number string
	url := config.ElasticUrls[0]
	number, err = client.ElasticsearchVersion(url)
	if err == nil {
		if config.Verbose {
			infoLog.Printf("Successfully connected to elasticsearch version %s", number)
		}
		err = config.parseElasticsearchVersion(number)
	}
	return
}

func normalizeIndexName(name string) (normal string) {
	normal = strings.ToLower(strings.TrimPrefix(name, "_"))
	return
}

func normalizeTypeName(name string) (normal string) {
	normal = strings.TrimPrefix(name, "_")
	return
}

func normalizeEsID(id string) (normal string) {
	normal = strings.TrimPrefix(id, "_")
	return
}

func deleteIndexes(client *elastic.Client, db string, config *configOptions) (err error) {
	ctx := context.Background()
	for ns, m := range mapIndexTypes {
		parts := strings.SplitN(ns, ".", 2)
		if parts[0] == db {
			if _, err = client.DeleteIndex(m.Index + "*").Do(ctx); err != nil {
				return
			}
		}
	}
	_, err = client.DeleteIndex(normalizeIndexName(db) + "*").Do(ctx)
	return
}

func deleteIndex(client *elastic.Client, namespace string, config *configOptions) (err error) {
	ctx := context.Background()
	esIndex := normalizeIndexName(namespace)
	if m := mapIndexTypes[namespace]; m != nil {
		esIndex = m.Index
	}
	_, err = client.DeleteIndex(esIndex).Do(ctx)
	return err
}

func ensureFileMapping(client *elastic.Client, namespace string, config *configOptions) (err error) {
	if config.ElasticMajorVersion < 5 {
		return ensureFileMappingMapperAttachment(client, namespace, config)
	}
	return ensureFileMappingIngestAttachment(client, namespace, config)
}

func ensureFileMappingIngestAttachment(client *elastic.Client, namespace string, config *configOptions) (err error) {
	ctx := context.Background()
	pipeline := map[string]interface{}{
		"description": "Extract file information",
		"processors": [1]map[string]interface{}{
			{
				"attachment": map[string]interface{}{
					"field": "file",
				},
			},
		},
	}
	_, err = client.IngestPutPipeline("attachment").BodyJson(pipeline).Do(ctx)
	return err
}

func ensureFileMappingMapperAttachment(conn *elastic.Client, namespace string, config *configOptions) (err error) {
	ctx := context.Background()
	parts := strings.SplitN(namespace, ".", 2)
	esIndex, esType := normalizeIndexName(namespace), normalizeTypeName(parts[1])
	if m := mapIndexTypes[namespace]; m != nil {
		esIndex, esType = m.Index, m.Type
	}
	props := map[string]interface{}{
		"properties": map[string]interface{}{
			"file": map[string]interface{}{
				"type": "attachment",
			},
		},
	}
	file := props["properties"].(map[string]interface{})["file"].(map[string]interface{})
	types := map[string]interface{}{
		esType: props,
	}
	mappings := map[string]interface{}{
		"mappings": types,
	}
	if config.FileHighlighting {
		file["fields"] = map[string]interface{}{
			"content": map[string]interface{}{
				"type":        "string",
				"term_vector": "with_positions_offsets",
				"store":       true,
			},
		}
	}
	var exists bool
	if exists, err = conn.IndexExists(esIndex).Do(ctx); exists {
		_, err = conn.PutMapping().Index(esIndex).Type(esType).BodyJson(types).Do(ctx)
	} else {
		_, err = conn.CreateIndex(esIndex).BodyJson(mappings).Do(ctx)
	}
	return err
}

func defaultIndexTypeMapping(op *gtm.Op) *indexTypeMapping {
	return &indexTypeMapping{
		Namespace: op.Namespace,
		Index:     normalizeIndexName(op.Namespace),
		Type:      normalizeTypeName(op.GetCollection()),
	}
}

func mapIndexType(op *gtm.Op) *indexTypeMapping {
	mapping := defaultIndexTypeMapping(op)
	if mapIndexTypes != nil {
		if m := mapIndexTypes[op.Namespace]; m != nil {
			mapping = m
		}
	}
	return mapping
}

func opIDToString(op *gtm.Op) string {
	var opIDStr string
	switch op.Id.(type) {
	case bson.ObjectId:
		opIDStr = op.Id.(bson.ObjectId).Hex()
	case float64:
		intID := int(op.Id.(float64))
		if op.Id.(float64) == float64(intID) {
			opIDStr = fmt.Sprintf("%v", intID)
		} else {
			opIDStr = fmt.Sprintf("%v", op.Id)
		}
	case float32:
		intID := int(op.Id.(float32))
		if op.Id.(float32) == float32(intID) {
			opIDStr = fmt.Sprintf("%v", intID)
		} else {
			opIDStr = fmt.Sprintf("%v", op.Id)
		}
	default:
		opIDStr = normalizeEsID(fmt.Sprintf("%v", op.Id))
	}
	return opIDStr
}

func mapDataJavascript(op *gtm.Op) error {
	if mapEnvs == nil {
		return nil
	}
	if env := mapEnvs[op.Namespace]; env != nil {
		val, err := env.VM.Call("module.exports", op.Data, op.Data)
		if err != nil {
			return err
		}
		if strings.ToLower(val.Class()) == "object" {
			data, err := val.Export()
			if err != nil {
				return err
			} else if data == val {
				return errors.New("exported function must return an object")
			} else {
				op.Data = data.(map[string]interface{})
			}
		} else {
			indexed, err := val.ToBoolean()
			if err != nil {
				return err
			} else if !indexed {
				op.Data = nil
			}
		}
	}
	return nil
}

func mapDataGolang(op *gtm.Op) error {
	input := &monstachemap.MapperPluginInput{
		Document:   op.Data,
		Namespace:  op.Namespace,
		Database:   op.GetDatabase(),
		Collection: op.GetCollection(),
		Operation:  op.Operation,
	}
	output, err := mapperPlugin(input)
	if err != nil {
		return err
	}
	if output != nil {
		if output.Drop {
			op.Data = nil
		} else {
			if output.Passthrough == false {
				op.Data = output.Document
			}
			var meta map[string]interface{}
			if output.Index != "" {
				meta["index"] = output.Index
			}
			if output.Type != "" {
				meta["type"] = output.Type
			}
			if output.Routing != "" {
				meta["routing"] = output.Routing
			}
			if len(meta) > 0 {
				op.Data["_meta_monstache"] = meta
			}
		}
	}
	return nil
}

func mapData(config *configOptions, op *gtm.Op) error {
	if config.MapperPluginPath != "" {
		return mapDataGolang(op)
	}
	return mapDataJavascript(op)
}

func prepareDataForIndexing(config *configOptions, op *gtm.Op) {
	data := op.Data
	if config.IndexOplogTime {
		secs := int64(op.Timestamp >> 32)
		t := time.Unix(secs, 0).UTC()
		data["_oplog_ts"] = op.Timestamp
		data["_oplog_date"] = t.Format("2006/01/02 15:04:05")
	}
	delete(data, "_id")
	delete(data, "_type")
	delete(data, "_index")
	delete(data, "_score")
	delete(data, "_source")
	delete(data, "_meta_monstache")
}

func parseIndexMeta(data map[string]interface{}) (meta *indexingMeta) {
	meta = &indexingMeta{}
	if m, ok := data["_meta_monstache"]; ok {
		switch m.(type) {
		case map[string]interface{}:
			metaAttrs := m.(map[string]interface{})
			if r, ok := metaAttrs["routing"]; ok {
				meta.Routing = fmt.Sprintf("%v", r)
			}
			if i, ok := metaAttrs["index"]; ok {
				meta.Index = fmt.Sprintf("%v", i)
			}
			if t, ok := metaAttrs["type"]; ok {
				meta.Type = fmt.Sprintf("%v", t)
			}
		case otto.Value:
			ex, err := m.(otto.Value).Export()
			if err == nil && ex != m {
				switch ex.(type) {
				case map[string]interface{}:
					metaAttrs := ex.(map[string]interface{})
					if r, ok := metaAttrs["routing"]; ok {
						meta.Routing = fmt.Sprintf("%v", r)
					}
					if i, ok := metaAttrs["index"]; ok {
						meta.Index = fmt.Sprintf("%v", i)
					}
					if t, ok := metaAttrs["type"]; ok {
						meta.Type = fmt.Sprintf("%v", t)
					}
				default:
					errorLog.Println("invalid indexing metadata")
				}
			}
		default:
			errorLog.Println("invalid indexing metadata")
		}
	}
	return meta
}

func addFileContent(session *mgo.Session, op *gtm.Op, config *configOptions) (err error) {
	op.Data["file"] = ""
	gridByteBuffer.Reset()
	db, bucket :=
		session.DB(op.GetDatabase()),
		strings.SplitN(op.GetCollection(), ".", 2)[0]
	encoder := base64.NewEncoder(base64.StdEncoding, &gridByteBuffer)
	file, err := db.GridFS(bucket).OpenId(op.Id)
	if err != nil {
		return
	}
	defer file.Close()
	if config.MaxFileSize > 0 {
		if file.Size() > config.MaxFileSize {
			infoLog.Printf("file %s md5(%s) exceeds max file size. file content omitted.",
				file.Name(), file.MD5())
			return
		}
	}
	if _, err = io.Copy(encoder, file); err != nil {
		return
	}
	if err = encoder.Close(); err != nil {
		return
	}
	op.Data["file"] = string(gridByteBuffer.Bytes())
	return
}

func notMonstache(op *gtm.Op) bool {
	return op.GetDatabase() != "monstache"
}

func notChunks(op *gtm.Op) bool {
	return !chunksRegex.MatchString(op.GetCollection())
}

func notSystem(op *gtm.Op) bool {
	return !systemsRegex.MatchString(op.GetCollection())
}

func filterWithRegex(regex string) gtm.OpFilter {
	var validNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		return validNameSpace.MatchString(op.Namespace)
	}
}

func filterInverseWithRegex(regex string) gtm.OpFilter {
	var invalidNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		return !invalidNameSpace.MatchString(op.Namespace)
	}
}

func ensureClusterTTL(session *mgo.Session) error {
	col := session.DB("monstache").C("cluster")
	return col.EnsureIndex(mgo.Index{
		Key:         []string{"expireAt"},
		Background:  true,
		ExpireAfter: time.Duration(30) * time.Second,
	})
}

func isEnabledProcess(session *mgo.Session, config *configOptions) (bool, error) {
	col := session.DB("monstache").C("cluster")
	doc := make(map[string]interface{})
	doc["_id"] = config.ResumeName
	doc["expireAt"] = time.Now().UTC()
	doc["pid"] = os.Getpid()
	if host, err := os.Hostname(); err == nil {
		doc["host"] = host
	} else {
		return false, err
	}
	err := col.Insert(doc)
	if err == nil {
		return true, nil
	}
	lastError := err.(*mgo.LastError)
	if lastError.Code == 11000 {
		return false, nil
	}
	return false, err
}

func resetClusterState(session *mgo.Session, config *configOptions) error {
	col := session.DB("monstache").C("cluster")
	return col.RemoveId(config.ResumeName)
}

func isEnabledProcessID(session *mgo.Session, config *configOptions) bool {
	col := session.DB("monstache").C("cluster")
	doc := make(map[string]interface{})
	col.FindId(config.ResumeName).One(doc)
	if doc["pid"] != nil && doc["host"] != nil {
		pid := doc["pid"].(int)
		host := doc["host"].(string)
		hostname, err := os.Hostname()
		if err != nil {
			errorLog.Println(err)
			return false
		}
		enabled := pid == os.Getpid() && host == hostname
		if enabled {
			col.UpdateId(config.ResumeName,
				bson.M{"$set": bson.M{"expireAt": time.Now().UTC()}})
		}
		return enabled
	}
	return false
}

func resumeWork(ctx *gtm.OpCtx, session *mgo.Session, config *configOptions) {
	col := session.DB("monstache").C("monstache")
	doc := make(map[string]interface{})
	col.FindId(config.ResumeName).One(doc)
	if doc["ts"] != nil {
		ts := doc["ts"].(bson.MongoTimestamp)
		ctx.Since(ts)
	}
	ctx.Resume()
}

func saveTimestamp(session *mgo.Session, ts bson.MongoTimestamp, resumeName string) error {
	col := session.DB("monstache").C("monstache")
	doc := make(map[string]interface{})
	doc["ts"] = ts
	_, err := col.UpsertId(resumeName, bson.M{"$set": doc})
	return err
}

func (config *configOptions) parseCommandLineFlags() *configOptions {
	flag.BoolVar(&config.Print, "print-config", false, "Print the configuration and then exit")
	flag.StringVar(&config.MongoURL, "mongo-url", "", "MongoDB connection URL")
	flag.StringVar(&config.MongoPemFile, "mongo-pem-file", "", "Path to a PEM file for secure connections to MongoDB")
	flag.BoolVar(&config.MongoValidatePemFile, "mongo-validate-pem-file", true, "Set to boolean false to not validate the MongoDB PEM file")
	flag.StringVar(&config.MongoOpLogDatabaseName, "mongo-oplog-database-name", "", "Override the database name which contains the mongodb oplog")
	flag.StringVar(&config.MongoOpLogCollectionName, "mongo-oplog-collection-name", "", "Override the collection name which contains the mongodb oplog")
	flag.StringVar(&config.MongoCursorTimeout, "mongo-cursor-timeout", "", "Override the duration before a cursor timeout occurs when tailing the oplog")
	flag.StringVar(&config.ElasticVersion, "elasticsearch-version", "", "Specify elasticsearch version directly instead of getting it from the server")
	flag.StringVar(&config.ElasticUser, "elasticsearch-user", "", "The elasticsearch user name for basic auth")
	flag.StringVar(&config.ElasticPassword, "elasticsearch-password", "", "The elasticsearch password for basic auth")
	flag.StringVar(&config.ElasticPemFile, "elasticsearch-pem-file", "", "Path to a PEM file for secure connections to elasticsearch")
	flag.BoolVar(&config.ElasticValidatePemFile, "elasticsearch-validate-pem-file", true, "Set to boolean false to not validate the Elasticsearch PEM file")
	flag.IntVar(&config.ElasticMaxConns, "elasticsearch-max-conns", 0, "Elasticsearch max connections")
	flag.BoolVar(&config.ElasticRetry, "elasticsearch-retry", false, "True to retry failed request to Elasticsearch")
	flag.IntVar(&config.ElasticMaxDocs, "elasticsearch-max-docs", 0, "Number of docs to hold before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticMaxBytes, "elasticsearch-max-bytes", 0, "Number of bytes to hold before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticMaxSeconds, "elasticsearch-max-seconds", 0, "Number of seconds before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticClientTimeout, "elasticsearch-client-timeout", 0, "Number of seconds before a request to Elasticsearch is timed out")
	flag.Int64Var(&config.MaxFileSize, "max-file-size", 0, "GridFs file content exceeding this limit in bytes will not be indexed in Elasticsearch")
	flag.StringVar(&config.ConfigFile, "f", "", "Location of configuration file")
	flag.BoolVar(&config.DroppedDatabases, "dropped-databases", true, "True to delete indexes from dropped databases")
	flag.BoolVar(&config.DroppedCollections, "dropped-collections", true, "True to delete indexes from dropped collections")
	flag.BoolVar(&config.Version, "v", false, "True to print the version number")
	flag.BoolVar(&config.Gzip, "gzip", false, "True to use gzip for requests to elasticsearch")
	flag.BoolVar(&config.Verbose, "verbose", false, "True to output verbose messages")
	flag.BoolVar(&config.Stats, "stats", false, "True to print out statistics")
	flag.BoolVar(&config.IndexStats, "index-stats", false, "True to index stats in elasticsearch")
	flag.StringVar(&config.StatsDuration, "stats-duration", "", "The duration after which stats are logged")
	flag.BoolVar(&config.Resume, "resume", false, "True to capture the last timestamp of this run and resume on a subsequent run")
	flag.Int64Var(&config.ResumeFromTimestamp, "resume-from-timestamp", 0, "Timestamp to resume syncing from")
	flag.BoolVar(&config.ResumeWriteUnsafe, "resume-write-unsafe", false, "True to speedup writes of the last timestamp synched for resuming at the cost of error checking")
	flag.BoolVar(&config.Replay, "replay", false, "True to replay all events from the oplog and index them in elasticsearch")
	flag.BoolVar(&config.IndexFiles, "index-files", false, "True to index gridfs files into elasticsearch. Requires the elasticsearch mapper-attachments (deprecated) or ingest-attachment plugin")
	flag.BoolVar(&config.FileHighlighting, "file-highlighting", false, "True to enable the ability to highlight search times for a file query")
	flag.BoolVar(&config.EnablePatches, "enable-patches", false, "True to include an json-patch field on updates")
	flag.BoolVar(&config.FailFast, "fail-fast", false, "True to exit if a single _bulk request fails")
	flag.BoolVar(&config.IndexOplogTime, "index-oplog-time", false, "True to add date/time information from the oplog to each document when indexing")
	flag.BoolVar(&config.ExitAfterDirectReads, "exit-after-direct-reads", false, "True to exit the program after reading directly from the configured namespaces")
	flag.IntVar(&config.DirectReadLimit, "direct-read-limit", 0, "Maximum number of documents to fetch in each direct read query")
	flag.IntVar(&config.DirectReadBatchSize, "direct-read-batch-size", 0, "The batch size to set on direct read queries")
	flag.IntVar(&config.DirectReadersPerCol, "direct-readers-per-col", 0, "Number of goroutines directly reading a single collection")
	flag.StringVar(&config.MergePatchAttr, "merge-patch-attribute", "", "Attribute to store json-patch values under")
	flag.StringVar(&config.ResumeName, "resume-name", "", "Name under which to load/store the resume state. Defaults to 'default'")
	flag.StringVar(&config.ClusterName, "cluster-name", "", "Name of the monstache process cluster")
	flag.StringVar(&config.Worker, "worker", "", "The name of this worker in a multi-worker configuration")
	flag.StringVar(&config.MapperPluginPath, "mapper-plugin-path", "", "The path to a .so file to load as a document mapper plugin")
	flag.StringVar(&config.NsRegex, "namespace-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which match are synched to elasticsearch")
	flag.StringVar(&config.NsExcludeRegex, "namespace-exclude-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which do not match are synched to elasticsearch")
	flag.Var(&config.DirectReadNs, "direct-read-namespace", "A list of direct read namespaces")
	flag.Var(&config.ElasticUrls, "elasticsearch-url", "A list of Elasticsearch URLs")
	flag.Var(&config.FileNamespaces, "file-namespace", "A list of file namespaces")
	flag.Var(&config.PatchNamespaces, "patch-namespace", "A list of patch namespaces")
	flag.Var(&config.Workers, "workers", "A list of worker names")
	flag.Parse()
	return config
}

func (config *configOptions) loadIndexTypes() {
	if config.Mapping != nil {
		mapIndexTypes = make(map[string]*indexTypeMapping)
		for _, m := range config.Mapping {
			if m.Namespace != "" && m.Index != "" && m.Type != "" {
				mapIndexTypes[m.Namespace] = &indexTypeMapping{
					Namespace: m.Namespace,
					Index:     normalizeIndexName(m.Index),
					Type:      normalizeTypeName(m.Type),
				}
			} else {
				panic("mappings must specify namespace, index, and type attributes")
			}
		}
	}
}

func (config *configOptions) loadScripts() {
	if config.Script != nil {
		mapEnvs = make(map[string]*executionEnv)
		for _, s := range config.Script {
			if s.Namespace != "" && s.Script != "" {
				env := &executionEnv{
					VM:      otto.New(),
					Script:  s.Script,
					Routing: s.Routing,
				}
				if err := env.VM.Set("module", make(map[string]interface{})); err != nil {
					panic(err)
				}
				if _, err := env.VM.Run(env.Script); err != nil {
					panic(err)
				}
				val, err := env.VM.Run("module.exports")
				if err != nil {
					panic(err)
				} else if !val.IsFunction() {
					panic("module.exports must be a function")

				}
				mapEnvs[s.Namespace] = env
			} else {
				panic("scripts must specify namespace and script attributes")
			}
		}
	}
}

func (config *configOptions) loadPlugins() *configOptions {
	if config.MapperPluginPath != "" {
		p, err := plugin.Open(config.MapperPluginPath)
		if err != nil {
			errorLog.Panicf("Unable to load mapper plugin %s: %s", config.MapperPluginPath, err)
		}
		mapper, err := p.Lookup("Map")
		if err != nil {
			errorLog.Panicf("Unable to find symbol 'Map' in mapper plugin: %s", err)
		}
		switch mapper.(type) {
		case func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error):
			mapperPlugin = mapper.(func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error))
		default:
			errorLog.Panicf("Plugin 'Map' function must be typed %T", mapperPlugin)
		}
	}
	return config
}

func (config *configOptions) loadConfigFile() *configOptions {
	if config.ConfigFile != "" {
		var tomlConfig = configOptions{
			DroppedDatabases:     true,
			DroppedCollections:   true,
			MongoDialSettings:    mongoDialSettings{Timeout: -1},
			MongoSessionSettings: mongoSessionSettings{SocketTimeout: -1, SyncTimeout: -1},
			GtmSettings:          gtmDefaultSettings(),
		}
		if _, err := toml.DecodeFile(config.ConfigFile, &tomlConfig); err != nil {
			panic(err)
		}
		if config.MongoURL == "" {
			config.MongoURL = tomlConfig.MongoURL
		}
		if config.MongoPemFile == "" {
			config.MongoPemFile = tomlConfig.MongoPemFile
		}
		if config.MongoValidatePemFile && !tomlConfig.MongoValidatePemFile {
			config.MongoValidatePemFile = false
		}
		if config.MongoOpLogDatabaseName == "" {
			config.MongoOpLogDatabaseName = tomlConfig.MongoOpLogDatabaseName
		}
		if config.MongoOpLogCollectionName == "" {
			config.MongoOpLogCollectionName = tomlConfig.MongoOpLogCollectionName
		}
		if config.MongoCursorTimeout == "" {
			config.MongoCursorTimeout = tomlConfig.MongoCursorTimeout
		}
		if config.ElasticUser == "" {
			config.ElasticUser = tomlConfig.ElasticUser
		}
		if config.ElasticPassword == "" {
			config.ElasticPassword = tomlConfig.ElasticPassword
		}
		if config.ElasticPemFile == "" {
			config.ElasticPemFile = tomlConfig.ElasticPemFile
		}
		if config.ElasticValidatePemFile && !tomlConfig.ElasticValidatePemFile {
			config.ElasticValidatePemFile = false
		}
		if config.ElasticVersion == "" {
			config.ElasticVersion = tomlConfig.ElasticVersion
		}
		if config.ElasticMaxConns == 0 {
			config.ElasticMaxConns = tomlConfig.ElasticMaxConns
		}
		if !config.ElasticRetry && tomlConfig.ElasticRetry {
			config.ElasticRetry = true
		}
		if config.ElasticMaxDocs == 0 {
			config.ElasticMaxDocs = tomlConfig.ElasticMaxDocs
		}
		if config.ElasticMaxBytes == 0 {
			config.ElasticMaxBytes = tomlConfig.ElasticMaxBytes
		}
		if config.ElasticMaxSeconds == 0 {
			config.ElasticMaxSeconds = tomlConfig.ElasticMaxSeconds
		}
		if config.ElasticClientTimeout == 0 {
			config.ElasticClientTimeout = tomlConfig.ElasticClientTimeout
		}
		if config.MaxFileSize == 0 {
			config.MaxFileSize = tomlConfig.MaxFileSize
		}
		if config.DirectReadLimit == 0 {
			config.DirectReadLimit = tomlConfig.DirectReadLimit
		}
		if config.DirectReadBatchSize == 0 {
			config.DirectReadBatchSize = tomlConfig.DirectReadBatchSize
		}
		if config.DirectReadersPerCol == 0 {
			config.DirectReadersPerCol = tomlConfig.DirectReadersPerCol
		}
		if config.DroppedDatabases && !tomlConfig.DroppedDatabases {
			config.DroppedDatabases = false
		}
		if config.DroppedCollections && !tomlConfig.DroppedCollections {
			config.DroppedCollections = false
		}
		if !config.Gzip && tomlConfig.Gzip {
			config.Gzip = true
		}
		if !config.Verbose && tomlConfig.Verbose {
			config.Verbose = true
		}
		if !config.Stats && tomlConfig.Stats {
			config.Stats = true
		}
		if !config.IndexStats && tomlConfig.IndexStats {
			config.IndexStats = true
		}
		if config.StatsDuration == "" {
			config.StatsDuration = tomlConfig.StatsDuration
		}
		if !config.IndexFiles && tomlConfig.IndexFiles {
			config.IndexFiles = true
		}
		if !config.FileHighlighting && tomlConfig.FileHighlighting {
			config.FileHighlighting = true
		}
		if !config.EnablePatches && tomlConfig.EnablePatches {
			config.EnablePatches = true
		}
		if !config.Replay && tomlConfig.Replay {
			config.Replay = true
		}
		if !config.Resume && tomlConfig.Resume {
			config.Resume = true
		}
		if !config.ResumeWriteUnsafe && tomlConfig.ResumeWriteUnsafe {
			config.ResumeWriteUnsafe = true
		}
		if config.ResumeFromTimestamp == 0 {
			config.ResumeFromTimestamp = tomlConfig.ResumeFromTimestamp
		}
		if config.MergePatchAttr == "" {
			config.MergePatchAttr = tomlConfig.MergePatchAttr
		}
		if !config.FailFast && tomlConfig.FailFast {
			config.FailFast = true
		}
		if !config.IndexOplogTime && tomlConfig.IndexOplogTime {
			config.IndexOplogTime = true
		}
		if !config.ExitAfterDirectReads && tomlConfig.ExitAfterDirectReads {
			config.ExitAfterDirectReads = true
		}
		if config.Resume && config.ResumeName == "" {
			config.ResumeName = tomlConfig.ResumeName
		}
		if config.ClusterName == "" {
			config.ClusterName = tomlConfig.ClusterName
		}
		if config.NsRegex == "" {
			config.NsRegex = tomlConfig.NsRegex
		}
		if config.NsExcludeRegex == "" {
			config.NsExcludeRegex = tomlConfig.NsExcludeRegex
		}
		if config.IndexFiles {
			if len(config.FileNamespaces) == 0 {
				config.FileNamespaces = tomlConfig.FileNamespaces
			}
			config.LoadGridFsConfig()
		}
		if config.Worker == "" {
			config.Worker = tomlConfig.Worker
		}
		if config.MapperPluginPath == "" {
			config.MapperPluginPath = tomlConfig.MapperPluginPath
		}
		if config.EnablePatches {
			if len(config.PatchNamespaces) == 0 {
				config.PatchNamespaces = tomlConfig.PatchNamespaces
			}
			config.LoadPatchNamespaces()
		}
		if len(config.DirectReadNs) == 0 {
			config.DirectReadNs = tomlConfig.DirectReadNs
		}
		if len(config.ElasticUrls) == 0 {
			config.ElasticUrls = tomlConfig.ElasticUrls
		}
		if len(config.Workers) == 0 {
			config.Workers = tomlConfig.Workers
		}
		config.MongoDialSettings = tomlConfig.MongoDialSettings
		config.MongoSessionSettings = tomlConfig.MongoSessionSettings
		config.GtmSettings = tomlConfig.GtmSettings
		config.Logs = tomlConfig.Logs
		tomlConfig.setupLogging()
		tomlConfig.loadScripts()
		tomlConfig.loadIndexTypes()
	}
	return config
}

func (config *configOptions) newLogger(path string) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   path,
		MaxSize:    500, // megabytes
		MaxBackups: 5,
		MaxAge:     28, //days
	}
}

func (config *configOptions) setupLogging() {
	logs := config.Logs
	if logs.Info != "" {
		infoLog.SetOutput(config.newLogger(logs.Info))
	}
	if logs.Error != "" {
		errorLog.SetOutput(config.newLogger(logs.Error))
	}
	if logs.Trace != "" {
		traceLog.SetOutput(config.newLogger(logs.Trace))
	}
	if logs.Stats != "" {
		statsLog.SetOutput(config.newLogger(logs.Stats))
	}
}

func (config *configOptions) LoadPatchNamespaces() *configOptions {
	patchNamespaces = make(map[string]bool)
	for _, namespace := range config.PatchNamespaces {
		patchNamespaces[namespace] = true
	}
	return config
}

func (config *configOptions) LoadGridFsConfig() *configOptions {
	fileNamespaces = make(map[string]bool)
	for _, namespace := range config.FileNamespaces {
		fileNamespaces[namespace] = true
	}
	return config
}

func (config *configOptions) dump() {
	json, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		errorLog.Printf("Unable to print configuration: %s", err)
	} else {
		infoLog.Println(string(json))
	}
}

/*
if ssl=true is set on the connection string, remove the option
from the connection string and enable TLS because the mgo
driver does not support the option in the connection string
*/
func (config *configOptions) parseMongoURL() *configOptions {
	const queryDelim string = "?"
	hostQuery := strings.SplitN(config.MongoURL, queryDelim, 2)
	if len(hostQuery) == 2 {
		host, query := hostQuery[0], hostQuery[1]
		r := regexp.MustCompile(`ssl=true&?|&ssl=true$`)
		qstr := r.ReplaceAllString(query, "")
		if qstr != query {
			config.MongoDialSettings.Ssl = true
			if qstr == "" {
				config.MongoURL = host
			} else {
				config.MongoURL = strings.Join([]string{host, qstr}, queryDelim)
			}
		}
	}
	return config
}

func (config *configOptions) setDefaults() *configOptions {
	if config.MongoURL == "" {
		config.MongoURL = mongoURLDefault
	}
	if config.ClusterName != "" {
		if config.ClusterName != "" && config.Worker != "" {
			config.ResumeName = fmt.Sprintf("%s:%s", config.ClusterName, config.Worker)
		} else {
			config.ResumeName = config.ClusterName
		}
		config.Resume = true
	} else if config.ResumeName == "" {
		if config.Worker != "" {
			config.ResumeName = config.Worker
		} else {
			config.ResumeName = resumeNameDefault
		}
	}
	if config.ElasticMaxConns == 0 {
		config.ElasticMaxConns = elasticMaxConnsDefault
	}
	if config.ElasticClientTimeout == 0 {
		config.ElasticClientTimeout = elasticClientTimeoutDefault
	}
	if config.MergePatchAttr == "" {
		config.MergePatchAttr = "json-merge-patches"
	}
	if config.ElasticMaxSeconds == 0 {
		config.ElasticMaxSeconds = 1
	}
	if config.ElasticMaxDocs == 0 {
		config.ElasticMaxDocs = elasticMaxDocsDefault
	}
	if config.MongoURL != "" {
		config.parseMongoURL()
	}
	return config
}

func (config *configOptions) DialMongo() (*mgo.Session, error) {
	ssl := config.MongoDialSettings.Ssl || config.MongoPemFile != ""
	if ssl {
		tlsConfig := &tls.Config{}
		if config.MongoPemFile != "" {
			certs := x509.NewCertPool()
			if ca, err := ioutil.ReadFile(config.MongoPemFile); err == nil {
				certs.AppendCertsFromPEM(ca)
			} else {
				return nil, err
			}
			tlsConfig.RootCAs = certs
		}
		// Check to see if we don't need to validate the PEM
		if config.MongoValidatePemFile == false {
			// Turn off validation
			tlsConfig.InsecureSkipVerify = true
		}
		dialInfo, err := mgo.ParseURL(config.MongoURL)
		if err != nil {
			return nil, err
		}
		dialInfo.Timeout = time.Duration(10) * time.Second
		if config.MongoDialSettings.Timeout != -1 {
			dialInfo.Timeout = time.Duration(config.MongoDialSettings.Timeout) * time.Second
		}
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			if err != nil {
				errorLog.Printf("Unable to dial mongodb: %s", err)
			}
			return conn, err
		}
		session, err := mgo.DialWithInfo(dialInfo)
		if err == nil {
			session.SetSyncTimeout(1 * time.Minute)
			session.SetSocketTimeout(1 * time.Minute)
		}
		return session, err
	}
	if config.MongoDialSettings.Timeout != -1 {
		return mgo.DialWithTimeout(config.MongoURL,
			time.Duration(config.MongoDialSettings.Timeout)*time.Second)
	}
	return mgo.Dial(config.MongoURL)
}

func (config *configOptions) NewHTTPClient() (client *http.Client, err error) {
	tlsConfig := &tls.Config{}
	if config.ElasticPemFile != "" {
		var ca []byte
		certs := x509.NewCertPool()
		if ca, err = ioutil.ReadFile(config.ElasticPemFile); err == nil {
			certs.AppendCertsFromPEM(ca)
			tlsConfig.RootCAs = certs
		} else {
			return client, err
		}
	}
	if config.ElasticValidatePemFile == false {
		// Turn off validation
		tlsConfig.InsecureSkipVerify = true
	}
	transport := &http.Transport{
		TLSHandshakeTimeout: time.Duration(30) * time.Second,
		TLSClientConfig:     tlsConfig,
	}
	client = &http.Client{
		Timeout:   time.Duration(config.ElasticClientTimeout) * time.Second,
		Transport: transport,
	}
	return client, err
}

func doDrop(mongo *mgo.Session, elastic *elastic.Client, op *gtm.Op, config *configOptions) (err error) {
	if db, drop := op.IsDropDatabase(); drop {
		if config.DroppedDatabases {
			if err = deleteIndexes(elastic, db, config); err == nil {
				if e := dropDBMeta(mongo, db); e != nil {
					errorLog.Printf("unable to delete meta for db: %s", e)
				}
			}
		}
	} else if col, drop := op.IsDropCollection(); drop {
		if config.DroppedCollections {
			if err = deleteIndex(elastic, op.GetDatabase()+"."+col, config); err == nil {
				if e := dropCollectionMeta(mongo, op.GetDatabase()+"."+col); e != nil {
					errorLog.Printf("unable to delete meta for collection: %s", e)
				}
			}
		}
	}
	return
}

func doFileContent(mongo *mgo.Session, op *gtm.Op, config *configOptions) (ingestAttachment bool, err error) {
	if !config.IndexFiles {
		return
	}
	if fileNamespaces[op.Namespace] {
		err = addFileContent(mongo, op, config)
		if config.ElasticMajorVersion >= 5 {
			if op.Data["file"] != "" {
				ingestAttachment = true
			}
		}
	}
	return
}

func doResume(mongo *mgo.Session, ts bson.MongoTimestamp, config *configOptions) (err error) {
	if config.Resume {
		if ts > 0 {
			err = saveTimestamp(mongo, ts, config.ResumeName)
		}
	}
	return
}

func addPatch(config *configOptions, client *elastic.Client, op *gtm.Op,
	objectID string, indexType *indexTypeMapping, meta *indexingMeta) (err error) {
	var merges []interface{}
	var toJSON []byte
	if op.IsSourceOplog() == false {
		return nil
	}
	if op.Timestamp == 0 {
		return nil
	}
	if op.IsUpdate() {
		ctx := context.Background()
		service := client.Get().Index(indexType.Index).Type(indexType.Type).Id(objectID)
		if meta.Routing != "" {
			service.Routing(meta.Routing)
		}
		var resp *elastic.GetResult
		if resp, err = service.Do(ctx); err == nil {
			if resp.Found {
				var src map[string]interface{}
				if err = json.Unmarshal(*resp.Source, &src); err == nil {
					if val, ok := src[config.MergePatchAttr]; ok {
						merges = val.([]interface{})
						for _, m := range merges {
							entry := m.(map[string]interface{})
							entry["ts"] = int(entry["ts"].(float64))
							entry["v"] = int(entry["v"].(float64))
						}
					}
					delete(src, config.MergePatchAttr)
					var fromJSON, mergeDoc []byte
					if fromJSON, err = json.Marshal(src); err == nil {
						if toJSON, err = json.Marshal(op.Data); err == nil {
							if mergeDoc, err = jsonpatch.CreateMergePatch(fromJSON, toJSON); err == nil {
								merge := make(map[string]interface{})
								merge["ts"] = op.Timestamp >> 32
								merge["p"] = string(mergeDoc)
								merge["v"] = len(merges) + 1
								merges = append(merges, merge)
								op.Data[config.MergePatchAttr] = merges
							}
						}
					}
				}
			} else {
				err = errors.New("last document revision not found")
			}

		}
	} else {
		if _, found := op.Data[config.MergePatchAttr]; !found {
			if toJSON, err = json.Marshal(op.Data); err == nil {
				merge := make(map[string]interface{})
				merge["v"] = 1
				merge["ts"] = op.Timestamp >> 32
				merge["p"] = string(toJSON)
				merges = append(merges, merge)
				op.Data[config.MergePatchAttr] = merges
			}
		}
	}
	return
}

func doIndexing(config *configOptions, mongo *mgo.Session, bulk *elastic.BulkProcessor, client *elastic.Client, op *gtm.Op, ingestAttachment bool) (err error) {
	meta := parseIndexMeta(op.Data)
	prepareDataForIndexing(config, op)
	objectID, indexType := opIDToString(op), mapIndexType(op)
	if config.EnablePatches {
		if patchNamespaces[op.Namespace] {
			if e := addPatch(config, client, op, objectID, indexType, meta); e != nil {
				errorLog.Printf("unable to save json-patch info: %s", e)
			}
		}
	}
	req := elastic.NewBulkIndexRequest().Index(indexType.Index).Type(indexType.Type)
	req.Id(objectID)
	req.Doc(op.Data)
	if meta.Routing != "" {
		req.Routing(meta.Routing)
	}
	if ingestAttachment {
		req.Pipeline("attachment")
	}
	bulk.Add(req)
	if meta.Empty() == false {
		if e := setIndexMeta(mongo, op.Namespace, objectID, meta); e != nil {
			errorLog.Printf("unable to save routing info: %s", e)
		}
	}
	return
}

func doIndex(config *configOptions, mongo *mgo.Session, bulk *elastic.BulkProcessor, client *elastic.Client, op *gtm.Op, ingestAttachment bool) (err error) {
	if err = mapData(config, op); err == nil {
		if op.Data != nil {
			err = doIndexing(config, mongo, bulk, client, op, ingestAttachment)
		} else if op.IsUpdate() {
			doDelete(mongo, bulk, op)
		}
	}
	return
}

func doIndexStats(bulkStats *elastic.BulkProcessor, stats elastic.BulkProcessorStats) (err error) {
	var hostname string
	doc := make(map[string]interface{})
	t := time.Now().UTC()
	doc["Timestamp"] = t.Format("2006-01-02T15:04:05")
	hostname, err = os.Hostname()
	if err == nil {
		doc["Host"] = hostname
	}
	doc["Pid"] = os.Getpid()
	doc["Stats"] = stats
	index := t.Format("monstache.stats.2006-01-02")
	req := elastic.NewBulkIndexRequest().Index(index).Type("stats")
	req.Doc(doc)
	bulkStats.Add(req)
	return
}

func dropDBMeta(session *mgo.Session, db string) (err error) {
	col := session.DB("monstache").C("meta")
	q := bson.M{"db": db}
	_, err = col.RemoveAll(q)
	return
}

func dropCollectionMeta(session *mgo.Session, namespace string) (err error) {
	col := session.DB("monstache").C("meta")
	q := bson.M{"namespace": namespace}
	_, err = col.RemoveAll(q)
	return
}

func (meta *indexingMeta) Empty() bool {
	return meta.Routing == "" && meta.Index == "" && meta.Type == ""
}

func setIndexMeta(session *mgo.Session, namespace, id string, meta *indexingMeta) error {
	col := session.DB("monstache").C("meta")
	metaID := fmt.Sprintf("%s.%s", namespace, id)
	doc := make(map[string]interface{})
	doc["routing"] = meta.Routing
	doc["index"] = meta.Index
	doc["type"] = meta.Type
	doc["db"] = strings.SplitN(namespace, ".", 2)[0]
	doc["namespace"] = namespace
	_, err := col.UpsertId(metaID, bson.M{"$set": doc})
	return err
}

func getIndexMeta(session *mgo.Session, namespace, id string) (meta *indexingMeta) {
	meta = &indexingMeta{}
	col := session.DB("monstache").C("meta")
	doc := make(map[string]interface{})
	metaID := fmt.Sprintf("%s.%s", namespace, id)
	col.FindId(metaID).One(doc)
	if doc["routing"] != nil {
		meta.Routing = doc["routing"].(string)
	}
	if doc["index"] != nil {
		meta.Index = doc["index"].(string)
	}
	if doc["type"] != nil {
		meta.Type = doc["type"].(string)
	}
	col.RemoveId(metaID)
	return
}

func doDelete(mongo *mgo.Session, bulk *elastic.BulkProcessor, op *gtm.Op) {
	objectID, indexType, meta := opIDToString(op), mapIndexType(op), &indexingMeta{}
	if mapEnvs != nil {
		if env := mapEnvs[op.Namespace]; env != nil && env.Routing {
			meta = getIndexMeta(mongo, op.Namespace, objectID)
		}
	}
	if meta.Index != "" {
		indexType.Index = meta.Index
	}
	if meta.Type != "" {
		indexType.Type = meta.Type
	}
	req := elastic.NewBulkDeleteRequest().Index(indexType.Index).Type(indexType.Type)
	if meta.Routing != "" {
		req.Routing(meta.Routing)
	}
	req.Id(objectID)
	bulk.Add(req)
	return
}

func gtmDefaultSettings() gtmSettings {
	return gtmSettings{
		ChannelSize:    gtmChannelSizeDefault,
		BufferSize:     32,
		BufferDuration: "750ms",
	}
}

func createAfterBulk(mongo *mgo.Session, config *configOptions) elastic.BulkAfterFunc {
	return func(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
		if err != nil || lastTimestamp == 0 {
			return
		}
		if err = doResume(mongo, lastTimestamp, config); err != nil {
			errorLog.Printf("Unable to save timestamp: %s", err)
		}
	}
}

func notifySdFailed(config *configOptions, err error) {
	if err != nil {
		errorLog.Printf("Systemd notification failed: %s", err)
	} else {
		if config.Verbose {
			infoLog.Println("Systemd notification not supported (i.e. NOTIFY_SOCKET is unset)")
		}
	}
}

func watchdogSdFailed(config *configOptions, err error) {
	if err != nil {
		errorLog.Printf("Error determining systemd WATCHDOG interval: %s", err)
	} else {
		if config.Verbose {
			infoLog.Println("Systemd WATCHDOG not enabled")
		}
	}
}

func notifySd(config *configOptions) {
	var interval time.Duration
	if config.Verbose {
		infoLog.Println("Sending systemd READY=1")
	}
	sent, err := daemon.SdNotify(false, "READY=1")
	if sent {
		if config.Verbose {
			infoLog.Println("READY=1 successfully sent to systemd")
		}
	} else {
		notifySdFailed(config, err)
		return
	}
	interval, err = daemon.SdWatchdogEnabled(false)
	if err != nil || interval == 0 {
		watchdogSdFailed(config, err)
		return
	}
	for {
		if config.Verbose {
			infoLog.Println("Sending systemd WATCHDOG=1")
		}
		sent, err = daemon.SdNotify(false, "WATCHDOG=1")
		if sent {
			if config.Verbose {
				infoLog.Println("WATCHDOG=1 successfully sent to systemd")
			}
		} else {
			notifySdFailed(config, err)
			return
		}
		time.Sleep(interval / 2)
	}
}

func main() {
	enabled := true
	config := &configOptions{
		MongoDialSettings:    mongoDialSettings{Timeout: -1},
		MongoSessionSettings: mongoSessionSettings{SocketTimeout: -1, SyncTimeout: -1},
		GtmSettings:          gtmDefaultSettings(),
	}
	config.parseCommandLineFlags()
	if config.Version {
		fmt.Println(version)
		os.Exit(0)
	}
	config.loadConfigFile().setDefaults()
	if config.Print {
		config.dump()
		os.Exit(0)
	}
	config.loadPlugins()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	mongo, err := config.DialMongo()
	if err != nil {
		errorLog.Panicf("Unable to connect to mongodb using URL %s: %s", config.MongoURL, err)
	}
	mongo.SetMode(mgo.Primary, true)
	defer mongo.Close()
	if config.Resume && config.ResumeWriteUnsafe {
		if config.ClusterName == "" {
			mongo.SetSafe(nil)
		}
	}
	if config.MongoSessionSettings.SocketTimeout != -1 {
		timeOut := time.Duration(config.MongoSessionSettings.SocketTimeout) * time.Second
		mongo.SetSocketTimeout(timeOut)
	}
	if config.MongoSessionSettings.SyncTimeout != -1 {
		timeOut := time.Duration(config.MongoSessionSettings.SyncTimeout) * time.Second
		mongo.SetSyncTimeout(timeOut)
	}

	elasticClient, err := config.newElasticClient()
	if err != nil {
		errorLog.Panicf("Unable to create elasticsearch client: %s", err)
	}
	if config.ElasticVersion == "" {
		if err := config.testElasticsearchConn(elasticClient); err != nil {
			errorLog.Panicf("Unable to validate connection to elasticsearch using client %s: %s",
				elasticClient, err)
		}
	} else {
		if err := config.parseElasticsearchVersion(config.ElasticVersion); err != nil {
			errorLog.Panicf("Elasticsearch version must conform to major.minor.fix: %s", err)
		}
	}
	bulk, err := config.newBulkProcessor(elasticClient, mongo)
	if err != nil {
		errorLog.Panicf("Unable to start bulk processor: %s", err)
	}
	defer bulk.Stop()
	var bulkStats *elastic.BulkProcessor
	if config.IndexStats {
		bulkStats, err = config.newStatsBulkProcessor(elasticClient)
		if err != nil {
			errorLog.Panicf("Unable to start stats bulk processor: %s", err)
		}
		defer bulkStats.Stop()
	}

	go func() {
		<-sigs
		done <- true
	}()

	var after gtm.TimestampGenerator
	if config.Resume {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			ts := gtm.LastOpTimestamp(session, options)
			if config.Replay {
				ts = bson.MongoTimestamp(0)
			} else if config.ResumeFromTimestamp != 0 {
				ts = bson.MongoTimestamp(config.ResumeFromTimestamp)
			} else {
				collection := session.DB("monstache").C("monstache")
				doc := make(map[string]interface{})
				collection.FindId(config.ResumeName).One(doc)
				if doc["ts"] != nil {
					ts = doc["ts"].(bson.MongoTimestamp)
				}
			}
			return ts
		}
	} else if config.Replay {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			return bson.MongoTimestamp(0)
		}
	}

	if config.IndexFiles {
		if len(config.FileNamespaces) == 0 {
			errorLog.Fatalln("File indexing is ON but no file namespaces are configured")
		}
		for _, namespace := range config.FileNamespaces {
			if err := ensureFileMapping(elasticClient, namespace, config); err != nil {
				panic(err)
			}
			if config.ElasticMajorVersion >= 5 {
				break
			}
		}
	}

	var filter gtm.OpFilter
	var directReadFilter gtm.OpFilter
	filterChain := []gtm.OpFilter{notMonstache, notSystem, notChunks}
	if config.NsRegex != "" {
		filterChain = append(filterChain, filterWithRegex(config.NsRegex))
	}
	if config.NsExcludeRegex != "" {
		filterChain = append(filterChain, filterInverseWithRegex(config.NsExcludeRegex))
	}
	if config.Worker != "" {
		workerFilter, err := consistent.ConsistentHashFilter(config.Worker, config.Workers)
		if err != nil {
			panic(err)
		}
		filterChain = append(filterChain, workerFilter)
		directReadFilter = workerFilter
	} else if config.Workers != nil {
		panic("workers configured but this worker is undefined. worker must be set to one of the workers.")
	}
	filter = gtm.ChainOpFilters(filterChain...)
	var oplogDatabaseName, oplogCollectionName, cursorTimeout *string
	if config.MongoOpLogDatabaseName != "" {
		oplogDatabaseName = &config.MongoOpLogDatabaseName
	}
	if config.MongoOpLogCollectionName != "" {
		oplogCollectionName = &config.MongoOpLogCollectionName
	}
	if config.MongoCursorTimeout != "" {
		cursorTimeout = &config.MongoCursorTimeout
	}
	if config.ClusterName != "" {
		if err = ensureClusterTTL(mongo); err == nil {
			infoLog.Printf("Joined cluster %s", config.ClusterName)
		} else {
			errorLog.Panicf("Unable to enable cluster mode: %s", err)
		}
		enabled, err = isEnabledProcess(mongo, config)
		if err != nil {
			errorLog.Panicf("Unable to determine enabled cluster process: %s", err)
		}
		if !enabled {
			config.DirectReadNs = stringargs{}
		}
	}
	gtmBufferDuration, err := time.ParseDuration(config.GtmSettings.BufferDuration)
	if err != nil {
		errorLog.Panicf("Unable to parse gtm buffer duration %s: %s", config.GtmSettings.BufferDuration, err)
	}
	gtmCtx := gtm.Start(mongo, &gtm.Options{
		After:               after,
		Filter:              filter,
		OpLogDatabaseName:   oplogDatabaseName,
		OpLogCollectionName: oplogCollectionName,
		CursorTimeout:       cursorTimeout,
		ChannelSize:         config.GtmSettings.ChannelSize,
		Ordering:            gtm.Oplog,
		WorkerCount:         1,
		BufferDuration:      gtmBufferDuration,
		BufferSize:          config.GtmSettings.BufferSize,
		DirectReadNs:        config.DirectReadNs,
		DirectReadLimit:     config.DirectReadLimit,
		DirectReadBatchSize: config.DirectReadBatchSize,
		DirectReadersPerCol: config.DirectReadersPerCol,
		DirectReadFilter:    directReadFilter,
	})
	if config.ClusterName != "" {
		if enabled {
			infoLog.Printf("Starting work for cluster %s", config.ClusterName)
		} else {
			infoLog.Printf("Pausing work for cluster %s", config.ClusterName)
			gtmCtx.Pause()
		}
	}
	heartBeat := time.NewTicker(10 * time.Second)
	if config.ClusterName == "" {
		heartBeat.Stop()
	}
	statsTimeout := time.Duration(30) * time.Second
	if config.StatsDuration != "" {
		statsTimeout, err = time.ParseDuration(config.StatsDuration)
		if err != nil {
			errorLog.Panicf("Unable to parse stats duration: %s", err)
		}
	}
	printStats := time.NewTicker(statsTimeout)
	if config.Stats == false {
		printStats.Stop()
	}
	exitStatus := 0
	if len(config.DirectReadNs) > 0 {
		go func(c *gtm.OpCtx, config *configOptions) {
			c.DirectReadWg.Wait()
			if config.ExitAfterDirectReads {
				done <- true
			}
		}(gtmCtx, config)
	}
	go notifySd(config)
	for {
		select {
		case <-done:
			bulk.Flush()
			bulk.Stop()
			if bulkStats != nil {
				bulkStats.Flush()
				bulkStats.Stop()
			}
			if config.ClusterName != "" {
				resetClusterState(mongo, config)
			}
			mongo.Close()
			os.Exit(exitStatus)
		case <-heartBeat.C:
			if config.ClusterName == "" {
				break
			}
			if enabled {
				enabled = isEnabledProcessID(mongo, config)
				if !enabled {
					infoLog.Printf("Pausing work for cluster %s", config.ClusterName)
					gtmCtx.Pause()
					bulk.Stop()
				}
			} else {
				if enabled, err = isEnabledProcess(mongo, config); err == nil {
					if enabled {
						infoLog.Printf("Resuming work for cluster %s", config.ClusterName)
						bulk.Start(context.Background())
						resumeWork(gtmCtx, mongo, config)
					}
				} else {
					gtmCtx.ErrC <- err
				}
			}
		case <-printStats.C:
			if !enabled {
				break
			}
			if config.IndexStats {
				if err := doIndexStats(bulkStats, bulk.Stats()); err != nil {
					errorLog.Printf("Error indexing statistics: %s", err)
				}
			} else {
				stats, err := json.Marshal(bulk.Stats())
				if err != nil {
					errorLog.Printf("Unable to log statistics: %s", err)
				} else {
					statsLog.Println(string(stats))
				}
			}
		case err = <-gtmCtx.ErrC:
			exitStatus = 1
			lastTimestamp = 0
			errorLog.Println(err)
			if config.FailFast {
				os.Exit(exitStatus)
			}
		case op := <-gtmCtx.OpC:
			if !enabled {
				break
			}
			if op.IsSourceOplog() {
				lastTimestamp = op.Timestamp
			}
			if op.IsDrop() {
				bulk.Flush()
				if err = doDrop(mongo, elasticClient, op, config); err != nil {
					gtmCtx.ErrC <- err
				}
			} else if op.IsDelete() {
				doDelete(mongo, bulk, op)
			} else if op.Data != nil {
				ingestAttachment := false
				if ingestAttachment, err = doFileContent(mongo, op, config); err != nil {
					gtmCtx.ErrC <- err
				}
				if err = doIndex(config, mongo, bulk, elasticClient, op, ingestAttachment); err != nil {
					gtmCtx.ErrC <- err
				}
			}
		}
	}
}
