# Quick Start - Monstache Kafka Plugin

Get the plugin working in 5 minutes.

## 1. Prepare Environment

```bash
# Copy example env
cp .env.example .env

# Edit .env with your actual values
# MONGODB_URI, ELASTIC_URL, KAFKA_BROKERS
```

## 2. Customize for Your Schema

Edit `kafka_plugin.go`:

```go
// Line 28-30: Change collection names
primaryCollection   = "your_main_collection"
relatedCollection   = "your_related_collection"

// Line 47-73: Update field lists
var collectionFields = map[string][]string{
    primaryCollection: {
        "_id", "field1", "field2", "createdAt", "updatedAt",
    },
    relatedCollection: {
        "_id", "action", "createdAt",
    },
}
```

Edit `monstache.toml`:

```toml
# Line 2-4: Use your database and collection names
direct-read-namespaces = ["your_db.your_main_collection", "your_db.your_related_collection"]
change-stream-namespaces = ["your_db.your_main_collection", "your_db.your_related_collection"]

# Line 10: Your sync name
resume-name = "your-sync-name"
```

## 3. Build Plugin

```bash
go build -buildmode=plugin -o kafka_plugin.so kafka_plugin.go
```

If build fails:
- Ensure Go 1.16+ installed: `go version`
- Check monstache v6 is available: `go get github.com/rwynn/monstache/v6@latest`

## 4. Start Infrastructure

```bash
# Start MongoDB (if not running)
docker run -d -p 27017:27017 mongo:latest

# Start Elasticsearch (if not running)
docker run -d -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:8.4.2

# Start Kafka (if not running)
docker run -d -p 9092:9092 confluentinc/cp-kafka:latest
```

## 5. Run Monstache

```bash
monstache -f monstache.toml
```

You should see:
```
[KafkaPlugin] 2025/08/05 10:30:00 Kafka plugin init brokers=[localhost:9092] topics=[example_primary]
[KafkaPlugin] 2025/08/05 10:30:00 Created topic: monstache.example_primary
```

## 6. Verify It Works

### Insert test document:
```javascript
db.your_main_collection.insertOne({
  _id: ObjectId(),
  field1: "test",
  field2: 42,
  createdAt: new Date(),
  updatedAt: new Date()
})
```

### Check Elasticsearch:
```bash
curl http://localhost:9200/your-main-collection-*/\*?pretty
```

### Check Kafka:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic monstache.your_main_collection --from-beginning
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Plugin not found | Verify .so path matches `mapper-plugin-path` in toml |
| Kafka connection error | Check `KAFKA_BROKERS` format: `host:port,host:port` |
| No documents syncing | Verify `KAFKA_TOPIC_COLLECTIONS` matches collection names |
| Empty ES indices | Check `direct-read-namespaces` - must be `db.collection` format |
| Missing fields | Verify fields in `collectionFields` map match your schema |

## Next Steps

- Read [PLUGIN_EXAMPLE.md](PLUGIN_EXAMPLE.md) for advanced customization
- Check [Monstache docs](https://docs.monstache.io) for configuration options
- Look at `Map()` function in plugin for enrichment examples
