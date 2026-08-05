# Monstache Kafka Plugin - Community Example

This is a Monstache mapper plugin that demonstrates dual-sync to Elasticsearch and Kafka with document transformation and enrichment.

## Features

- **Dual Sync**: Publish documents to both Elasticsearch and Kafka simultaneously
- **Field Filtering**: Whitelist specific fields per collection
- **Document Enrichment**: Join and aggregate data from related collections
- **Time-based Indexing**: Monthly index naming for time-series data
- **External Versioning**: Use document timestamps for ES update ordering
- **Error Handling**: Graceful degradation and retry mechanisms
- **Stateful Resume**: Track sync progress via Monstache resume tokens

## Architecture

### Data Flow

```
MongoDB (Change Stream)
    ↓
Monstache Core
    ↓
Plugin Map() function
    ├→ filterFields() - whitelist fields
    ├→ attachRelatedDocuments() - optional join
    ├→ enrichDocumentWithRelated() - optional aggregate
    ↓
Kafka Topic
↓
Elasticsearch Index
```

### Customization Points

1. **Collection Names** (`primaryCollection`, `relatedCollection`, etc.)
   - Change to match your MongoDB collections

2. **Field Filtering** (`collectionFields` map)
   - Define which fields to include in sync
   - Supports nested fields via dot notation (e.g., `"metadata.region"`)
   - Remove collection entries to include all fields

3. **Document Enrichment** (`attachRelatedDocuments`, `enrichDocumentWithRelated`)
   - Example shows joining data from another collection
   - Add aggregation logic per use case
   - Customize field names and queries

4. **Index Naming** (in `Map()`)
   - Default: `{collection}-{YYYYMM}` (monthly)
   - Change `parseMonth()` or index format string as needed

5. **Skip Logic** (in `Map()`)
   - Uncomment example to skip certain documents
   - Customize filter condition

## Setup & Usage

### 1. Configure Environment

```bash
export KAFKA_BROKERS=localhost:9092
export KAFKA_ALLOW_SYNC=true
export KAFKA_TOPIC_COLLECTIONS=example_primary,example_related,example_detail
```

### 2. Update monstache.toml

```toml
direct-read-namespaces = ["your_db.example_primary", "your_db.example_related"]
change-stream-namespaces = ["your_db.example_primary", "your_db.example_related"]

mapper-plugin-path = "./kafka_plugin.so"
mapper-plugin-compress = false

resume = true
resume-name = "example-monstache-sync"
```

### 3. Customize Plugin

Edit collection names, fields, and enrichment logic to match your schema:

```go
// Update these:
primaryCollection   = "your_collection_name"
relatedCollection   = "your_related_collection"

// Update this map:
var collectionFields = map[string][]string{
    primaryCollection: {
        "_id", "userId", "status", "createdAt", // your fields
    },
    // ...
}

// Customize enrichment in enrichDocumentWithRelated()
```

### 4. Build & Run

```bash
# Build plugin
go build -buildmode=plugin -o kafka_plugin.so kafka_plugin.go

# Run Monstache
monstache -f monstache.toml
```

## Kafka Topics

Topics are auto-created with pattern: `monstache.{collection_name}`

Example:
- `monstache.example_primary`
- `monstache.example_related`

### Message Format

```json
{
  "collection": "example_primary",
  "namespace": "your_db.example_primary",
  "operation": "insert",
  "data": {
    "_id": "...",
    "userId": "...",
    // filtered fields only
  }
}
```

## Elasticsearch Indexing

### Index Naming
- Default: `{collection}-{YYYYMM}` (e.g., `example-primary-202608`)
- Monthly rotation for time-series data management

### Version Handling
- Uses `updatedAt` field as external version (if present)
- Prevents older updates from overwriting newer data
- `index-as-update = true` in config for partial updates

## Error Handling

### Kafka Write Failures
- Default: returns error to Monstache (triggers retry)
- Alternative: log warning and continue ES sync (uncomment in code)

### Related Document Queries
- Timeouts: 5s for MongoDB queries, 3s for Kafka writes
- Errors logged but don't block ES sync
- Missing related data results in nil (no enrichment)

## Extending the Plugin

### Add Collection-Specific Logic

```go
if input.Collection == primaryCollection {
    // Your custom transformation
    doc["customField"] = someValue
}
```

### Modify Enrichment

```go
func enrichDocumentWithRelated(doc map[string]interface{}, related []map[string]interface{}) {
    // Your aggregation logic
    for _, r := range related {
        // process related documents
    }
}
```

### Add Field Transformation

```go
// In Map() after filterFields():
if name, ok := doc["name"].(string); ok {
    doc["name"] = strings.ToUpper(name)  // example: normalize
}
```

## Troubleshooting

### Plugin not loaded
- Verify `mapper-plugin-path` points to correct .so file
- Check Monstache logs for build errors
- Ensure KAFKA_BROKERS and KAFKA_TOPIC_COLLECTIONS env vars set

### Kafka connection fails
- Verify brokers are running: `nc -zv localhost 9092`
- Check KAFKA_BROKERS format (comma-separated, include ports)

### Missing enriched fields
- Verify `relatedIds` field exists in primary documents
- Check collection name in `attachRelatedDocuments()` call
- Enable verbose logging in monstache.toml

### Stale data in Elasticsearch
- Set `index-as-update = true` to apply partial updates
- Ensure `updatedAt` field is present for versioning
- Check external version value via ES API

## Performance Tips

- **Reduce field count**: fewer fields = faster serialization
- **Increase batch size**: tune Kafka BatchTimeout
- **Projection optimization**: use `SetProjection()` to load only needed fields
- **Parallel processing**: Monstache handles parallelism automatically
- **Monitoring**: check Kafka consumer lag and ES indexing rate

## License

Same as Monstache project

## Contributing

When contributing this plugin to Monstache:
1. Remove any domain-specific field names
2. Use generic collection/field names as examples
3. Document customization points clearly
4. Test with multiple MongoDB/ES versions
5. Include inline comments for non-obvious logic
