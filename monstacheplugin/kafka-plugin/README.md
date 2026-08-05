# Monstache Kafka Plugin - Community Example

A production-ready **Golang plugin** for Monstache that synchronizes MongoDB collections to both Elasticsearch and Kafka with document transformation and enrichment.

This is a **generic, reusable example** suitable for contribution to the [Monstache](https://github.com/rwynn/monstache) community. It demonstrates best practices for:
- Field filtering and whitelisting
- Document enrichment via related collection joins
- Dual-destination sync (ES + Kafka)
- Stateful resume and error handling

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for a 5-minute setup.

## Detailed Guide

See [PLUGIN_EXAMPLE.md](PLUGIN_EXAMPLE.md) for customization, architecture, and advanced usage.

## Setup Overview

1. **Customize for Your Schema**
   - Edit collection names, field lists, and enrichment logic in `kafka_plugin.go`
   - Update `monstache.toml` with your database/collection names

2. **Build the Plugin**
   ```bash
   go build -buildmode=plugin -o kafka_plugin.so kafka_plugin.go
   ```

3. **Configure Environment**
   - Copy `.env.example` to `.env`
   - Update MongoDB, Elasticsearch, and Kafka connection details

4. **Run Monstache**
   ```bash
   monstache -f monstache.toml
   ```

5. **Optional: Docker**
   - Use Docker Compose: `docker-compose up -d --build`
   - Automatically builds plugin and starts Monstache

## Architecture

### Plugin Components

**Key Features:**
- **Field Filtering**: Whitelist fields per collection (supports nested fields via dot notation)
- **Document Enrichment**: Join and aggregate data from related collections (optional)
- **Kafka Publishing**: Dual-destination sync (Elasticsearch + Kafka topics)
- **Dynamic ES Indexing**: Time-based monthly index rotation (customizable)
- **Error Handling**: Panic recovery and graceful error propagation
- **Auto Topic Creation**: Kafka topics auto-created on startup
- **Stateful Resume**: Resume from checkpoint via Monstache tokens
- **External Versioning**: Prevent out-of-order updates using document timestamps

**Hook Functions:**
- `init()`: Setup Kafka writer/admin, create topics, validate env vars
- `Map()`: Main transformation: filter → enrich → publish to Kafka+ES
- `OnShutdown()`: Graceful Kafka connection cleanup

### Why Golang Plugin?

**vs JavaScript Scripts:**
- Orders of magnitude faster (compiled vs interpreted)
- Native concurrent processing
- No JavaScript environment contention

**Requirements:**
- Monstache v6.8+ required
- Go 1.16+ for plugin compilation

**Compatibility Rules**:

- ❌ **Cannot use both** JS Transform scripts and Golang plugin for the same output document
- ✅ **Use Golang plugin only**: Single source of truth for data transformation (current approach)
- The plugin handles all transformations and outputs to both Elasticsearch and Kafka

> **Important**: If you enable a Golang plugin, Monstache will ignore any JavaScript middleware in your configuration. The choice of middleware language is mutually exclusive.

## Customization

Edit the following sections in `kafka_plugin.go` to adapt for your schema:

1. **Collection Names** (lines 27-30): `primaryCollection`, `relatedCollection`, etc.
2. **Field Lists** (lines 47-79): Update `collectionFields` map per collection
3. **Enrichment Logic** (lines 180-235): Customize `attachRelatedDocuments()` and `enrichDocumentWithRelated()`
4. **Index Naming** (line 289): Modify the index format if needed

See [PLUGIN_EXAMPLE.md](PLUGIN_EXAMPLE.md) for detailed customization guide.

## Contributing to Monstache

This plugin is designed for community contribution to [Monstache](https://github.com/rwynn/monstache).

**Before submitting:**
1. Verify all domain-specific names are replaced with generic examples ✓
2. Add clear inline documentation for customization points ✓
3. Test with multiple MongoDB/Elasticsearch versions
4. Include examples of common use cases

### References

- [Monstache Golang Plugins Documentation](https://rwynn.github.io/monstache-site/advanced/#golang)
- [Monstache Configuration Reference](https://docs.monstache.io/v6/)
- [Kafka-go Library](https://github.com/segmentio/kafka-go)
- [MongoDB Go Driver](https://pkg.go.dev/go.mongodb.org/mongo-driver)
