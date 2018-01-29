# monstache
a go daemon which syncs mongodb to elasticsearch in realtime

<img src="https://raw.github.com/rwynn/monstache/master/images/monstache.png"/>

### Features

- Single binary with a light footprint 

- Optionally filter the set of collections to sync

- Advanced support for sharded MongoDB clusters including auto-detection of new shards

- Direct read mode to do a full sync of collections in addition to tailing the oplog

- Transform and filter documents before indexing using Golang plugins or JavaScript

- Index the content of GridFS files

- Support for hard and soft deletes in MongoDB

- Support for propogating database and collection drops

- Optional custom document routing in Elasticsearch

- Stateful resume feature

- Worker and Clustering modes for High Availability

- Support for [rfc7396](https://tools.ietf.org/html/rfc7396) JSON merge patches

- Systemd support

- Optional http server to get access to liveness, stats, etc

### Documentation

See the [monstache site](https://rwynn.github.io/monstache-site/) for information on configuration and usage.

