# monstache
a go daemon that syncs mongodb to elasticsearch in realtime

[![Build Status](https://travis-ci.org/rwynn/monstache.svg?branch=master)](https://travis-ci.org/rwynn/monstache)
[![Go Report Card](https://goreportcard.com/badge/github.com/rwynn/monstache)](https://goreportcard.com/report/github.com/rwynn/monstache)

### Features

- Supports up to and including the latest versions of Elasticsearch and MongoDB

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

- Optional http server to get access to liveness, stats, isEnabled(for cluster mode) etc

### Documentation

See the [monstache site](https://rwynn.github.io/monstache-site/) for information on configuration and usage.

