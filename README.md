# monstache
a go daemon that syncs mongodb to elasticsearch in realtime

### Version 5

This is a version of monstache built against the official MongoDB golang driver.

Some of the monstache settings related to MongoDB have been removed in this version as they are now supported in the 
[connection string](https://github.com/mongodb/mongo-go-driver/blob/v1.0.0/x/network/connstring/connstring.go)

This version of monstache targets MongoDB 3.6+ and Elasticsearch 6+. For better compability with Elasticsearch 7 use
the `rel6` branch.

### Changes from previous versions

Monstache now defaults to use change streams instead of tailing the oplog for changes.  Without any configuration
monstache watches the entire MongoDB deployment.  You can specify specific namespaces to watch by setting the option
`change-stream-namespaces` to an array of strings.

The interface for golang plugins has changed due to the switch to the new driver. Previously the API exposed
a `Session` field typed as a `*mgo.Session`.  Now that has been replaced with a `MongoClient` field which has the type
`*mongo.Client`. 

See the MongoDB go driver docs for details on how to use this client.
