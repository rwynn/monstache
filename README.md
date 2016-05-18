# monstache
a go daemon which synchs mongodb to elasticsearch in near realtime

<img src="https://raw.github.com/rwynn/monstache/master/images/monstache.png"/>

### Install ###

	go get github.com/rwynn/monstache

### Getting Started ###

Since monstache uses the mongodb oplog to tail events it is required that mongodb is cofigured to produce an oplog.

This can be ensured by:
+ Setting up [replica sets](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/)
+ Passing --master to the mongod process
+ Setting the following in /etc/mongod.conf

	master = true

monstache is not bi-directional.  It only synchs from mongodb to elasticsearch.

### Usage ###

monstache \[-f PATH-TO-JSON\] \[options\]

All command line arguments are optional.  With no arguments monstache expects to connect to mongodb and
elasticsearch on localhost using the default ports. 

If the -f option is supplied the argument value should be the file path of a TOML config file.

A sample TOML config file looks like this:

	mongo-url = "mongodb://someuser:password@localhost:40001"
	elasticsearch-url = "http://someuser:password@localhost:9200"
	elasticsearch-max-conns = 10
	replay = false
	resume = true
	resume-name = "default"
	namespace-regex = "^mydb.mycollection$"
	namespace-exclude-regex = "^mydb.ignorecollection$"
	gtm-channel-size = 200
	

All options in the config file above also work if passed explicity by the same name to the monstache command

Arguments supplied on the command line override settings in a config file

The following defaults are used for missing config values:

	mongo-url -> localhost
	elasticsearch-url -> localhost
	elasticsearch-max-conns -> 10
	replay -> false
	resume -> false
	resume-name -> default
	namespace-regex -> nil
	namespace-exclude-regex -> nil
	gtm-channel-size -> 100

When `resume` is true, monstache writes the timestamp of mongodb operations it has succefully synched to elasticsearch
to the collection `monstache.monstache`.  It also reads this value from that collection when it starts in order to replay
events which it might have missed because monstache was stopped. monstache uses the value of `resume-name` as a key when
storing and retrieving timestamps.  If resume is true but `resume-name` is not supplied this key defaults to `default`.

When `replay` is true, monstache replays all events from the beginning of the mongodb oplog and synchs them to elasticsearch.

When `resume` and `replay` are both true, monstache replays all events from the beginning of the mongodb oplog and synchs them
to elasticsearch and also writes the timestamp of processed event to `monstache.monstache`. 

When neither `resume` nor `replay` are true, monstache reads the last timestamp in the oplog and starts listening for events
occurring after this timestamp.  Timestamps are not written to `monstache.monstache`.  This is the default behavior. 

When `namespace-regex` is supplied this regex is tested against the namespace, `database.collection`, of the event. If
the regex matches continues processing event filters, otherwise it drops the event. By default monstache
processes events in all database and all collections with the exception of the reserved database `monstache`, any
collections suffixed with `.chunks` and the system collections.

When `namespace-exclude-regex` is supplied this regex is tested against the namespace, `database.collection`, of the event. If
the regex matches monstache ignores the event, otherwise it continues processing event filters. By default monstache
processes events in all database and all collections with the exception of the reserved database `monstache`, any
collections suffixed with `.chunks` and the system collections.

When `gtm-channel-size` is supplied it controls the size of the go channels created for processing events.  When many events
are processed at once a larger channel size may prevent blocking in gtm.

### Config Syntax ###

For information on the syntax of the mongodb URL see [Standard Connection String Format](https://docs.mongodb.com/v3.0/reference/connection-string/#standard-connection-string-format)

The elasticsearch URL should point to where elasticsearch's RESTful API is configured

### Document Mapping ###

When indexing documents from mongodb into elasticsearch the mapping is as follows:

	mongodb database -> elasticsearch index
	mongodb collection -> elasticsearch type
	mongodb document id -> elasticsearch document id
