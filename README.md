# monstache
a go daemon which synchs mongodb to elasticsearch

<img src="https://raw.github.com/rwynn/monstache/master/images/monstache.png"/>

### Install ###

	go get github.com/rwynn/monstache

### Usage ###

monstache -f PATH-TO-JSON -mongo-url URL -elasticsearch-url URL -elasticsearch-max-conns CONNS 

All command line arguments are optional.  With no arguments monstache expects to connect to mongodb and
elasticsearch on localhost using the default ports.  The maximum connection for elasticsearch will be set
by default to 10.

If the -f option is supplied the argument value should correspond to the file path of a JSON config file.

A JSON config file looks like this:

	{
	  "mongo-url": "mongodb://someuser:password@localhost:40001",
	  "elasticsearch-url": "http://someuser:password@localhost:9200"
	  "elasticsearch-max-conns": 10
	}

Arguments supplied on the command line override settings in a config file

### Config Syntax ###

For information on the syntax of the mongodb URL see [Standard Connection String Format](https://docs.mongodb.com/v3.0/reference/connection-string/#standard-connection-string-format)

The elasticsearch URL should point to where elasticsearch's RESTful API is configured

### Document Mapping ###

When indexing documents from mongodb into elasticsearch the mapping is as follows:

	mongodb database -> elasticsearch index
	mongodb collection -> elasticsearch type
	mongodb document id -> elasticsearch document id

### Planned Enhancments ###

Provide control over resuming from the last recorded change in mongodb. Currently, if monstache is stopped
and restarted, any changes between the stop and start will not be queued for elasticsearch.  

Provide options to catchup from previous events.  Currently, when monstache starts it finds the last 
operation in the log and starts tailing and processing events after that event.
