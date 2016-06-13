# monstache
a go daemon which syncs mongodb to elasticsearch in near realtime

<img src="https://raw.github.com/rwynn/monstache/master/images/monstache.png"/>

### Install ###

	go get github.com/rwynn/monstache

### Getting Started ###

Since monstache uses the mongodb oplog to tail events it is required that mongodb is configured to produce an oplog.

This can be ensured by:
+ Setting up [replica sets](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/)
+ Passing --master to the mongod process
+ Setting the following in /etc/mongod.conf

	```
	master = true
	```

monstache is not bi-directional.  It only syncs from mongodb to elasticsearch.

### Usage ###

monstache \[-f PATH-TO-TOML\] \[options\]

All command line arguments are optional.  With no arguments monstache expects to connect to mongodb and
elasticsearch on localhost using the default ports. 

If the -f option is supplied the argument value should be the file path of a TOML config file.

A sample TOML config file looks like this:

	mongo-url = "mongodb://someuser:password@localhost:40001"
	mongo-pem-file = "/path/to/mongoCert.pem"
	elasticsearch-url = "http://someuser:password@localhost:9200"
	elasticsearch-max-conns = 10
	replay = false
	resume = true
	resume-name = "default"
	namespace-regex = "^mydb.mycollection$"
	namespace-exclude-regex = "^mydb.ignorecollection$"
	gtm-channel-size = 200
	index-files = true
	

All options in the config file above also work if passed explicity by the same name to the monstache command

Arguments supplied on the command line override settings in a config file

The following defaults are used for missing config values:

	mongo-url -> localhost
	mongo-pem-file -> nil
	elasticsearch-url -> localhost
	elasticsearch-max-conns -> 10
	replay -> false
	resume -> false
	resume-name -> default
	namespace-regex -> nil
	namespace-exclude-regex -> nil
	gtm-channel-size -> 100
	index-files -> false

When `resume` is true, monstache writes the timestamp of mongodb operations it has successfully synced to elasticsearch
to the collection `monstache.monstache`.  It also reads this value from that collection when it starts in order to replay
events which it might have missed because monstache was stopped. monstache uses the value of `resume-name` as a key when
storing and retrieving timestamps.  If `resume` is true but `resume-name` is not supplied this key defaults to `default`.

When `replay` is true, monstache replays all events from the beginning of the mongodb oplog and syncs them to elasticsearch.

When `resume` and `replay` are both true, monstache replays all events from the beginning of the mongodb oplog, syncs them
to elasticsearch and also writes the timestamp of processed events to `monstache.monstache`. 

When neither `resume` nor `replay` are true, monstache reads the last timestamp in the oplog and starts listening for events
occurring after this timestamp.  Timestamps are not written to `monstache.monstache`.  This is the default behavior. 

When `namespace-regex` is supplied this regex is tested against the namespace, `database.collection`, of the event. If
the regex matches monstache continues processing event filters, otherwise it drops the event. By default monstache
processes events in all databases and all collections with the exception of the reserved database `monstache`, any
collections suffixed with `.chunks`, and the system collections.

When `namespace-exclude-regex` is supplied this regex is tested against the namespace, `database.collection`, of the event. If
the regex matches monstache ignores the event, otherwise it continues processing event filters. By default monstache
processes events in all databases and all collections with the exception of the reserved database `monstache`, any
collections suffixed with `.chunks`, and the system collections.

When `gtm-channel-size` is supplied it controls the size of the go channels created for processing events.  When many events
are processed at once a larger channel size may prevent blocking in gtm.

When `mongo-pem-file` is supplied monstache will use the supplied file path to add a local certificate to x509 cert
pool when connecting to mongodb. This should only be used when mongodb is configured with SSL enabled.

When `index-files` is true monstache will index the raw content of files stored in GridFS into elasticsearch as an attachment type.
By default `index-files` is false meaning that monstache will only index metadata associated with files stored in GridFS.
In order for `index-files` to index the raw content of files stored in GridFS you must install a plugin for elasticsearch.
For versions of elasticsearch prior to version 5, you should install the [mapper-attachments](https://www.elastic.co/guide/en/elasticsearch/plugins/2.3/mapper-attachments.html) plugin.  In version 5 or greater
of elasticsearch the mapper-attachment plugin is deprecated and you should install the [ingest-attachment](https://www.elastic.co/guide/en/elasticsearch/plugins/master/ingest-attachment.html) plugin instead.
For further information on how to configure monstache to index content from grids, see the section [Indexing Gridfs Files](#files).

### Config Syntax ###

For information on the syntax of the mongodb URL see [Standard Connection String Format](https://docs.mongodb.com/v3.0/reference/connection-string/#standard-connection-string-format)

The elasticsearch URL should point to where elasticsearch's RESTful API is configured

### Document Mapping ###

When indexing documents from mongodb into elasticsearch the mapping is as follows:

	mongodb database -> elasticsearch index
	mongodb collection -> elasticsearch type
	mongodb document id -> elasticsearch document id

If these default won't work for some reason you can override the index and collection mapping on a per collection basis by adding
the following to your TOML config file:

	[[mapping]]
	namespace = "test.test"
	index = "index1"
	type = "type1"

	[[mapping]]
	namespace = "test.test2"
	index = "index2"
	type = "type2"

With the configuration above documents in the `test.test` namespace in mongodb are indexed into the `index1` 
index in elasticsearch with the `type1` type.

### Field Mapping ###

monstache uses the amazing [otto](https://github.com/robertkrimen/otto) library to provide transformation at the document field
level in javascript.  You can associate one javascript mapping function per mongodb collection.  These javascript functions are
added to your TOML config file, for example:
	
	[[script]]
	namespace = "mydb.mycollection"
	script = """
	var counter = 1;
	module.exports = function(doc) {
		doc.foo += "test" + counter;
		counter++;
		return _.omit(doc, "password", "secret");
	}
	"""

	[[script]]
	namespace = "anotherdb.anothercollection"
	script = """
	var counter = 1;
	module.exports = function(doc) {
		doc.foo += "test2" + counter;
		counter++;
		return doc;
	}
	"""

The example TOML above configures 2 scripts. The first is applied to `mycollection` in `mydb` while the second is applied
to `anothercollection` in `anotherdb`.

You will notice that the multi-line string feature of TOML is used to assign a javascript snippet to the variable named
`script`.  The javascript assigned to script must assign a function to the exports property of the `module` object.  This 
function will be passed the document from mongodb just before it is indexed in elasticsearch.  Inside the function you can
manipulate the document to drop fields, add fields, or augment the existing fields.  The only requirement is that you
return an object.  The object returned from the mapping function is what actually gets indexed in elasticsearch. The `this`
reference in the mapping function is assigned to the document from mongodb.  

You may have noticed that in the example above the exported mapping function closes over a var named `counter`.  You can
use closures to maintain state between invocations of your mapping function.

Finally, since Otto makes it so easy, the venerable [Underscore](http://underscorejs.org/) library is included for you at 
no extra charge.  Feel free to abuse the power of the `_`.  

<a name="files"></a>
### Indexing GridFS Files ###

As of version 1.1 monstache supports indexing the raw content of files stored in GridFS into elasticsearch for full
text search.  This feature requires that you install an elasticsearch plugin which enables the field type `attachment`.
For versions of elasticsearch prior to version 5 you should install the 
[mapper-attachments](https://www.elastic.co/guide/en/elasticsearch/plugins/2.3/mapper-attachments.html) plugin.
For version 5 or later of elasticsearch you should instead install the 
[ingest-attachment](https://www.elastic.co/guide/en/elasticsearch/plugins/master/ingest-attachment.html) plugin.

Once you have installed the appropriate plugin for elasticsearch, getting file content from GridFS into elasticsearch is
as simple as configuring monstache.  You will want to enable the `index-files` option and also tell monstache the 
namespace of all collections which will hold GridFS files. For example in your TOML config file,

	index-files = true

	file-namespaces = ["users.fs.files", "posts.fs.files"]

The above configuration tells monstache that you wish to index the raw content of GridFS files in the `users` and `posts`
mongodb databases. By default, mongodb uses a bucket named `fs`, so if you just use the defaults your collection name will
be `fs.files`.  However, if you have customized the bucket name, then your file collection would be something like `mybucket.files`
and the entire namespace would be `users.mybucket.files`.

When you configure monstache this way it will perform an additional operation at startup to ensure the destination indexes in
elasticsearch have a field named `filecontent` with a type mapping of `attachment`.  

For the example TOML configuration above, monstache would initialize 2 indices in preparation for indexing into
elasticsearch by issuing the following REST commands:

	POST /users
	{
	  "mappings": {
	    "fs.files": {
	      "properties": {
		"filecontent": { "type": "attachment" }
	}}}}

	POST /posts
	{
	  "mappings": {
	    "fs.files": {
	      "properties": {
		"filecontent": { "type": "attachment" }
	}}}}


When a file is inserted into mongodb via GridFS, monstache will detect the new file, use the mongodb api to retrieve the raw
content, and index a document into elasticsearch with the raw content stored in a `filecontent` field as a base64 
encoded string. The elasticsearch plugin will then extract text content from the raw content using 
[Apache Tika](https://tika.apache.org/)
, tokenize the text content, and allow you to query on the content of the file.

To test this feature of monstache you can simply use the [mongofiles](https://docs.mongodb.com/manual/reference/program/mongofiles/)
command to quickly add a file to mongodb via GridFS.  Continuing the example above one could issue the following command to put a 
file named `resume.docx` into GridFS and after a short time this file should be searchable in elasticsearch in the index `users`
under the type `fs.files`.


	mongofiles -d users put resume.docx


After a short time you should be able to query the contents of resume.docx in the users index in elasticsearch

	curl -XGET 'http://localhost:9200/users/fs.files/_search?q=golang'


