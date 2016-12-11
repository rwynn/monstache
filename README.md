# monstache
a go daemon which syncs mongodb to elasticsearch in near realtime

<img src="https://raw.github.com/rwynn/monstache/master/images/monstache.png"/>

### Install ###

You can download monstache binaries for Linux from the [Releases](https://github.com/rwynn/monstache/releases) page.

Or you can build monstache from source using go get

	go get github.com/rwynn/monstache

### Getting Started ###

Since monstache uses the mongodb oplog to tail events it is required that mongodb is configured to produce an oplog.

This can be ensured by doing one of the following:
+ Setting up [replica sets](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/)
+ Passing --master to the mongod process
+ Setting the following in /etc/mongod.conf

	```
	master = true
	```

You will also want to ensure that automatic index creation is not disabled in elasticsearch.yml.

monstache is not bi-directional.  It only syncs from mongodb to elasticsearch.

### Usage ###

monstache \[-f PATH-TO-TOML\] \[options\]

All command line arguments are optional.  With no arguments monstache expects to connect to mongodb and
elasticsearch on localhost using the default ports. 

If the -f option is supplied the argument value should be the file path of a TOML config file.

A sample TOML config file looks like this:

	gzip = true
	mongo-url = "mongodb://someuser:password@localhost:40001"
	mongo-pem-file = "/path/to/mongoCert.pem"
	elasticsearch-url = "http://someuser:password@localhost:9200"
	elasticsearch-max-conns = 10
	dropped-collections = true
	dropped-databases = true
	replay = false
	resume = true
	resume-write-unsafe = false
	resume-name = "default"
	namespace-regex = "^mydb\.(mycollection|\$cmd)$"
	namespace-exclude-regex = "^mydb\.(ignorecollection|\$cmd)$"
	gtm-channel-size = 200
	index-files = true
	file-highlighting = true
	file-namespaces = ["users.fs.files"]
	verbose = true
	
All options in the config file above also work if passed explicity by the same name to the monstache command

Arguments supplied on the command line override settings in a config file

The following defaults are used for missing config values:

	gzip -> false
	mongo-url -> localhost
	mongo-pem-file -> nil
	elasticsearch-url -> localhost
	elasticsearch-max-conns -> 10
	elasticsearch-retry-seconds -> 0 
	elasticsearch-max-docs -> 100
	elasticsearch-max-bytes -> 16384
	elasticsearch-max-seconds -> 5
	dropped-databases -> true
	dropped-collections -> true
	replay -> false
	resume -> false
	resume-write-unsafe -> false
	resume-name -> default
	namespace-regex -> nil
	namespace-exclude-regex -> nil
	gtm-channel-size -> 100
	index-files -> false
	max-file-size -> 0
	file-highlighting -> false
	file-namespaces -> nil
	worker -> nil
	workers -> nil
	verbose -> false

When `gzip` is true, monstache will compress requests to elasticsearch to increase performance.  Compression is enabled
by default in elasticsearch 5, but is disabled by default in previous elasticsearch versions.  If you enable `gzip` in
monstache and are using elasticsearch prior to version 5 you will need to update the elasticsearch config file to
set `http.compression: true`. Enabling `gzip` is recommended especially if you enable the `index-files` setting.

When `resume` is true, monstache writes the timestamp of mongodb operations it has successfully synced to elasticsearch
to the collection `monstache.monstache`.  It also reads this value from that collection when it starts in order to replay
events which it might have missed because monstache was stopped. monstache uses the value of `resume-name` as a key when
storing and retrieving timestamps.  If `resume` is true but `resume-name` is not supplied this key defaults to `default`.

When `replay` is true, monstache replays all events from the beginning of the mongodb oplog and syncs them to elasticsearch.

When `resume` and `replay` are both true, monstache replays all events from the beginning of the mongodb oplog, syncs them
to elasticsearch and also writes the timestamp of processed events to `monstache.monstache`. 

When neither `resume` nor `replay` are true, monstache reads the last timestamp in the oplog and starts listening for events
occurring after this timestamp.  Timestamps are not written to `monstache.monstache`.  This is the default behavior. 

When `resume-write-unsafe` is true monstache sets the safety mode of the mongodb session such that writes are fire and forget.
This speeds up writing of timestamps used to resume synching in a subsequent run of monstache.  This speed up comes at the cost
of no error checking on the write of the timestamp.  Since errors writing the last synched timestamp are only logged by monstache
and do not stop execution it's not unreasonable to set this to true to get a speedup.  

When `namespace-regex` is given this regex is tested against the namespace, `database.collection`, of the event. If
the regex matches monstache continues processing event filters, otherwise it drops the event. By default monstache
processes events in all databases and all collections with the exception of the reserved database `monstache`, any
collections suffixed with `.chunks`, and the system collections. For more information see the section [Namespaces](#namespaces).

When `namespace-exclude-regex` is given this regex is tested against the namespace, `database.collection`, of the event. If
the regex matches monstache ignores the event, otherwise it continues processing event filters. By default monstache
processes events in all databases and all collections with the exception of the reserved database `monstache`, any
collections suffixed with `.chunks`, and the system collections. For more information see the section [Namespaces](#namespaces).

When `gtm-channel-size` is given it controls the size of the go channels created for processing events.  When many events
are processed at once a larger channel size may prevent blocking in gtm.

When `mongo-pem-file` is given monstache will use the given file path to add a local certificate to x509 cert
pool when connecting to mongodb. This should only be used when mongodb is configured with SSL enabled.

When `index-files` is true monstache will index the raw content of files stored in GridFS into elasticsearch as an attachment type.
By default `index-files` is false meaning that monstache will only index metadata associated with files stored in GridFS.
In order for `index-files` to index the raw content of files stored in GridFS you must install a plugin for elasticsearch.
For versions of elasticsearch prior to version 5, you should install the [mapper-attachments](https://www.elastic.co/guide/en/elasticsearch/plugins/2.3/mapper-attachments.html) plugin.  In version 5 or greater
of elasticsearch the mapper-attachment plugin is deprecated and you should install the [ingest-attachment](https://www.elastic.co/guide/en/elasticsearch/plugins/master/ingest-attachment.html) plugin instead.
For further information on how to configure monstache to index content from grids, see the section [Indexing Gridfs Files](#files).

When `max-file-size` is greater than 0 monstache will not index the content of GridFS files that exceed this limit in bytes.

The `file-namespaces` config must be set when `index-files` is enabled.  `file-namespaces` must be set to an array of mongodb
namespace strings.  Files uploaded through gridfs to any of the namespaces in `file-namespaces` will be retrieved and their
raw content indexed into elasticsearch via either the mapper-attachments or ingest-attachment plugin. 

When `file-highlighting` is true monstache will enable the ability to return highlighted keywords in the extracted text of files
for queries on files which were indexed in elasticsearch from gridfs.

When `verbose` is true monstache with enable debug logging including a trace of requests to elasticsearch

When `elasticseach-retry-seconds` is greater than 0 a failed request to elasticsearch with retry the request after the given number of seconds

When `elasticsearch-max-docs` is given a bulk index request to elasticsearch will be forced when the buffer reaches the given number of documents

When `elasticsearch-max-bytes` is given a bulk index request to elasticsearch will be forced when the buffer reaches the given number of bytes

When `elasticsearch-max-seconds` is given a bulk index request to elasticsearch will be forced when a request has not been made in the given number of seconds

When `dropped-databases` is false monstache will not delete the mapped indexes in elasticsearch if a mongodb database is dropped

When `dropped-collections` is false monstache will not delete the mapped index in elasticsearch if a mongodb collection is dropped

When `worker` is given monstache will enter multi-worker mode and will require you to also provide the config option `workers`.  Use this mode to run
multiple monstache processes and distribute the work between them.  In this mode monstache will ensure that each mongo document id always goes to the
same worker and none of the other workers. See the section [workers](#workers) for more information.

### Config Syntax ###

For information on the syntax of the mongodb URL see [Standard Connection String Format](https://docs.mongodb.com/v3.0/reference/connection-string/#standard-connection-string-format)

The elasticsearch URL should point to where elasticsearch's RESTful API is configured

<a name="namespaces"></a>
### Namespaces ###

When a document is inserted, updated, or deleted in mongodb a document is appended to the oplog representing the event.  This document has a field `ns` which is the namespace.  For inserts, updates, and deletes the namespace is the database name and collection name of the document changed joined by a dot. E.g. for `use test; db.foo.insert({hello: "world"});` the namespace for the event in the oplog would be `test.foo`.

In addition to inserts, updates, and deletes monstache also supports database and collection drops.  When a database or collection is dropped in mongodb an event is appended to the oplog.  Like the other types of changes this event has a field `ns` representing the namespace.  However, for drops the namespace is the database name and the string `$cmd` joined by a dot.  E.g. for `use test; db.foo.drop()` the namespace for the event in the oplog would be `test.$cmd`.  

When configuring namespaces in monstache you will need to account for both cases.  Specifically, be careful if you have configured `dropped-databases|dropped-collections=true` AND you also have a `namespace-regex` set.  If your namespace regex does not take into account the `db.$cmd` namespace the event may be filtered and the elasticsearch index not deleted on a drop.

### Document Mapping ###

When indexing documents from mongodb into elasticsearch the mapping is as follows:

	mongodb database name . mongodb collection name -> elasticsearch index name
	mongodb collection name -> elasticsearch type
	mongodb document _id -> elasticsearch document _id

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

Make sure that automatic index creation is not disabled in elasticsearch.yml.

If automatic index creation must be controlled, whitelist any indexes in elasticsearch.yml that monstache will create.

Note that when monstache maps index and type names for ElasticSearch it does normalization based on the 
[Validity Rules](https://github.com/elastic/elasticsearch/issues/6736).  This includes making sure index names are
all lowercase and that index, types, and ids do not being with an underscore.

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
manipulate the document to drop fields, add fields, or augment the existing fields.

The `this` reference in the mapping function is assigned to the document from mongodb.  

When the return value from the mapping function is an `object` then that mapped object is what actually gets indexed in elasticsearch.
For these purposes an object is a javascript non-primitive, excluding `Function`, `Array`, `String`, `Number`, `Boolean`, `Date`, `Error` and `RegExp`.

If the return value from the mapping function is not an `object` per the definition above then the result is converted into a `boolean`
and if the boolean value is `false` then that indicates to monstache that you would not like to index the document. If the boolean value is `true` then
the original document from mongodb gets indexed in elasticsearch.

This allows you to return false or null if you have implemented soft deletes in mongodb.

	namespace = "db.collection"
	script = """
	module.exports = function(doc) {
		if (!!doc.deletedAt) {
			return false;
		}
		return true;
	}
	"""

In the above example monstache will index any document except the ones with a `deletedAt` property.  If the document is first
inserted without a `deletedAt` property, but later updated to include the `deletedAt` property then monstache will remove the
previously indexed document from the elasticsearch index. 

Note you could also return `doc` above instead of returning `true` and get the same result, however, it's a slight performance gain
to simply return `true` when not changing the document because you are not copying data in that case.

You may have noticed that in the first example above the exported mapping function closes over a var named `counter`.  You can
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

	file-highlighting = true

The above configuration tells monstache that you wish to index the raw content of GridFS files in the `users` and `posts`
mongodb databases. By default, mongodb uses a bucket named `fs`, so if you just use the defaults your collection name will
be `fs.files`.  However, if you have customized the bucket name, then your file collection would be something like `mybucket.files`
and the entire namespace would be `users.mybucket.files`.

When you configure monstache this way it will perform an additional operation at startup to ensure the destination indexes in
elasticsearch have a field named `file` with a type mapping of `attachment`.  

For the example TOML configuration above, monstache would initialize 2 indices in preparation for indexing into
elasticsearch by issuing the following REST commands:

For elasticsearch versions prior to version 5...

	POST /users.fs.files
	{
	  "mappings": {
	    "fs.files": {
	      "properties": {
		"file": { "type": "attachment" }
	}}}}

	POST /posts.fs.files
	{
	  "mappings": {
	    "fs.files": {
	      "properties": {
		"file": { "type": "attachment" }
	}}}}

For elasticsearch version 5 and above...

	PUT /_ingest/pipeline/attachment
	{
	  "description" : "Extract file information",
	  "processors" : [
	    {
	      "attachment" : {
		"field" : "file"
	      }
	    }
	  ]
	}

When a file is inserted into mongodb via GridFS, monstache will detect the new file, use the mongodb api to retrieve the raw
content, and index a document into elasticsearch with the raw content stored in a `file` field as a base64 
encoded string. The elasticsearch plugin will then extract text content from the raw content using 
[Apache Tika](https://tika.apache.org/), tokenize the text content, and allow you to query on the content of the file.

To test this feature of monstache you can simply use the [mongofiles](https://docs.mongodb.com/manual/reference/program/mongofiles/)
command to quickly add a file to mongodb via GridFS.  Continuing the example above one could issue the following command to put a 
file named `resume.docx` into GridFS and after a short time this file should be searchable in elasticsearch in the index `users.fs.files`.

	mongofiles -d users put resume.docx

After a short time you should be able to query the contents of resume.docx in the users index in elasticsearch

	curl -XGET 'http://localhost:9200/users.fs.files/_search?q=golang'

If you would like to see the text extracted by Apache Tika you can project the appropriate sub-field

For elasticsearch versions prior to version 5...

	curl localhost:9200/users.fs.files/_search?pretty -d '{
		"fields": [ "file.content" ],
		"query": {
			"match": {
				"_all": "golang"
			}
		}
	}'

For elasticsearch version 5 and above...

	curl localhost:9200/users.fs.files/_search?pretty -d '{
		"_source": [ "attachment.content" ],
		"query": {
			"match": {
				"_all": "golang"
			}
		}
	}'

When `file-highlighting` is enabled you can add a highlight clause to your query

For elasticsearch versions prior to version 5...

	curl localhost:9200/users.fs.files/_search?pretty -d '{
		"fields": ["file.content"],
		"query": {
			"match": {
				"file.content": "golang"
			}
		},
		"highlight": {
			"fields": {
				"file.content": {
				}
			}
		}
	}'

For elasticsearch version 5 and above...

	curl localhost:9200/users.fs.files/_search?pretty -d '{
		"_source": ["attachment.content"],
		"query": {
			"match": {
				"attachment.content": "golang"
			}
		},
		"highlight": {
			"fields": {
				"attachment.content": {
				}
			}
		}
	}'


The highlight response will contain emphasis on the matching terms

For elasticsearch versions prior to version 5...

	"hits" : [ {
		"highlight" : {
			"file.content" : [ "I like to program in <em>golang</em>.\n\n" ]
		}
	} ]

For elasticsearch version 5 and above...

	"hits" : [{
		"highlight" : {
			"attachment.content" : [ "I like to program in <em>golang</em>." ]
		}
	}]

<a name="workers"></a>
### Multiple Workers ###

You can run multiple monstache processes and distribute the work between them.  First configure
the names of all the workers in a shared config.toml file.

	workers = ["Tom", "Dick", "Harry"]

In this case we have 3 workers.  Now we can start 3 monstache processes and give each one of the worker
names.

	monstache -f config.toml -worker Tom
	monstache -f config.toml -worker Dick
	monstache -f config.toml -worker Harry

monstache will hash the id of each document using consistent hashing so that each id is handled by only
one of the available workers.

