# monstache
a go daemon which syncs mongodb to elasticsearch in near realtime

<img src="https://raw.github.com/rwynn/monstache/master/images/monstache.png"/>

### Install ###

You can download monstache binaries from the [Releases](https://github.com/rwynn/monstache/releases) page.

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

### Documentation ###

See the [monstache site](https://rwynn.github.io/monstache-site/) for information on configuration and usage.

