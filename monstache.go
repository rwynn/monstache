package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/rwynn/gtm"
	"stathat.com/c/jconfig"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func OpIdToString(op *gtm.Op) string {
	var opIdStr string
	switch op.Id.(type) {
	case bson.ObjectId:
		opIdStr = op.Id.(bson.ObjectId).Hex()
	default:
		opIdStr = fmt.Sprintf("%v", op.Id)
	}
	return opIdStr
}

func PrepareDataForIndexing(data map[string]interface{}) {
	delete(data, "_id")
	delete(data, "_type")
	delete(data, "_index")
	delete(data, "_score")
	delete(data, "_source")
}

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	const mongoUrlDefault string = "localhost"
	const elasticMaxConnsDefault int = 10
	var mongoUrl, elasticUrl, configFile string
	var elasticMaxConns int
	flag.StringVar(&mongoUrl, "mongo-url", "", "MongoDB connection URL")
	flag.StringVar(&elasticUrl, "elasticsearch-url", "", "ElasticSearch connection URL")
	flag.IntVar(&elasticMaxConns, "elasticsearch-max-conns", -1, "ElasticSearch max connections")
	flag.StringVar(&configFile, "f", "", "Location of configuration file")
	flag.Parse()
	if configFile != "" {
		config := jconfig.LoadConfig(configFile)
		if mongoUrl == "" {
			mongoUrl = config.GetString("mongo-url")
		}
		if elasticUrl == "" {
			elasticUrl = config.GetString("elasticsearch-url")
		}
		if elasticMaxConns == -1 {
			elasticMaxConns = config.GetInt("elasticsearch-max-conns")
		}
	}
	if mongoUrl == "" {
		mongoUrl = mongoUrlDefault
	}
	mongo, err := mgo.Dial(mongoUrl)
	if err != nil {
		panic(err)
	}
	defer mongo.Close()
	mongo.SetMode(mgo.Monotonic, true)
	elastic := elastigo.NewConn()
	if elasticUrl != "" {
		elastic.SetFromUrl(elasticUrl)
	}
	if elasticMaxConns == -1 {
		elasticMaxConns = elasticMaxConnsDefault
	}
	indexer := elastic.NewBulkIndexer(elasticMaxConns)
	indexer.Start()
	defer indexer.Stop()
	go func(mongo *mgo.Session, indexer *elastigo.BulkIndexer) {
		<-sigs
		mongo.Close()
		indexer.Stop()
		done <- true
	}(mongo, indexer)
	ops, errs := gtm.Tail(mongo, nil)
	for {
		select {
		case <-done:
			os.Exit(0)
		case err = <-errs:
			fmt.Println(err)
		case op := <-ops:
			objectId := OpIdToString(op)
			if op.IsDelete() {
				indexer.Delete(op.GetDatabase(), op.GetCollection(), objectId)
			} else {
				PrepareDataForIndexing(op.Data)
				indexer.Index(op.GetDatabase(), op.GetCollection(), objectId, "", "", nil, op.Data)
			}
		}
	}
}
