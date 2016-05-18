package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/rwynn/gtm"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"os/signal"
	"regexp"
	"syscall"
)

var chunksRegex = regexp.MustCompile("\\.chunks$")
var systemsRegex = regexp.MustCompile("system\\..+$")
const mongoUrlDefault string = "localhost"
const resumeNameDefault string = "default"
const elasticMaxConnsDefault int = 10
const gtmChannelSizeDefault int = 100

type configOptions struct {
	MongoUrl        string `toml:"mongo-url"`
	ElasticUrl      string `toml:"elasticsearch-url"`
	ResumeName      string `toml:"resume-name"`
	NsRegex		string `toml:"namespace-regex"`
	NsExcludeRegex  string `toml:"namespace-exclude-regex"`
	Resume          bool
	Replay          bool
	ElasticMaxConns int `toml:"elasticsearch-max-conns"`
	ChannelSize	int `toml:"gtm-channel-size"`
	ConfigFile      string
}

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

func NotMonstache(op *gtm.Op) bool {
	return op.GetDatabase() != "monstache"
}

func NotChunks(op *gtm.Op) bool {
	return !chunksRegex.MatchString(op.GetCollection())
}

func NotSystem(op *gtm.Op) bool {
	return !systemsRegex.MatchString(op.GetCollection())
}

func FilterWithRegex(regex string) gtm.OpFilter {
	var validNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		return validNameSpace.MatchString(op.Namespace)
	}
}

func FilterInverseWithRegex(regex string) gtm.OpFilter {
	var invalidNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		return !invalidNameSpace.MatchString(op.Namespace)
	}
}

func SaveTimestamp(session *mgo.Session, op *gtm.Op, resumeName string) error {
	col := session.DB("monstache").C("monstache")
	doc := make(map[string]interface{})
	doc["ts"] = op.Timestamp
	_, err := col.UpsertId(resumeName, bson.M{"$set": doc})
	return err
}

func (configuration *configOptions) ParseCommandLineFlags() *configOptions {
	flag.StringVar(&configuration.MongoUrl, "mongo-url", "", "MongoDB connection URL")
	flag.StringVar(&configuration.ElasticUrl, "elasticsearch-url", "", "ElasticSearch connection URL")
	flag.IntVar(&configuration.ElasticMaxConns, "elasticsearch-max-conns", 0, "ElasticSearch max connections")
	flag.IntVar(&configuration.ChannelSize, "gtm-channel-size", 0, "Size of gtm channels")
	flag.StringVar(&configuration.ConfigFile, "f", "", "Location of configuration file")
	flag.BoolVar(&configuration.Resume, "resume", false, "True to capture the last timestamp of this run and resume on a subsequent run")
	flag.BoolVar(&configuration.Replay, "replay", false, "True to replay all events from the oplog and index them in elasticsearch")
	flag.StringVar(&configuration.ResumeName, "resume-name", "", "Name under which to load/store the resume state. Defaults to 'default'")
	flag.StringVar(&configuration.NsRegex, "namespace-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which match are synched to elasticsearch")
	flag.StringVar(&configuration.NsRegex, "namespace-exclude-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which do not match are synched to elasticsearch")
	flag.Parse()
	return configuration
}

func (configuration *configOptions) LoadConfigFile() *configOptions {
	if configuration.ConfigFile != "" {
		var tomlConfig configOptions
		if _, err := toml.DecodeFile(configuration.ConfigFile, &tomlConfig); err != nil {
			panic(err)
		}
		if configuration.MongoUrl == "" {
			configuration.MongoUrl = tomlConfig.MongoUrl
		}
		if configuration.ElasticUrl == "" {
			configuration.ElasticUrl = tomlConfig.ElasticUrl
		}
		if configuration.ElasticMaxConns == 0 {
			configuration.ElasticMaxConns = tomlConfig.ElasticMaxConns
		}
		if configuration.ChannelSize == 0 {
			configuration.ChannelSize = tomlConfig.ChannelSize
		}
		if !configuration.Replay && tomlConfig.Replay {
			configuration.Replay = true
		}
		if !configuration.Resume && tomlConfig.Resume {
			configuration.Resume = true
		}
		if configuration.Resume && configuration.ResumeName == "" {
			configuration.ResumeName = tomlConfig.ResumeName
		}
		if configuration.NsRegex == "" {
			configuration.NsRegex = tomlConfig.NsRegex
		}
	}
	return configuration
}

func (configuration *configOptions) SetDefaults() *configOptions {
	if configuration.MongoUrl == "" {
		configuration.MongoUrl = mongoUrlDefault
	}
	if configuration.ResumeName == "" {
		configuration.ResumeName = resumeNameDefault
	}
	if configuration.ElasticMaxConns == 0 {
		configuration.ElasticMaxConns = elasticMaxConnsDefault
	}
	if configuration.ChannelSize == 0 {
		configuration.ChannelSize = gtmChannelSizeDefault
	}
	return configuration
}

func main() {

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	configuration := &configOptions{}
	configuration.ParseCommandLineFlags().LoadConfigFile().SetDefaults()

	mongo, err := mgo.Dial(configuration.MongoUrl)
	if err != nil {
		fmt.Println(fmt.Sprintf("Unable to connect to mongodb using URL <%s>",
			configuration.MongoUrl))
		panic(err)
	}
	defer mongo.Close()
	mongo.SetMode(mgo.Monotonic, true)

	elastic := elastigo.NewConn()
	if configuration.ElasticUrl != "" {
		elastic.SetFromUrl(configuration.ElasticUrl)
	}
	indexer := elastic.NewBulkIndexer(configuration.ElasticMaxConns)
	indexer.Start()
	defer indexer.Stop()

	go func(mongo *mgo.Session, indexer *elastigo.BulkIndexer) {
		<-sigs
		mongo.Close()
		indexer.Flush()
		indexer.Stop()
		done <- true
	}(mongo, indexer)

	var after gtm.TimestampGenerator = nil
	if configuration.Resume {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			ts := gtm.LastOpTimestamp(session, options)
			if configuration.Replay {
				ts = 0
			} else {
				collection := session.DB("monstache").C("monstache")
				doc := make(map[string]interface{})
				collection.FindId(configuration.ResumeName).One(doc)
				if doc["ts"] != nil {
					ts = doc["ts"].(bson.MongoTimestamp)
				}
			}
			return ts
		}
	} else if configuration.Replay {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			return 0
		}
	}

	var filter gtm.OpFilter = nil
	filterChain := []gtm.OpFilter{ NotMonstache, NotSystem, NotChunks }
	if configuration.NsRegex != "" {
		filterChain = append(filterChain, FilterWithRegex(configuration.NsRegex))
	}
	if configuration.NsExcludeRegex != "" {
		filterChain = append(filterChain, FilterInverseWithRegex(configuration.NsExcludeRegex))
	}
	filter = gtm.ChainOpFilters(filterChain...)

	ops, errs := gtm.Tail(mongo, &gtm.Options{
		After:  after,
		Filter: filter,
		ChannelSize: configuration.ChannelSize,
	})
	exitStatus := 0
	for {
		select {
		case <-done:
			os.Exit(exitStatus)
		case err = <-errs:
			exitStatus = 1
			fmt.Println(err)
		case op := <-ops:
			objectId := OpIdToString(op)
			if op.IsDelete() {
				indexer.Delete(op.GetDatabase(), op.GetCollection(), objectId)
			} else {
				PrepareDataForIndexing(op.Data)
				indexer.Index(op.GetDatabase(), op.GetCollection(), objectId, "", "", nil, op.Data)
			}
			if configuration.Resume {
				err := SaveTimestamp(mongo, op, configuration.ResumeName)
				if err != nil {
					exitStatus = 1
					fmt.Println(err)
				}
			}
		}
	}
}
