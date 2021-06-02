package monstachemap

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/olivere/elastic"
)

// plugins must import this package
// import "github.com/rwynn/monstache/monstachemap"

// plugins must implement a function named "Map" with the following signature
// func Map(input *monstachemap.MapperPluginInput) (output *monstachemap.MapperPluginOutput, err error)

// plugins can be compiled using go build -buildmode=plugin -o myplugin.so myplugin.go
// to enable the plugin start with monstache -mapper-plugin-path /path/to/myplugin.so

// MapperPluginInput is the input to the Map function
type MapperPluginInput struct {
	Document          map[string]interface{} // the original document from MongoDB
	Database          string                 // the origin database in MongoDB
	Collection        string                 // the origin collection in MongoDB
	Namespace         string                 // the entire namespace for the original document
	Operation         string                 // "i" for a insert or "u" for update
	Session           *mgo.Session           // MongoDB session handle
	UpdateDescription map[string]interface{} // map describing changes to the document
}

// MapperPluginOutput is the output of the Map function
type MapperPluginOutput struct {
	Document        map[string]interface{} // an updated document to index into Elasticsearch
	Index           string                 // the name of the index to use
	Type            string                 // the document type
	Routing         string                 // the routing value to use
	Drop            bool                   // set to true to indicate that the document should not be indexed but removed
	Passthrough     bool                   // set to true to indicate the original document should be indexed unchanged
	Parent          string                 // the parent id to use
	Version         int64                  // the version of the document
	VersionType     string                 // the version type of the document (internal, external, external_gte)
	Pipeline        string                 // the pipeline to index with
	RetryOnConflict int                    // how many times to retry updates before failing
	Skip            bool                   // set to true to indicate that the document should be ignored
	ID              string                 // override the _id of the indexed document; not recommended
}

// ProcessPluginInput is the input to the Process function
type ProcessPluginInput struct {
	MapperPluginInput
	ElasticClient        *elastic.Client
	ElasticBulkProcessor *elastic.BulkProcessor
	Timestamp            bson.MongoTimestamp
}
