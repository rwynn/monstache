package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/olivere/elastic"
	"github.com/rwynn/gtm"
	"github.com/rwynn/monstache/monstachemap"
	"golang.org/x/net/context"
)

/*
This test requires the following processes to be running on localhost
	- elasticsearch v6.2+
	- mongodb
	- monstache

monstache should be run with the following settings to force bulk requests
 -elasticsearch-max-docs 1
 -elasticsearch-max-seconds 1

WARNING: This test is destructive for the database test in mongodb and
any index prefixed with test in elasticsearch

If the tests are failing you can try increasing the delay between when
an operation in mongodb is checked in elasticsearch by passing the delay
argument (number of seconds; defaults to 5)

go test -v -delay 10
*/

var delay int

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

var mongoUrl = getEnv("MONGO_DB_URL", "localhost:27017")

var elasticUrl = getEnv("ELASTIC_SEARCH_URL", "http://localhost:9200")

var elasticUrlConfig = elastic.SetURL(elasticUrl)
var elasticNoSniffConfig = elastic.SetSniff(false)

func init() {
	testing.Init()
	fmt.Printf("MongoDB Url: %v\nElasticsearch Url: %v\n", mongoUrl, elasticUrl)

	flag.IntVar(&delay, "delay", 3, "Delay between operations in seconds")
	flag.Parse()
}

func DropTestDB(t *testing.T, session *mgo.Session) {
	db := session.DB("test")
	if err := db.DropDatabase(); err != nil {
		t.Fatal(err)
	}
}

func ValidateDocResponse(t *testing.T, doc map[string]string, resp *elastic.GetResult) {
	if resp.Id != doc["_id"] {
		t.Fatalf("elasticsearch id %s does not match mongo id %s", resp.Id, doc["_id"])
	}
	var src map[string]interface{}
	err := json.Unmarshal(*resp.Source, &src)
	if err != nil {
		t.Fatal(err)
	}
	if src["data"].(string) != doc["data"] {
		t.Fatalf("elasticsearch data %s does not match mongo data %s", src["data"], doc["data"])
	}
}

func TestMarshallEmptyArray(t *testing.T) {
	var data = map[string]interface{}{
		"data": make([]interface{}, 0),
		"ints": []interface{}{1, 2, 3},
	}
	b, err := json.Marshal(monstachemap.ConvertMapForJSON(data))
	if err != nil {
		t.Fatalf("Unable to marshal object: %s", err)
	}
	expectedJSON := "{\"data\":[],\"ints\":[1,2,3]}"
	actualJSON := string(b)
	if actualJSON != expectedJSON {
		t.Fatalf("Expected %s but got %s", expectedJSON, actualJSON)
	}
}

func TestParseElasticsearchVersion(t *testing.T) {
	var err error
	c := &configOptions{}
	err = c.parseElasticsearchVersion("6.2.4")
	if err != nil {
		t.Fatal(err)
	}
	if c.ElasticMajorVersion != 6 {
		t.Fatalf("Expect major version 6")
	}
	if c.ElasticMinorVersion != 2 {
		t.Fatalf("Expect minor version 2")
	}
	err = c.parseElasticsearchVersion("")
	if err == nil {
		t.Fatalf("Expected error for blank version")
	}
	err = c.parseElasticsearchVersion("0")
	if err == nil {
		t.Fatalf("Expected error for invalid version")
	}
}

func TestExtractRelateData(t *testing.T) {
	data, err := extractData("foo", map[string]interface{}{"foo": 1})
	if err != nil {
		t.Fatalf("Expected nil error")
	}
	if data != 1 {
		t.Fatalf("Expected extracting foo value of 1")
	}
	data, err = extractData("foo.bar", map[string]interface{}{"foo": map[string]interface{}{"bar": 1}})
	if err != nil {
		t.Fatalf("Expected nil error")
	}
	if data != 1 {
		t.Fatalf("Expected extracting foo.bar value of 1")
	}
	data, err = extractData("foo.bar", map[string]interface{}{"foo": map[string]interface{}{"foo": 1}})
	if err == nil {
		t.Fatalf("Expected error for missing key")
	}
	data, err = extractData("foo", map[string]interface{}{"bar": 1})
	if err == nil {
		t.Fatalf("Expected error for missing key")
	}
	data, err = extractData("foo.bar", map[string]interface{}{"foo": []string{"a", "b", "c"}})
	if err == nil {
		t.Fatalf("Expected error for missing key")
	}
}

func TestBuildRelateSelector(t *testing.T) {
	sel := buildSelector("foo", 1)
	if sel == nil {
		t.Fatalf("Expected non-nil selector")
	}
	if len(sel) != 1 {
		t.Fatalf("Expected 1 foo key in selector")
	}
	if sel["foo"] != 1 {
		t.Fatalf("Expected matching foo to 1: %v", sel)
	}
	sel = buildSelector("foo.bar", 1)
	if sel == nil {
		t.Fatalf("Expected non-nil selector")
	}
	if len(sel) != 1 {
		t.Fatalf("Expected 1 foo key in selector")
	}
	bar, ok := sel["foo"].(bson.M)
	if !ok {
		t.Fatalf("Expected nested selector under foo")
	}
	if bar["bar"] != 1 {
		t.Fatalf("Expected matching foo.bar to 1: %v", sel)
	}
}

func TestOpIdToString(t *testing.T) {
	var result string
	var id float64 = 10.0
	var id2 int64 = 1
	var id3 float32 = 12.0
	op := &gtm.Op{Id: id}
	result = opIDToString(op)
	if result != "10" {
		t.Fatalf("Expected decimal dropped from float64 for ID")
	}
	op.Id = id2
	result = opIDToString(op)
	if result != "1" {
		t.Fatalf("Expected int64 converted to string")
	}
	op.Id = id3
	result = opIDToString(op)
	if result != "12" {
		t.Fatalf("Expected int64 converted to string")
	}
}

func TestPruneInvalidJSON(t *testing.T) {
	ts := time.Date(-1, time.November, 10, 23, 0, 0, 0, time.UTC)
	m := make(map[string]interface{})
	m["a"] = math.Inf(1)
	m["b"] = math.Inf(-1)
	m["c"] = math.NaN()
	m["d"] = 1
	m["e"] = ts
	m["f"] = []interface{}{m["a"], m["b"], m["c"], m["d"], m["e"]}
	out := fixPruneInvalidJSON("docId-1", m)
	if len(out) != 2 {
		t.Fatalf("Expected 4 fields to be pruned")
	}
	if out["d"] != 1 {
		t.Fatalf("Expected 1 field to remain intact")
	}
	if len(out["f"].([]interface{})) != 1 {
		t.Fatalf("Expected 4 array fields to be pruned")
	}
	if out["f"].([]interface{})[0] != 1 {
		t.Fatalf("Expected 1 array field to remain intact")
	}
}

func TestSetElasticClientScheme(t *testing.T) {
	c := &configOptions{
		ElasticUrls: []string{"https://example.com:9200"},
	}
	if c.needsSecureScheme() == false {
		t.Fatalf("secure scheme should be required")
	}
	c = &configOptions{
		ElasticUrls: []string{"http://example.com:9200"},
	}
	if c.needsSecureScheme() {
		t.Fatalf("secure scheme should not be required")
	}
	c = &configOptions{}
	if c.needsSecureScheme() {
		t.Fatalf("secure scheme should not be required")
	}
}

func TestParseSecureMongoUrl(t *testing.T) {
	c := &configOptions{MongoURL: "mongo://host:47/db?a=b&ssl=true&c=d"}
	c.setDefaults()
	if c.MongoURL != "mongo://host:47/db?a=b&c=d" {
		t.Fatalf("ssl param not removed from url")
	}
	if c.MongoDialSettings.Ssl == false {
		t.Fatalf("ssl not enabled")
	}
	c = &configOptions{MongoURL: "mongo://host:47/db?a=b&c=d&ssl=true"}
	c.setDefaults()
	if c.MongoURL != "mongo://host:47/db?a=b&c=d" {
		t.Fatalf("ssl param not removed from url")
	}
	if c.MongoDialSettings.Ssl == false {
		t.Fatalf("ssl not enabled")
	}
	c = &configOptions{MongoURL: "mongo://host:47/db?ssl=true"}
	c.setDefaults()
	if c.MongoURL != "mongo://host:47/db" {
		t.Fatalf("ssl param not removed from url")
	}
	if c.MongoDialSettings.Ssl == false {
		t.Fatalf("ssl not enabled")
	}
	c = &configOptions{MongoURL: "mongo://host:47/db?ssl=true&a=b"}
	c.setDefaults()
	if c.MongoURL != "mongo://host:47/db?a=b" {
		t.Fatalf("ssl param not removed from url")
	}
	if c.MongoDialSettings.Ssl == false {
		t.Fatalf("ssl not enabled")
	}
}

func TestInsert(t *testing.T) {
	client, err := elastic.NewClient(elasticUrlConfig, elasticNoSniffConfig)
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial(mongoUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := client.Get().Index("test.test").Type("_doc").Id("1").Do(context.Background()); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Fatal(err)
		}
	} else {
		t.Fatal(err)
	}
}

func TestUpdate(t *testing.T) {
	client, err := elastic.NewClient(elasticUrlConfig, elasticNoSniffConfig)
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial(mongoUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := client.Get().Index("test.test").Type("_doc").Id("1").Do(context.Background()); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Fatal(err)
		}
		doc["data"] = "updated"
		if err = col.UpdateId("1", doc); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := client.Get().Index("test.test").Type("_doc").Id("1").Do(context.Background()); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Fatal(err)
		}
	} else {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	client, err := elastic.NewClient(elasticUrlConfig, elasticNoSniffConfig)
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial(mongoUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := client.Get().Index("test.test").Type("_doc").Id("1").Do(context.Background()); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Fatal(err)
		}
		if err = col.RemoveId("1"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(delay) * time.Second)
		_, err := client.Get().Index("test.test").Type("_doc").Id("1").Do(context.Background())
		if !elastic.IsNotFound(err) {
			t.Fatal("clientsearch record not deleted")
		}
	} else {
		t.Fatal(err)
	}
}

func TestDropDatabase(t *testing.T) {
	client, err := elastic.NewClient(elasticUrlConfig, elasticNoSniffConfig)
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial(mongoUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := client.Get().Index("test.test").Type("_doc").Id("1").Do(context.Background()); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Fatal(err)
		}
		db := session.DB("test")
		if err = db.DropDatabase(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(delay) * time.Second)
		exists, err := client.IndexExists("test.test").Do(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("clientsearch index not deleted")
		}
	} else {
		t.Fatal(err)
	}
}

func TestDropCollection(t *testing.T) {
	client, err := elastic.NewClient(elasticUrlConfig, elasticNoSniffConfig)
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial(mongoUrl)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := client.Get().Index("test.test").Type("_doc").Id("1").Do(context.Background()); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Fatal(err)
		}
		if err = col.DropCollection(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(delay) * time.Second)
		exists, err := client.IndexExists("test.test").Do(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("clientsearch index not deleted")
		}
	} else {
		t.Fatal(err)
	}
}
