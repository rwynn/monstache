package main

import (
	"encoding/json"
	"flag"
	"github.com/globalsign/mgo"
	"github.com/olivere/elastic"
	"golang.org/x/net/context"
	"testing"
	"time"
)

/*
This test requires the following processes to be running on localhost
	- elasticsearch v6.2+
	- mongodb
	- monstache

WARNING: This test is destructive for the database test in mongodb and
any index prefixed with test in elasticsearch

If the tests are failing you can try increasing the delay between when
an operation in mongodb is checked in elasticsearch by passing the delay
argument (number of seconds; defaults to 5)

go test -v -delay 10
*/

var delay int

func init() {
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
	client, err := elastic.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial("localhost")
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
	client, err := elastic.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial("localhost")
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
	client, err := elastic.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial("localhost")
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
	client, err := elastic.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial("localhost")
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
	client, err := elastic.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	session, err := mgo.Dial("localhost")
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
