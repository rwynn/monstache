package main

import (
	"encoding/json"
	"flag"
	elastigo "github.com/rwynn/elastigo/lib"
	"gopkg.in/mgo.v2"
	"testing"
	"time"
)

/*
This test requires the following processes to be running on localhost
	- elasticsearch
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
	flag.IntVar(&delay, "delay", 5, "Delay between operations in seconds")
	flag.Parse()
}

func DropTestDB(t *testing.T, session *mgo.Session) {
	db := session.DB("test")
	if err := db.DropDatabase(); err != nil {
		t.Error(err)
	}
}

func ValidateDocResponse(t *testing.T, doc map[string]string, resp elastigo.BaseResponse) {
	if resp.Id != doc["_id"] {
		t.Fatalf("elasticsearch id %s does not match mongo id %s", resp.Id, doc["_id"])
	}
	var src map[string]interface{}
	err := json.Unmarshal(*resp.Source, &src)
	if err != nil {
		t.Error(err)
	}
	if src["data"].(string) != doc["data"] {
		t.Fatalf("elasticsearch data %s does not match mongo data %s", src["data"], doc["data"])
	}
}

func TestInsert(t *testing.T) {
	elastic := elastigo.NewConn()
	session, err := mgo.Dial("localhost")
	if err != nil {
		t.Error(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := elastic.Get("test.test", "test", "1", nil); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Error(err)
		}
	} else {
		t.Error(err)
	}
}

func TestUpdate(t *testing.T) {
	elastic := elastigo.NewConn()
	session, err := mgo.Dial("localhost")
	if err != nil {
		t.Error(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := elastic.Get("test.test", "test", "1", nil); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Error(err)
		}
		doc["data"] = "updated"
		if err = col.UpdateId("1", doc); err != nil {
			t.Error(err)
		}
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := elastic.Get("test.test", "test", "1", nil); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Error(err)
		}
	} else {
		t.Error(err)
	}
}

func TestDelete(t *testing.T) {
	elastic := elastigo.NewConn()
	session, err := mgo.Dial("localhost")
	if err != nil {
		t.Error(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := elastic.Get("test.test", "test", "1", nil); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Error(err)
		}
		if err = col.RemoveId("1"); err != nil {
			t.Error(err)
		}
		time.Sleep(time.Duration(delay) * time.Second)
		_, err := elastic.Get("test.test", "test", "1", nil)
		if err != elastigo.RecordNotFound {
			t.Fatal("elasticsearch record not deleted")
		}
	} else {
		t.Error(err)
	}
}

func TestDropDatabase(t *testing.T) {
	elastic := elastigo.NewConn()
	session, err := mgo.Dial("localhost")
	if err != nil {
		t.Error(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := elastic.Get("test.test", "test", "1", nil); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Error(err)
		}
		db := session.DB("test")
		if err = db.DropDatabase(); err != nil {
			t.Error(err)
		}
		time.Sleep(time.Duration(delay) * time.Second)
		exists, err := elastic.IndicesExists("test.test")
		if err != nil {
			t.Error(err)
		}
		if exists {
			t.Fatal("elasticsearch index not deleted")
		}
	} else {
		t.Error(err)
	}
}

func TestDropCollection(t *testing.T) {
	elastic := elastigo.NewConn()
	session, err := mgo.Dial("localhost")
	if err != nil {
		t.Error(err)
	}
	defer session.Close()
	DropTestDB(t, session)
	col := session.DB("test").C("test")
	doc := make(map[string]string)
	doc["_id"] = "1"
	doc["data"] = "data"
	if err = col.Insert(doc); err == nil {
		time.Sleep(time.Duration(delay) * time.Second)
		if resp, err := elastic.Get("test.test", "test", "1", nil); err == nil {
			ValidateDocResponse(t, doc, resp)
		} else {
			t.Error(err)
		}
		if err = col.DropCollection(); err != nil {
			t.Error(err)
		}
		time.Sleep(time.Duration(delay) * time.Second)
		exists, err := elastic.IndicesExists("test.test")
		if err != nil {
			t.Error(err)
		}
		if exists {
			t.Fatal("elasticsearch index not deleted")
		}
	} else {
		t.Error(err)
	}
}
