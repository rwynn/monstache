package main

// Requires MongoDB; skipped unless MONSTACHE_TEST_MONGO_URL is set. Works in
// its own uniquely named database and drops it afterwards.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Related docs must carry the triggering event's timestamp (used for their
// external version), not the monstache host's wall clock.
func TestRelatedDocUsesEventTimestamp(t *testing.T) {
	url := os.Getenv("MONSTACHE_TEST_MONGO_URL")
	if url == "" {
		t.Skip("set MONSTACHE_TEST_MONGO_URL to run")
	}
	client, err := (&configOptions{}).dialMongo(url)
	if err != nil {
		t.Fatal(err)
	}
	db := fmt.Sprintf("monstache_test_%d", time.Now().UnixNano())
	defer func() {
		client.Database(db).Drop(context.Background())
		client.Disconnect(context.Background())
	}()
	if _, err := client.Database(db).Collection("child").InsertOne(context.Background(), bson.M{"_id": "c1", "parent": "p1"}); err != nil {
		t.Fatal(err)
	}
	saved := relates
	defer func() { relates = saved }()
	relates = map[string][]*relation{
		db + ".parent": {{Namespace: db + ".parent", WithNamespace: db + ".child",
			SrcField: "_id", MatchField: "parent", db: db, col: "child"}},
	}

	ic := &indexClient{config: &configOptions{}, mongo: client, indexC: make(chan *gtm.Op, 1)}
	eventTs := primitive.Timestamp{T: 1600000000, I: 9} // well behind the wall clock
	root := &gtm.Op{Id: "p1", Operation: "u", Namespace: db + ".parent",
		Source: gtm.OplogQuerySource, Timestamp: eventTs, Data: map[string]interface{}{"_id": "p1"}}
	if err := ic.processRelated(root); err != nil {
		t.Fatal(err)
	}
	select {
	case rop := <-ic.indexC:
		if rop.Timestamp != eventTs {
			t.Fatalf("related doc timestamp %+v, want the triggering event's %+v", rop.Timestamp, eventTs)
		}
	default:
		t.Fatal("related doc was not sent for indexing")
	}
}
