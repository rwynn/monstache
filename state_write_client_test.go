package main

// Requires MongoDB; skipped unless MONSTACHE_TEST_MONGO_URL is set, e.g.
//
//	MONSTACHE_TEST_MONGO_URL=mongodb://localhost:27017 go test -run TestStateUsesWriteClient .
//
// It works in its own uniquely named database and drops it afterwards.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rwynn/gtm/v2"
	"github.com/rwynn/monstache/v6/pkg/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// monstache's own state is written through the write client
// (mongo-write-url), so it must also be read through it. Here the read
// client is disconnected, so any state access through it fails.
func TestStateUsesWriteClient(t *testing.T) {
	url := os.Getenv("MONSTACHE_TEST_MONGO_URL")
	if url == "" {
		t.Skip("set MONSTACHE_TEST_MONGO_URL to run")
	}
	writer, err := (&configOptions{}).dialMongo(url)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := (&configOptions{}).dialMongo(url)
	if err != nil {
		t.Fatal(err)
	}
	reader.Disconnect(context.Background())
	db := fmt.Sprintf("monstache_test_%d", time.Now().UnixNano())
	defer func() {
		writer.Database(db).Drop(context.Background())
		writer.Disconnect(context.Background())
	}()

	config := &configOptions{
		ConfigDatabaseName: db,
		ResumeName:         "test",
		Resume:             true,
		DeleteStrategy:     statefulDeleteStrategy,
		DirectReadNs:       []string{"db.col"},
	}
	ic := &indexClient{config: config, mongo: reader, mongoWriter: writer, tokens: bson.M{}}

	// cluster ownership
	if err := ic.ensureClusterTTL(); err != nil {
		t.Fatal(err)
	}
	if enabled, err := ic.enableProcess(); err != nil || !enabled {
		t.Fatalf("enableProcess = %v, %v; want true, nil", enabled, err)
	}
	// routing metadata
	if err := ic.setIndexMeta("db.col", "1", &indexingMeta{Routing: "r1"}); err != nil {
		t.Fatal(err)
	}
	if got := ic.getIndexMeta("db.col", "1"); got.Routing != "r1" {
		t.Fatalf("getIndexMeta = %+v; want routing r1", got)
	}
	// direct read state
	if err := ic.saveDirectReadNamespaces(); err != nil {
		t.Fatal(err)
	}
	if left, err := ic.filterDirectReadNamespaces([]string{"db.col", "db.other"}); err != nil || len(left) != 1 || left[0] != "db.other" {
		t.Fatalf("filterDirectReadNamespaces = %v, %v; want [db.other]", left, err)
	}
	// resume timestamp: gtm passes its own (possibly per-shard) client
	ic.lastTs = primitive.Timestamp{T: 1700000000, I: 3}
	if err := ic.saveTimestamp(); err != nil {
		t.Fatal(err)
	}
	ic.oplogTsResolver = oplog.TimestampResolverSimple{}
	if ts, err := ic.buildTimestampGen()(reader, &gtm.Options{}); err != nil || ts != (primitive.Timestamp{T: 1700000000, I: 4}) {
		t.Fatalf("timestamp generator = %+v, %v; want the saved timestamp + 1", ts, err)
	}
	// resume token
	config.ResumeStrategy = tokenResumeStrategy
	ic.tokens = bson.M{"s1": "tok"}
	if err := ic.saveTokens(); err != nil {
		t.Fatal(err)
	}
	if tok, err := ic.buildTokenGen()(reader, "s1", &gtm.Options{}); err != nil || tok != "tok" {
		t.Fatalf("token generator = %v, %v; want tok", tok, err)
	}
}
