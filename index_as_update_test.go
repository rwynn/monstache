package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// With index-as-update, a pipeline configured in [[mapping]] must still be
// applied, which needs an index request rather than an update.
func TestIndexAsUpdateKeepsMappingPipeline(t *testing.T) {
	saved := mapIndexTypes
	t.Cleanup(func() { mapIndexTypes = saved })
	mapIndexTypes = map[string]*indexMapping{
		"db.col": {Namespace: "db.col", Index: "idx", Pipeline: "my-pipeline"},
	}

	var mu sync.Mutex
	var body strings.Builder
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		body.Write(b)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"took":1,"errors":false,"items":[{"index":{"_index":"idx","_id":"1","status":201}}]}`)
	}))
	defer srv.Close()
	client, err := elastic.NewClient(elastic.SetURL(srv.URL), elastic.SetSniff(false), elastic.SetHealthcheck(false))
	if err != nil {
		t.Fatal(err)
	}

	ic := &indexClient{config: &configOptions{
		IndexAsUpdate:   true,
		ElasticMaxConns: 1,
		ElasticMaxDocs:  -1,
		ElasticMaxBytes: elasticMaxBytesDefault,
	}}
	if ic.bulk, err = ic.newBulkProcessor(client); err != nil {
		t.Fatal(err)
	}
	op := &gtm.Op{
		Id: "1", Operation: "i", Namespace: "db.col",
		Source: gtm.OplogQuerySource, Timestamp: primitive.Timestamp{T: 100, I: 1},
		Data: map[string]interface{}{"_id": "1", "a": 1},
	}
	if err := ic.doIndexing(op); err != nil {
		t.Fatal(err)
	}
	ic.bulk.Flush()

	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(body.String(), `"pipeline":"my-pipeline"`) {
		t.Fatalf("mapping pipeline not applied with index-as-update; bulk body:\n%s", body.String())
	}
}
