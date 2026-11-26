package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
)

// Permanent per-document failures (non-retryable 4xx) must not pause all
// indexing; transient ones still back off.
func TestBackoffOnlyOnTransientItemFailures(t *testing.T) {
	for _, tc := range []struct {
		status  int
		backoff bool
	}{
		{http.StatusBadRequest, false},
		{http.StatusForbidden, false},
		{http.StatusRequestTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusServiceUnavailable, true},
	} {
		ic := &indexClient{
			config:         &configOptions{},
			bulkBackoff:    elastic.NewConstantBackoff(time.Millisecond),
			bulkBackoffMax: time.Millisecond,
			bulkBackoffC:   make(chan time.Duration, 1),
		}
		ic.afterBulk()(1, nil, &elastic.BulkResponse{Errors: true, Items: []map[string]*elastic.BulkResponseItem{
			{"index": {Index: "idx", Id: "1", Status: tc.status}},
		}}, nil)
		got := false
		select {
		case <-ic.bulkBackoffC:
			got = true
		default:
		}
		if got != tc.backoff {
			t.Errorf("status %d: backed off = %v, want %v", tc.status, got, tc.backoff)
		}
	}
}

// A batch with only permanent failures means Elasticsearch is healthy, so it
// resets the consecutive-error count that sizes the next backoff.
func TestPermanentItemFailuresResetBackoffCount(t *testing.T) {
	ic := &indexClient{
		config:         &configOptions{},
		bulkBackoff:    elastic.NewConstantBackoff(time.Millisecond),
		bulkBackoffMax: time.Millisecond,
		bulkBackoffC:   make(chan time.Duration, 1),
	}
	ic.bulkErrs.Store(5)
	ic.afterBulk()(1, nil, &elastic.BulkResponse{Errors: true, Items: []map[string]*elastic.BulkResponseItem{
		{"index": {Index: "idx", Id: "1", Status: http.StatusBadRequest}},
	}}, nil)
	if n := ic.bulkErrs.Load(); n != 0 {
		t.Fatalf("consecutive error count = %d after a permanent-only batch, want 0", n)
	}
}
