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
