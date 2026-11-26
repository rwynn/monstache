package main

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
)

func newBulkFailureTestClient() *indexClient {
	return &indexClient{
		config:         &configOptions{BackoffExemptStatusCodes: []int{http.StatusNotFound}},
		bulkBackoff:    elastic.NewConstantBackoff(time.Millisecond),
		bulkBackoffMax: time.Millisecond,
		bulkBackoffC:   make(chan time.Duration, 1),
	}
}

// A _bulk request that fails as a whole (response nil, err set) applied none
// of its actions, so it must back off rather than be treated as a success.
func TestAfterBulkBacksOffOnRequestFailure(t *testing.T) {
	ic := newBulkFailureTestClient()
	ic.bulkErrs.Store(3)
	ic.afterBulk()(1, nil, nil, errors.New("dial tcp: connection refused"))
	select {
	case <-ic.bulkBackoffC:
	default:
		t.Fatalf("no backoff after a failed _bulk request; error count reset to %d", ic.bulkErrs.Load())
	}
	if ic.bulkErrs.Load() != 4 {
		t.Fatalf("consecutive error count = %d, want 4", ic.bulkErrs.Load())
	}
}

// Whole-request failures with a backoff-exempt status don't back off.
func TestAfterBulkRequestFailureHonoursExemptStatus(t *testing.T) {
	ic := newBulkFailureTestClient()
	ic.afterBulk()(1, nil, nil, &elastic.Error{Status: http.StatusNotFound})
	select {
	case <-ic.bulkBackoffC:
		t.Fatal("backed off for an exempt status")
	default:
	}
}
