package main

import (
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
)

// The bulk worker's after-callback must not block when the event loop isn't
// receiving on bulkBackoffC (e.g. because it is waiting in bulk.Flush() for
// this very worker), otherwise the two deadlock.
func TestAfterBulkBackoffSignalDoesNotBlock(t *testing.T) {
	ic := &indexClient{
		config:         &configOptions{},
		bulkBackoff:    elastic.NewConstantBackoff(time.Millisecond),
		bulkBackoffMax: time.Millisecond,
		bulkBackoffC:   make(chan time.Duration, 1),
	}
	after := ic.afterBulk()
	failed := &elastic.BulkResponse{Errors: true, Items: []map[string]*elastic.BulkResponseItem{
		{"index": {Index: "idx", Id: "1", Status: 400}},
	}}
	done := make(chan struct{})
	go func() {
		// nobody receives on bulkBackoffC; two failures fill the buffer
		after(1, nil, failed, nil)
		after(2, nil, failed, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("bulk after-callback blocked sending the backoff signal")
	}
}
