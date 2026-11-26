package main

import (
	"testing"
	"time"

	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func trackerTestOp(id string, t, i uint32) *gtm.Op {
	return &gtm.Op{Id: id, Namespace: "db.col", Timestamp: primitive.Timestamp{T: t, I: i}}
}

// The checkpoint only covers the completed prefix of received events, and
// the latest timestamp is held until a later one arrives.
func TestTrackerCheckpointAdvancesInOrder(t *testing.T) {
	tr := newOpTracker()
	a, b, c := trackerTestOp("a", 1, 1), trackerTestOp("b", 1, 2), trackerTestOp("c", 1, 3)
	tr.begin(a, false)
	tr.link(a, a) // handed to an index worker
	tr.done(a)    // event loop finished routing a
	tr.begin(b, false)
	tr.done(b)
	if got := tr.safe(); got.T != 0 {
		t.Fatalf("checkpoint %+v passed a, which is still in a worker", got)
	}
	tr.done(a) // index worker finished a
	if got := tr.safe(); got != a.Timestamp {
		t.Fatalf("checkpoint %+v, want %+v (b is held as the latest timestamp)", got, a.Timestamp)
	}
	tr.begin(c, false)
	if got := tr.safe(); got != b.Timestamp {
		t.Fatalf("checkpoint %+v, want %+v once a later event arrived", got, b.Timestamp)
	}
	tr.done(c)
	if len(tr.byOp) != 0 || len(tr.queue) != 0 {
		t.Fatalf("tracker leaked state: %d ops, %d queued", len(tr.byOp), len(tr.queue))
	}
}

// Events in a transaction share a timestamp, and resuming from a timestamp
// skips past it, so it must not be saved until the whole transaction is done.
func TestTrackerHoldsIncompleteTransaction(t *testing.T) {
	tr := newOpTracker()
	t1, t2, next := trackerTestOp("1", 5, 1), trackerTestOp("2", 5, 1), trackerTestOp("3", 5, 2)
	tr.begin(t1, false)
	tr.done(t1)
	if got := tr.safe(); got.T != 0 {
		t.Fatalf("checkpoint %+v saved before the rest of the transaction was received", got)
	}
	tr.begin(t2, false)
	tr.link(t2, t2)
	tr.done(t2)
	tr.begin(next, false)
	if got := tr.safe(); got.T != 0 {
		t.Fatalf("checkpoint %+v saved while a transaction event is still in a worker", got)
	}
	tr.done(t2)
	if got := tr.safe(); got != t1.Timestamp {
		t.Fatalf("checkpoint %+v, want the transaction's %+v once all of it is done", got, t1.Timestamp)
	}
}

// Resume tokens identify single events, so they advance per event even when
// events share a timestamp.
func TestTrackerTokensAdvancePerEvent(t *testing.T) {
	tr := newOpTracker()
	t1, t2 := trackerTestOp("1", 5, 1), trackerTestOp("2", 5, 1)
	t1.ResumeToken.StreamID, t1.ResumeToken.ResumeToken = "s", "tok1"
	t2.ResumeToken.StreamID, t2.ResumeToken.ResumeToken = "s", "tok2"
	tr.begin(t1, true)
	tr.done(t1)
	_, gen1 := tr.tokenSnapshot()
	tr.begin(t2, true)
	tr.done(t2)
	tokens, gen2 := tr.tokenSnapshot()
	if gen2 == gen1 || tokens["s"] != "tok2" {
		t.Fatalf("tokens didn't advance for a second same-timestamp event: gen %d -> %d, %v", gen1, gen2, tokens)
	}
}

// End to end through the event loop: an op still in an index worker must not
// be covered by the reported checkpoint.
func TestEventLoopCheckpointWaitsForIndexWorker(t *testing.T) {
	ic := &indexClient{
		config:      &configOptions{},
		sigH:        &sigHandler{clientStartedC: make(chan *indexClient, 1)},
		gtmCtx:      &gtm.OpCtxMulti{OpC: make(gtm.OpChan), ErrC: make(chan error)},
		opsConsumed: make(chan bool),
		statusReqC:  make(chan *statusRequest),
		doneC:       make(chan int),
		indexC:      make(chan *gtm.Op),
		tracker:     newOpTracker(),
		enabled:     true,
	}
	held := make(chan *gtm.Op, 2)
	go func() { // a slow index worker: takes ops and never finishes them
		for op := range ic.indexC {
			held <- op
		}
	}()
	go ic.eventLoop()

	for i, id := range []string{"1", "2"} {
		ic.gtmCtx.OpC <- &gtm.Op{Id: id, Operation: "i", Namespace: "db.col",
			Source: gtm.OplogQuerySource, Timestamp: primitive.Timestamp{T: 100, I: uint32(i + 1)},
			Data: map[string]interface{}{"_id": id}}
		<-held
	}
	req := &statusRequest{responseC: make(chan *statusResponse, 1)}
	ic.statusReqC <- req
	select {
	case status := <-req.responseC:
		if status.lastTs.T != 0 {
			t.Fatalf("checkpoint %+v covers ops still in an index worker", status.lastTs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no status response")
	}
}
