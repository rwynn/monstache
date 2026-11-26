package main

import (
	"sync"

	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// opTracker computes a resume checkpoint that is safe to persist.
//
// The event loop hands each change event to worker goroutines (index,
// file download, relate, post-process) which finish out of order. A
// checkpoint may only cover an event once every piece of work derived from it
// has been handed to the bulk processor, and only if the same is true of
// every event received before it. The tracker keeps received events in a FIFO
// with a reference count each; the checkpoint is the last event of the fully
// completed prefix.
//
// Usage: the event loop calls begin(op) when it receives an oplog/change
// event and done(op) when it has finished routing it. Before handing op (or a
// copy derived from it) to another goroutine, call link(parent, child); that
// goroutine calls done(child) when the work has reached the bulk processor.
// link and done are no-ops for untracked ops (e.g. direct reads).
type opTracker struct {
	mu         sync.Mutex
	queue      []*trackedOp
	byOp       map[*gtm.Op]*trackedOp
	safeTs     primitive.Timestamp
	safeTokens map[string]interface{}
	tokenGen   uint64 // bumped whenever safeTokens changes

	// Events in one transaction share a timestamp, and a timestamp checkpoint
	// resumes *after* that timestamp. So a timestamp only becomes safe once a
	// later event with a different timestamp has been received (the stream is
	// ordered, so nothing more with the old timestamp can follow). Until then
	// the completed prefix's timestamp is held here.
	lastRecvTs primitive.Timestamp
	heldTs     primitive.Timestamp
	hasHeld    bool
}

type trackedOp struct {
	op       *gtm.Op
	ts       primitive.Timestamp
	streamID string
	token    interface{}
	hasToken bool
	pending  int
	keys     []*gtm.Op
}

func newOpTracker() *opTracker {
	return &opTracker{
		byOp:       make(map[*gtm.Op]*trackedOp),
		safeTokens: make(map[string]interface{}),
	}
}

// begin registers an op received by the event loop. The event loop holds one
// reference until it calls done(op).
func (t *opTracker) begin(op *gtm.Op, withToken bool) {
	e := &trackedOp{
		op:      op,
		ts:      op.Timestamp,
		pending: 1,
		keys:    []*gtm.Op{op},
	}
	if withToken {
		e.streamID = op.ResumeToken.StreamID
		e.token = op.ResumeToken.ResumeToken
		e.hasToken = true
	}
	t.mu.Lock()
	if t.hasHeld && len(t.queue) == 0 && op.Timestamp != t.heldTs {
		// every event with the held timestamp is done and none can follow
		t.safeTs, t.hasHeld = t.heldTs, false
	}
	t.lastRecvTs = op.Timestamp
	t.queue = append(t.queue, e)
	t.byOp[op] = e
	t.mu.Unlock()
}

// link records child as outstanding work of parent's event. It must be called
// before child is handed to another goroutine. Returns false if parent is not
// tracked.
func (t *opTracker) link(parent, child *gtm.Op) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.byOp[parent]
	if e == nil {
		return false
	}
	e.pending++
	if _, ok := t.byOp[child]; !ok {
		t.byOp[child] = e
		e.keys = append(e.keys, child)
	}
	return true
}

// done releases one reference on op's event.
func (t *opTracker) done(op *gtm.Op) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.byOp[op]
	if e == nil {
		return
	}
	e.pending--
	if e.pending > 0 {
		return
	}
	for len(t.queue) > 0 && t.queue[0].pending == 0 {
		h := t.queue[0]
		t.queue[0] = nil
		t.queue = t.queue[1:]
		// resume tokens identify single events (including their position in
		// a transaction), so they can advance per event
		if h.hasToken {
			t.safeTokens[h.streamID] = h.token
			t.tokenGen++
		}
		next := t.lastRecvTs
		if len(t.queue) > 0 {
			next = t.queue[0].ts
		}
		if h.ts != next {
			t.safeTs, t.hasHeld = h.ts, false
		} else {
			t.heldTs, t.hasHeld = h.ts, true
		}
		for _, k := range h.keys {
			delete(t.byOp, k)
		}
	}
}

// tokenSnapshot returns a copy of the safe resume tokens and their
// generation, which changes whenever any token advances.
func (t *opTracker) tokenSnapshot() (map[string]interface{}, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tokens := make(map[string]interface{}, len(t.safeTokens))
	for k, v := range t.safeTokens {
		tokens[k] = v
	}
	return tokens, t.tokenGen
}

func (t *opTracker) safe() primitive.Timestamp {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.safeTs
}

func tsAfter(a, b primitive.Timestamp) bool {
	return a.T > b.T || (a.T == b.T && a.I > b.I)
}
