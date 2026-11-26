package main

import (
	"testing"
	"time"

	"github.com/rwynn/gtm/v2"
)

// A paused cluster member (enabled=false) must still report that all ops were
// consumed once gtm closes its op channel, or shutdown hangs.
func TestEventLoopSignalsOpsConsumedWhilePaused(t *testing.T) {
	ic := &indexClient{
		config:      &configOptions{},
		sigH:        &sigHandler{clientStartedC: make(chan *indexClient, 1)},
		gtmCtx:      &gtm.OpCtxMulti{OpC: make(gtm.OpChan), ErrC: make(chan error)},
		opsConsumed: make(chan bool),
		statusReqC:  make(chan *statusRequest),
		doneC:       make(chan int),
		enabled:     false,
	}
	go ic.eventLoop()
	close(ic.gtmCtx.OpC) // what gtmCtx.Stop() does during shutdown
	close(ic.gtmCtx.ErrC)
	select {
	case <-ic.opsConsumed:
	case <-time.After(3 * time.Second):
		t.Fatal("opsConsumed never signalled after OpC closed: stopAllWorkers would hang")
	}
}
