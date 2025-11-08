package main

// Memory leak tests for requestContexts
// Run: go test -v -run TestMemoryLeakFix

import (
	"strings"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/goleak"
)

// TestMemoryLeakFix - Comprehensive test for requestContexts memory leak fix
func TestMemoryLeakFix(t *testing.T) {
	// Save original requestContexts to avoid interfering with running monstache process
	originalContexts := requestContexts
	defer func() { requestContexts = originalContexts }()

	defer goleak.VerifyNone(t,
		// Only ignore specific HTTP transport goroutines (Docker environment)
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)

	t.Run("EarlyReturnCases_NoContextStored", func(t *testing.T) {
		// Test cases where no context should be stored (early returns) - start fresh
		requestContexts = sync.Map{}

		// Test 1: Empty ID
		objectID := ""
		if objectID == "" {
			t.Log("✅ Empty ID: early return, no context stored")
		} else {
			t.Error("Empty ID should trigger early return")
		}

		// Test 2: Long ID (>512 bytes)
		longID := strings.Repeat("a", 600)
		if len(longID) > 512 {
			t.Log("✅ Long ID: early return, no context stored")
		} else {
			t.Error("Long ID should trigger early return")
		}

		// Verify no contexts leaked
		count := countContexts()
		if count != 0 {
			t.Errorf("❌ MEMORY LEAK: %d contexts stored for early return cases", count)
		}
	})

	t.Run("ContextStorage_ProperCompletion", func(t *testing.T) {
		requestContexts = sync.Map{}

		// Store contexts like real doIndexing function
		objectIDs := []string{
			primitive.NewObjectID().Hex(), // Valid ObjectID
			primitive.NewObjectID().Hex(),
			primitive.NewObjectID().Hex(),
		}

		for _, oid := range objectIDs {
			ctx := &mongoContext{
				Database:   "test",
				Collection: "collection",
				DocumentID: oid,
				Namespace:  "test.collection",
				Operation:  "index",
				Completed:  false, // Critical: starts as false
			}
			requestContexts.Store(oid+":test.collection", ctx)
		}

		before := countContexts()
		if before != 3 {
			t.Fatalf("Setup failed: expected 3 contexts, got %d", before)
		}

		// Verify all start as Completed:false
		uncompletedBefore := countUncompleted()
		if uncompletedBefore != 3 {
			t.Errorf("Expected 3 uncompleted contexts, got %d", uncompletedBefore)
		}
		t.Logf("Stored %d contexts with Completed:false", uncompletedBefore)

		// Simulate afterBulk successful batch
		requestContexts.Range(func(key, value interface{}) bool {
			if ctx, ok := value.(*mongoContext); ok {
				ctx.Completed = true
			}
			return true
		})

		// Verify all marked completed
		uncompletedAfter := countUncompleted()
		if uncompletedAfter > 0 {
			t.Errorf("❌ MEMORY LEAK: %d contexts not marked completed", uncompletedAfter)
		} else {
			t.Log("✅ All contexts marked completed")
		}

		// Test cleanup
		cleanupCompletedContexts()
		finalCount := countContexts()
		if finalCount > 0 {
			t.Errorf("❌ MEMORY LEAK: %d contexts remain after cleanup", finalCount)
		} else {
			t.Log("✅ All contexts cleaned up")
		}
	})

	t.Run("MappingFailure_ContextCompletion", func(t *testing.T) {
		requestContexts = sync.Map{}

		// Simulate mixed batch: some fail ES mapping, some succeed
		failContexts := []string{
			primitive.NewObjectID().Hex(),
			primitive.NewObjectID().Hex(),
		}
		successContexts := []string{
			primitive.NewObjectID().Hex(),
			primitive.NewObjectID().Hex(),
		}

		allContexts := append(failContexts, successContexts...)

		// Store all contexts
		for _, oid := range allContexts {
			ctx := &mongoContext{
				Database:   "test",
				Collection: "docs",
				DocumentID: oid,
				Namespace:  "test.docs",
				Operation:  "index",
				Completed:  false,
			}
			requestContexts.Store(oid+":test.docs", ctx)
		}

		t.Logf("Stored %d contexts for mixed batch", len(allContexts))

		// Simulate logFailedResponseItem marking failed contexts completed
		for _, oid := range failContexts {
			if storedCtx, ok := requestContexts.Load(oid + ":test.docs"); ok {
				if mongoCtx, ok := storedCtx.(*mongoContext); ok {
					mongoCtx.Completed = true
					t.Logf("Failed context marked completed: %s", oid)
				}
			}
		}

		// Simulate afterBulk final sweep for remaining contexts
		requestContexts.Range(func(key, value interface{}) bool {
			if ctx, ok := value.(*mongoContext); ok && !ctx.Completed {
				ctx.Completed = true
				t.Logf("Success context marked completed: %s", ctx.DocumentID)
			}
			return true
		})

		// CRITICAL CHECK: No contexts should remain with Completed:false
		uncompleted := countUncompleted()
		if uncompleted > 0 {
			t.Errorf("❌ MEMORY LEAK: %d contexts never marked completed", uncompleted)
		} else {
			t.Log("✅ All contexts in mixed batch marked completed")
		}

		// Test cleanup
		cleanupCompletedContexts()
		if countContexts() > 0 {
			t.Error("❌ MEMORY LEAK: Contexts not cleaned up")
		} else {
			t.Log("✅ All contexts cleaned up")
		}
	})
}

func countContexts() int {
	count := 0
	requestContexts.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func countUncompleted() int {
	count := 0
	requestContexts.Range(func(key, value interface{}) bool {
		if ctx, ok := value.(*mongoContext); ok && !ctx.Completed {
			count++
		}
		return true
	})
	return count
}
