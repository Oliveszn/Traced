package store_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Oliveszn/Traced/server/models"
	"github.com/Oliveszn/Traced/server/store"
)

// helpers

func nowNano() int64 {
	return time.Now().UnixNano()
}

func span(traceID, spanID, parentID string, status string, startNano int64) models.Span {
	return models.Span{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentID,
		Service:      "test-service",
		Operation:    "test-op",
		Status:       status,
		StartTime:    startNano,
		EndTime:      startNano + int64(10*time.Millisecond),
	}
}

// Out-of-order assembly

// TestOutOfOrderAssembly verifies that children arriving before their root
// are stored correctly and the trace becomes visible when the root arrives.
func TestOutOfOrderAssembly(t *testing.T) {
	s := store.New(30 * time.Minute)
	now := nowNano()

	child1 := span("trace-1", "span-child-1", "span-root", "ok", now+1000)
	child2 := span("trace-1", "span-child-2", "span-root", "ok", now+2000)
	root := span("trace-1", "span-root", "", "ok", now)

	// Children arrive first — trace should NOT be visible yet
	s.Add([]models.Span{child1, child2})

	traces, total := s.ListTraces(100, 0, 0)
	if total != 0 {
		t.Fatalf("expected 0 visible traces before root arrives, got %d", total)
	}
	if len(traces) != 0 {
		t.Fatalf("expected 0 traces in list, got %d", len(traces))
	}

	// Root arrives — trace should now be visible with span_count=3
	s.Add([]models.Span{root})

	traces, total = s.ListTraces(100, 0, 0)
	if total != 1 {
		t.Fatalf("expected 1 visible trace after root arrives, got %d", total)
	}
	if traces[0].SpanCount != 3 {
		t.Fatalf("expected span_count=3, got %d", traces[0].SpanCount)
	}

	// GetTrace should return all 3 spans ordered by start_time ascending
	spans, found := s.GetTrace("trace-1")
	if !found {
		t.Fatal("expected trace to be found")
	}
	if len(spans) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(spans))
	}
	// First span should be the root (earliest start_time)
	if spans[0].SpanID != "span-root" {
		t.Fatalf("expected root span first, got %s", spans[0].SpanID)
	}
}

//  Rolling window eviction

// TestEvictionRemovesStaleData verifies that Evict() removes spans older than
// the window and leaves recent spans untouched
func TestEvictionRemovesStaleData(t *testing.T) {
	// Use a very short window (1 second) so we can trigger eviction quickly
	s := store.New(1 * time.Second)

	now := nowNano()
	old := now - int64(2*time.Second) // 2 seconds ago → outside 1s window

	// Add one recent trace and one stale trace.
	recentRoot := span("trace-recent", "span-r1", "", "ok", now)
	staleRoot := span("trace-stale", "span-s1", "", "ok", old)

	s.Add([]models.Span{recentRoot, staleRoot})

	// Both should be visible before eviction (stale span passed ingest
	// since we bypassed the cutoff check by inserting directly... wait,
	// actually Add() will also discard stale spans. So let's verify
	// that ingest itself discards the stale one)
	traces, total := s.ListTraces(100, 0, 0)
	if total != 1 {
		t.Fatalf("expected only recent trace visible (stale discarded on ingest), got %d", total)
	}
	if traces[0].TraceID != "trace-recent" {
		t.Fatalf("expected trace-recent, got %s", traces[0].TraceID)
	}

	// Now test background eviction: sleep past the window, then evict
	time.Sleep(1100 * time.Millisecond)
	s.Evict()

	traces, total = s.ListTraces(100, 0, 0)
	if total != 0 {
		t.Fatalf("expected 0 traces after eviction, got %d", total)
	}
}

// TestEvictionKeepsSpansInWindow verifies that eviction does not remove
// spans that are still within the window
func TestEvictionKeepsSpansInWindow(t *testing.T) {
	s := store.New(30 * time.Minute)
	now := nowNano()

	root := span("trace-keep", "span-1", "", "ok", now)
	s.Add([]models.Span{root})

	s.Evict()

	_, found := s.GetTrace("trace-keep")
	if !found {
		t.Fatal("expected trace to still be present after eviction within window")
	}
}

// Concurrent writes

// TestConcurrentWrites verifies that simultaneous goroutines writing to the
// store don't corrupt state (no panics, correct span counts)
func TestConcurrentWrites(t *testing.T) {
	s := store.New(30 * time.Minute)
	now := nowNano()

	const goroutines = 50
	const spansPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			traceID := fmt.Sprintf("trace-%d", id)
			spans := make([]models.Span, spansPerGoroutine)
			// First span is root, rest are children
			spans[0] = span(traceID, fmt.Sprintf("span-%d-root", id), "", "ok", now)
			for j := 1; j < spansPerGoroutine; j++ {
				spans[j] = span(
					traceID,
					fmt.Sprintf("span-%d-%d", id, j),
					fmt.Sprintf("span-%d-root", id),
					"ok",
					now+int64(j),
				)
			}
			s.Add(spans)
		}(i)
	}

	wg.Wait()

	// Every goroutine wrote one trace with spansPerGoroutine spans
	_, total := s.ListTraces(1000, 0, 0)
	if total != goroutines {
		t.Fatalf("expected %d traces, got %d", goroutines, total)
	}

	for i := 0; i < goroutines; i++ {
		traceID := fmt.Sprintf("trace-%d", i)
		spans, found := s.GetTrace(traceID)
		if !found {
			t.Errorf("trace %s not found", traceID)
			continue
		}
		if len(spans) != spansPerGoroutine {
			t.Errorf("trace %s: expected %d spans, got %d", traceID, spansPerGoroutine, len(spans))
		}
	}
}

// Status propagation

// TestErrorStatusPropagation verifies that a trace is "error" if any child
// span is an error, even when the root span is "ok"
func TestErrorStatusPropagation(t *testing.T) {
	s := store.New(30 * time.Minute)
	now := nowNano()

	root := span("trace-err", "span-root", "", "ok", now)
	child := span("trace-err", "span-child", "span-root", "error", now+1000)

	s.Add([]models.Span{root, child})

	traces, _ := s.ListTraces(100, 0, 0)
	if len(traces) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(traces))
	}
	if traces[0].Status != "error" {
		t.Fatalf("expected status=error when child has error, got %s", traces[0].Status)
	}
}

// TestOkStatusWhenNoErrors verifies that a trace with all-ok spans is "ok"
func TestOkStatusWhenNoErrors(t *testing.T) {
	s := store.New(30 * time.Minute)
	now := nowNano()

	root := span("trace-ok", "span-root", "", "ok", now)
	child := span("trace-ok", "span-child", "span-root", "ok", now+1000)

	s.Add([]models.Span{root, child})

	traces, _ := s.ListTraces(100, 0, 0)
	if traces[0].Status != "ok" {
		t.Fatalf("expected status=ok, got %s", traces[0].Status)
	}
}
