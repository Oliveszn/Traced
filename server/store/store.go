package store

import (
	"sort"
	"sync"
	"time"

	"github.com/Oliveszn/Traced/server/models"
)

// Store holds all span data in memory, grouped by trace ID
// Safe for concurrent use cos all reads use RLock, all writes use Lock
type Store struct {
	mu     sync.RWMutex
	traces map[string][]models.Span
	window time.Duration
}

// New creates a store with the given rolling window
func New(window time.Duration) *Store {
	return &Store{
		traces: make(map[string][]models.Span),
		window: window,
	}
}

// Add ingests a batch of spans. Spansoutside the rolling window are silently discarded
func (s *Store) Add(spans []models.Span) {
	cutoff := time.Now().Add(-s.window).UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, span := range spans {
		if span.StartTime < cutoff {
			continue //Discard spann older than rolling window
		}
		s.traces[span.TraceID] = append(s.traces[span.TraceID], span)
	}
}

// Evict removes all traces where every span is older than the rolling window
// called by the bg eviction goroutine
func (s *Store) Evict() {
	cutoff := time.Now().Add(-s.window).UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	for traceID, spans := range s.traces {
		//keep only spans still within the window
		kept := spans[:0]
		for _, span := range spans {
			if span.StartTime >= cutoff {
				kept = append(kept, span)
			}
		}
		if len(kept) == 0 {
			delete(s.traces, traceID)
		} else {
			s.traces[traceID] = kept
		}
	}
}

// ListTraces returns Tracesummary objects for all traces that have a root span
// filteres by the optional after/before nanosecond timestamp
func (s *Store) ListTraces(limit, after, before int64) (summaries []models.TraceSummary, total int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	//Build summaries for every trace that has a root span
	var all []models.TraceSummary
	for _, spans := range s.traces {
		summary, ok := buildSummary(spans)
		if !ok {
			//no root span yet, trace is not visible
			continue
		}

		//apply time filters on root span start_time
		if after > 0 && summary.StartTime < after {
			continue
		}
		if before > 0 && summary.StartTime >= before {
			continue
		}
		all = append(all, summary)
	}

	//sort descending by start_time
	sort.Slice(all, func(i, j int) bool {
		return all[i].StartTime > all[j].StartTime
	})

	total = len(all)

	if limit <= 0 {
		limit = 20
	}
	if int(limit) < len(all) {
		all = all[:limit]
	}
	return all, total
}

// GetTrace returns all spans for a given trace ID, ordered by start_time
func (s *Store) GetTrace(traceID string) ([]models.Span, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	spans, exists := s.traces[traceID]
	if !exists || len(spans) == 0 {
		return nil, false
	}

	//only return the trace if it has a root span
	hasRoot := false
	for _, sp := range spans {
		if sp.IsRoot() {
			hasRoot = true
			break
		}
	}
	if !hasRoot {
		return nil, false
	}

	//Return a copy sorted by ascending(start_time)
	result := make([]models.Span, len(spans))
	copy(result, spans)
	sort.Slice(result, func(i, j int) bool {
		return result[i].StartTime < result[j].StartTime
	})
	return result, true
}

// TraceCount returns the total number of visible traces in the store
func (s *Store) TraceCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, spans := range s.traces {
		for _, sp := range spans {
			if sp.IsRoot() {
				count++
				break
			}
		}
	}
	return count
}

//HELPERS

// buildsummary gets a tracesummary from a slice of spans, return false if no root span is present
func buildSummary(spans []models.Span) (models.TraceSummary, bool) {
	var root *models.Span
	hasError := false

	for i := range spans {
		if spans[i].Status == "error" {
			hasError = true
		}
		if spans[i].IsRoot() {
			root = &spans[i]
		}
	}

	if root == nil {
		return models.TraceSummary{}, false
	}

	status := "ok"
	if hasError {
		status = "error"
	}

	return models.TraceSummary{
		TraceID:       root.TraceID,
		RootService:   root.Service,
		RootOperation: root.Operation,
		SpanCount:     len(spans),
		DurationMs:    (root.EndTime - root.StartTime) / 1_000_000,
		StartTime:     root.StartTime,
		Status:        status,
	}, true
}
