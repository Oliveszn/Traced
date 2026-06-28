package models

// Span is a single unit of work within a trace.
// Root spans have no ParentSpanID.
// Timestamps are Unix nanoseconds.
type Span struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	Service      string            `json:"service"`
	Operation    string            `json:"operation"`
	StartTime    int64             `json:"start_time"`
	EndTime      int64             `json:"end_time"`
	Status       string            `json:"status"`
	Tags         map[string]string `json:"tags,omitempty"`
}

// IsRoot returns true if this span has no parent — i.e. it is the root of a trace.
func (s Span) IsRoot() bool {
	return s.ParentSpanID == ""
}

// TraceSummary is returned by GET /traces.
// Status is "error" if ANY span in the trace is an error, not just the root.
type TraceSummary struct {
	TraceID       string `json:"trace_id"`
	RootService   string `json:"root_service"`
	RootOperation string `json:"root_operation"`
	SpanCount     int    `json:"span_count"`
	DurationMs    int64  `json:"duration_ms"`
	StartTime     int64  `json:"start_time"`
	Status        string `json:"status"`
}

// IngestRequest is the body of POST /spans.
type IngestRequest struct {
	Spans []Span `json:"spans"`
}

// TraceListResponse is the body of GET /traces.
// Total is the count of matching traces BEFORE limit is applied.
type TraceListResponse struct {
	Total  int            `json:"total"`
	Traces []TraceSummary `json:"traces"`
}

// TraceDetailResponse is the body of GET /traces/{trace_id}.
type TraceDetailResponse struct {
	TraceID string `json:"trace_id"`
	Spans   []Span `json:"spans"`
}

// StatusResponse is returned by endpoints with no resource to return.
type StatusResponse struct {
	Status string `json:"status"`
}

// ErrorResponse is returned on 4xx responses.
type ErrorResponse struct {
	Error string `json:"error"`
}
