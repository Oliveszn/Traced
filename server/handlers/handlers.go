package handlers

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"

	"github.com/Oliveszn/Traced/server/models"
	"github.com/Oliveszn/Traced/server/store"
)

// uuidRE validates that a string is a well-formed UUID (RFC 4122).
var uuidRE = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Handler struct {
	store *store.Store
}

func New(s *store.Store) *Handler {
	return &Handler{store: s}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("POST /spans", h.IngestSpans)
	mux.HandleFunc("GET /traces", h.ListTraces)
	mux.HandleFunc("GET /traces/{trace_id}", h.GetTrace)
}

// Health handles GET /health
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, models.StatusResponse{Status: "ok"})
}

// IngestSpans handles POST /spans
// Accepts a batch of spans, discards those outside the rolling window, stores the rest
func (h *Handler) IngestSpans(w http.ResponseWriter, r *http.Request) {
	var req models.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if len(req.Spans) == 0 {
		writeError(w, http.StatusBadRequest, "spans array must not be empty")
		return
	}

	h.store.Add(req.Spans)

	writeJSON(w, http.StatusAccepted, models.StatusResponse{Status: "ok"})
}

// ListTraces handles GET /traces
// optional query params: limit, after, before (nanoseconds)
func (h *Handler) ListTraces(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit, err := parseInt64Param(q.Get("limit"), 20)
	if err != nil || limit < 1 || limit > 1000 {
		writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 1000")
		return
	}

	after, err := parseInt64Param(q.Get("after"), 0)
	if err != nil || after < 0 {
		writeError(w, http.StatusBadRequest, "after must be a non-negative integer (nanoseconds)")
		return
	}

	before, err := parseInt64Param(q.Get("before"), 0)
	if err != nil || before < 0 {
		writeError(w, http.StatusBadRequest, "before must be a non-negative integer (nanoseconds)")
		return
	}

	if after > 0 && before > 0 && after >= before {
		writeError(w, http.StatusBadRequest, "after must be less than before")
		return
	}

	traces, total := h.store.ListTraces(limit, after, before)

	// Return an empty array, never null.
	if traces == nil {
		traces = []models.TraceSummary{}
	}

	writeJSON(w, http.StatusOK, models.TraceListResponse{
		Total:  total,
		Traces: traces,
	})
}

// GetTrace handles GET /traces/{trace_id}.
func (h *Handler) GetTrace(w http.ResponseWriter, r *http.Request) {
	traceID := r.PathValue("trace_id")

	if !uuidRE.MatchString(traceID) {
		writeError(w, http.StatusBadRequest, "trace_id must be a valid UUID")
		return
	}

	spans, found := h.store.GetTrace(traceID)
	if !found {
		writeError(w, http.StatusNotFound, "trace not found or outside the rolling window")
		return
	}

	writeJSON(w, http.StatusOK, models.TraceDetailResponse{
		TraceID: traceID,
		Spans:   spans,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, models.ErrorResponse{Error: msg})
}

// parseInt64Param parses a query string value as int64, returns default if empty
func parseInt64Param(s string, defaultVal int64) (int64, error) {
	if s == "" {
		return defaultVal, nil
	}
	return strconv.ParseInt(s, 10, 64)
}
