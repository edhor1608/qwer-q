package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jonas/qwer-q/internal/broker"
	"github.com/jonas/qwer-q/internal/schema"
	"golang.org/x/net/websocket"
)

// Handler provides REST API endpoints for broker admin operations.
type Handler struct {
	broker   *broker.Broker
	registry *schema.Registry
	hub      *wsHub
	done     chan struct{}
}

// New creates a new API handler.
func New(b *broker.Broker, r *schema.Registry) *Handler {
	h := &Handler{broker: b, registry: r, hub: newWSHub(), done: make(chan struct{})}
	h.startWSBroadcast()
	return h
}

// Close stops the WebSocket broadcast goroutine.
func (h *Handler) Close() {
	close(h.done)
}

// Register adds all API routes to the given mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/queues", h.cors(h.handleQueues))
	mux.HandleFunc("/api/v1/queues/", h.cors(h.handleQueueByName))
	mux.HandleFunc("/api/v1/schemas", h.cors(h.handleSchemas))
	mux.HandleFunc("/api/v1/schemas/", h.cors(h.handleSchemaByName))
	mux.HandleFunc("/api/v1/stats", h.cors(h.handleStats))
	mux.HandleFunc("/api/v1/consumers", h.cors(h.handleConsumers))
	mux.Handle("/api/v1/ws", websocket.Handler(h.handleWS))
}

// cors wraps a handler with CORS headers.
func (h *Handler) cors(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// --- Queue endpoints ---

type queueSummary struct {
	Name          string `json:"name"`
	Depth         int    `json:"depth"`
	InFlight      int    `json:"in_flight"`
	ConsumerCount int    `json:"consumer_count"`
}

type queueDetail struct {
	Name          string               `json:"name"`
	Depth         int                  `json:"depth"`
	InFlight      int                  `json:"in_flight"`
	ConsumerCount int                  `json:"consumer_count"`
	MaxSize       int                  `json:"max_size"`
	MaxRetries    uint32               `json:"max_retries"`
	FailurePolicy broker.FailurePolicy `json:"failure_policy"`
	HasSchema     bool                 `json:"has_schema"`
}

func (h *Handler) handleQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	names := h.broker.ListQueues()
	queues := make([]queueSummary, 0, len(names))
	for _, name := range names {
		q := h.broker.GetQueue(name)
		if q == nil {
			continue
		}
		queues = append(queues, queueSummary{
			Name:          name,
			Depth:         q.Len(),
			InFlight:      q.InFlightLen(),
			ConsumerCount: q.ConsumerCount(),
		})
	}
	writeJSON(w, http.StatusOK, queues)
}

// handleQueueByName routes /api/v1/queues/{name}[/...] requests.
func (h *Handler) handleQueueByName(w http.ResponseWriter, r *http.Request) {
	// Parse: /api/v1/queues/{name}[/dlq[/retry]][/messages]
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/queues/")
	if path == "" {
		writeError(w, http.StatusBadRequest, "queue name required")
		return
	}

	// Split into parts: name, sub-resource
	parts := strings.SplitN(path, "/", 2)
	name := parts[0]
	sub := ""
	if len(parts) > 1 {
		sub = parts[1]
	}

	switch {
	case sub == "" && r.Method == http.MethodGet:
		h.handleQueueInfo(w, r, name)
	case sub == "" && r.Method == http.MethodDelete:
		h.handleQueuePurge(w, r, name)
	case sub == "dlq" && r.Method == http.MethodGet:
		h.handleDLQList(w, r, name)
	case sub == "dlq/retry" && r.Method == http.MethodPost:
		h.handleDLQRetry(w, r, name)
	case sub == "dlq" && r.Method == http.MethodDelete:
		h.handleDLQPurge(w, r, name)
	case sub == "messages" && r.Method == http.MethodGet:
		h.handleMessagesPeek(w, r, name)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *Handler) handleQueueInfo(w http.ResponseWriter, _ *http.Request, name string) {
	q := h.broker.GetQueue(name)
	if q == nil {
		writeError(w, http.StatusNotFound, "queue not found")
		return
	}
	_, hasSchema := h.registry.Get(name)
	detail := queueDetail{
		Name:          name,
		Depth:         q.Len(),
		InFlight:      q.InFlightLen(),
		ConsumerCount: q.ConsumerCount(),
		MaxSize:       q.MaxSize(),
		MaxRetries:    q.MaxRetries(),
		FailurePolicy: q.FailurePolicy(),
		HasSchema:     hasSchema == nil,
	}
	writeJSON(w, http.StatusOK, detail)
}

func (h *Handler) handleQueuePurge(w http.ResponseWriter, _ *http.Request, name string) {
	q := h.broker.GetQueue(name)
	if q == nil {
		writeError(w, http.StatusNotFound, "queue not found")
		return
	}
	count := q.Purge()
	writeJSON(w, http.StatusOK, map[string]int{"purged": count})
}

// --- DLQ endpoints ---

type dlqMessage struct {
	ID          string            `json:"id"`
	Queue       string            `json:"queue"`
	Payload     string            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attempt     uint32            `json:"attempt"`
	PublishedAt time.Time         `json:"published_at"`
}

func (h *Handler) handleDLQList(w http.ResponseWriter, r *http.Request, name string) {
	dlqName := broker.DLQName(name)
	q := h.broker.GetQueue(dlqName)
	if q == nil || q.Len() == 0 {
		writeJSON(w, http.StatusOK, []dlqMessage{})
		return
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	msgs := q.Peek(limit)
	result := make([]dlqMessage, len(msgs))
	for i, m := range msgs {
		result[i] = dlqMessage{
			ID:          m.ID,
			Queue:       m.Queue,
			Payload:     base64.StdEncoding.EncodeToString(m.Payload),
			Headers:     m.Headers,
			Attempt:     m.Attempt,
			PublishedAt: m.PublishedAt,
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) handleDLQRetry(w http.ResponseWriter, _ *http.Request, name string) {
	dlqName := broker.DLQName(name)
	dlq := h.broker.GetQueue(dlqName)
	if dlq == nil || dlq.Len() == 0 {
		writeJSON(w, http.StatusOK, map[string]int{"retried": 0})
		return
	}

	// Move all DLQ messages back to the original queue (copy to avoid mutating originals)
	msgs := dlq.Peek(dlq.Len())
	q := h.broker.GetOrCreateQueue(name)
	retried := 0
	for _, msg := range msgs {
		copy := *msg
		copy.Queue = name
		copy.Attempt = 0
		copy.VisibleAt = time.Now()
		if err := q.Enqueue(&copy); err == nil {
			retried++
		}
	}
	// Only purge DLQ if all messages were successfully moved
	if retried == len(msgs) {
		dlq.Purge()
	}
	writeJSON(w, http.StatusOK, map[string]int{"retried": retried})
}

func (h *Handler) handleDLQPurge(w http.ResponseWriter, _ *http.Request, name string) {
	dlqName := broker.DLQName(name)
	dlq := h.broker.GetQueue(dlqName)
	if dlq == nil {
		writeJSON(w, http.StatusOK, map[string]int{"purged": 0})
		return
	}
	count := dlq.Purge()
	writeJSON(w, http.StatusOK, map[string]int{"purged": count})
}

// --- Messages peek ---

type peekMessage struct {
	ID          string            `json:"id"`
	Queue       string            `json:"queue"`
	Payload     string            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attempt     uint32            `json:"attempt"`
	PublishedAt time.Time         `json:"published_at"`
}

func (h *Handler) handleMessagesPeek(w http.ResponseWriter, r *http.Request, name string) {
	q := h.broker.GetQueue(name)
	if q == nil {
		writeError(w, http.StatusNotFound, "queue not found")
		return
	}
	limit := 10
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	msgs := q.Peek(limit)
	result := make([]peekMessage, len(msgs))
	for i, m := range msgs {
		result[i] = peekMessage{
			ID:          m.ID,
			Queue:       m.Queue,
			Payload:     base64.StdEncoding.EncodeToString(m.Payload),
			Headers:     m.Headers,
			Attempt:     m.Attempt,
			PublishedAt: m.PublishedAt,
		}
	}
	writeJSON(w, http.StatusOK, result)
}

// --- Schema endpoints ---

type schemaSummary struct {
	Queue       string `json:"queue"`
	MessageType string `json:"message_type"`
	Version     uint32 `json:"version"`
}

type schemaDetail struct {
	Queue       string    `json:"queue"`
	MessageType string    `json:"message_type"`
	Version     uint32    `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
}

func (h *Handler) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	names := h.registry.List()
	schemas := make([]schemaSummary, 0, len(names))
	for _, name := range names {
		s, err := h.registry.Get(name)
		if err != nil {
			continue
		}
		schemas = append(schemas, schemaSummary{
			Queue:       s.Queue,
			MessageType: s.MessageType,
			Version:     s.Version,
		})
	}
	writeJSON(w, http.StatusOK, schemas)
}

func (h *Handler) handleSchemaByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/schemas/")
	if name == "" {
		writeError(w, http.StatusBadRequest, "schema name required")
		return
	}
	s, err := h.registry.Get(name)
	if err != nil {
		writeError(w, http.StatusNotFound, "schema not found")
		return
	}
	writeJSON(w, http.StatusOK, schemaDetail{
		Queue:       s.Queue,
		MessageType: s.MessageType,
		Version:     s.Version,
		CreatedAt:   s.CreatedAt,
	})
}

// --- Stats endpoint ---

type statsResponse struct {
	Uptime       string `json:"uptime"`
	StartedAt    string `json:"started_at"`
	QueueCount   int    `json:"queue_count"`
	MemoryAlloc  uint64 `json:"memory_alloc_bytes"`
	MemoryLimit  uint64 `json:"memory_limit_bytes"`
	NumGoroutine int    `json:"num_goroutine"`
}

func (h *Handler) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, statsResponse{
		Uptime:       time.Since(h.broker.StartedAt()).Round(time.Second).String(),
		StartedAt:    h.broker.StartedAt().UTC().Format(time.RFC3339),
		QueueCount:   len(h.broker.ListQueues()),
		MemoryAlloc:  h.broker.MemoryAlloc(),
		MemoryLimit:  h.broker.MemoryLimit(),
		NumGoroutine: runtime.NumGoroutine(),
	})
}

// --- Consumers endpoint ---

type consumerInfo struct {
	Queue         string `json:"queue"`
	ConsumerCount int    `json:"consumer_count"`
}

func (h *Handler) handleConsumers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	names := h.broker.ListQueues()
	consumers := make([]consumerInfo, 0)
	for _, name := range names {
		q := h.broker.GetQueue(name)
		if q == nil {
			continue
		}
		count := q.ConsumerCount()
		if count > 0 {
			consumers = append(consumers, consumerInfo{
				Queue:         name,
				ConsumerCount: count,
			})
		}
	}
	writeJSON(w, http.StatusOK, consumers)
}
