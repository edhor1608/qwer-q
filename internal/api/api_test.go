package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/broker"
	"github.com/jonas/qwer-q/internal/schema"
	"github.com/jonas/qwer-q/internal/types"
)

func newTestHandler() (*Handler, *broker.Broker) {
	b := broker.NewBroker(broker.WithMemoryLimit(0))
	r := schema.NewRegistry()
	return New(b, r), b
}

func TestListQueues_Empty(t *testing.T) {
	h, _ := newTestHandler()
	req := httptest.NewRequest("GET", "/api/v1/queues", nil)
	w := httptest.NewRecorder()
	h.handleQueues(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []queueSummary
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 0 {
		t.Fatalf("expected 0 queues, got %d", len(result))
	}
}

func TestListQueues_WithQueues(t *testing.T) {
	h, b := newTestHandler()
	q := b.GetOrCreateQueue("test-queue")
	q.Enqueue(&types.Message{ID: "m1", Queue: "test-queue", PublishedAt: time.Now(), VisibleAt: time.Now()})

	req := httptest.NewRequest("GET", "/api/v1/queues", nil)
	w := httptest.NewRecorder()
	h.handleQueues(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []queueSummary
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(result))
	}
	if result[0].Name != "test-queue" {
		t.Errorf("expected name 'test-queue', got %q", result[0].Name)
	}
	if result[0].Depth != 1 {
		t.Errorf("expected depth 1, got %d", result[0].Depth)
	}
}

func TestQueueInfo(t *testing.T) {
	h, b := newTestHandler()
	b.GetOrCreateQueue("my-queue")

	req := httptest.NewRequest("GET", "/api/v1/queues/my-queue", nil)
	w := httptest.NewRecorder()
	h.handleQueueByName(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result queueDetail
	json.NewDecoder(w.Body).Decode(&result)
	if result.Name != "my-queue" {
		t.Errorf("expected name 'my-queue', got %q", result.Name)
	}
	if result.MaxSize != broker.DefaultMaxQueueSize {
		t.Errorf("expected max_size %d, got %d", broker.DefaultMaxQueueSize, result.MaxSize)
	}
}

func TestQueueInfo_NotFound(t *testing.T) {
	h, _ := newTestHandler()
	req := httptest.NewRequest("GET", "/api/v1/queues/nonexistent", nil)
	w := httptest.NewRecorder()
	h.handleQueueByName(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestQueuePurge(t *testing.T) {
	h, b := newTestHandler()
	q := b.GetOrCreateQueue("purge-me")
	q.Enqueue(&types.Message{ID: "m1", Queue: "purge-me", PublishedAt: time.Now(), VisibleAt: time.Now()})
	q.Enqueue(&types.Message{ID: "m2", Queue: "purge-me", PublishedAt: time.Now(), VisibleAt: time.Now()})

	req := httptest.NewRequest("DELETE", "/api/v1/queues/purge-me", nil)
	w := httptest.NewRecorder()
	h.handleQueueByName(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]int
	json.NewDecoder(w.Body).Decode(&result)
	if result["purged"] != 2 {
		t.Errorf("expected purged 2, got %d", result["purged"])
	}
	if q.Len() != 0 {
		t.Errorf("expected queue len 0, got %d", q.Len())
	}
}

func TestMessagesPeek(t *testing.T) {
	h, b := newTestHandler()
	q := b.GetOrCreateQueue("peek-queue")
	q.Enqueue(&types.Message{ID: "m1", Queue: "peek-queue", Payload: []byte("hello"), PublishedAt: time.Now(), VisibleAt: time.Now()})
	q.Enqueue(&types.Message{ID: "m2", Queue: "peek-queue", Payload: []byte("world"), PublishedAt: time.Now(), VisibleAt: time.Now()})

	req := httptest.NewRequest("GET", "/api/v1/queues/peek-queue/messages?limit=1", nil)
	w := httptest.NewRecorder()
	h.handleQueueByName(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []peekMessage
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result))
	}
	if result[0].ID != "m1" {
		t.Errorf("expected message id 'm1', got %q", result[0].ID)
	}
	// Verify messages are still in queue (non-destructive)
	if q.Len() != 2 {
		t.Errorf("expected queue len 2 after peek, got %d", q.Len())
	}
}

func TestDLQList_Empty(t *testing.T) {
	h, _ := newTestHandler()
	req := httptest.NewRequest("GET", "/api/v1/queues/my-queue/dlq", nil)
	w := httptest.NewRecorder()
	h.handleQueueByName(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []dlqMessage
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 0 {
		t.Fatalf("expected 0 dlq messages, got %d", len(result))
	}
}

func TestDLQRetryAndPurge(t *testing.T) {
	h, b := newTestHandler()
	// Create original queue
	b.GetOrCreateQueue("retry-queue")
	// Create DLQ with messages
	dlq := b.GetOrCreateQueue("retry-queue.dlq")
	dlq.Enqueue(&types.Message{ID: "d1", Queue: "retry-queue.dlq", Payload: []byte("failed"), PublishedAt: time.Now(), VisibleAt: time.Now()})

	// Retry
	req := httptest.NewRequest("POST", "/api/v1/queues/retry-queue/dlq/retry", nil)
	w := httptest.NewRecorder()
	h.handleQueueByName(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]int
	json.NewDecoder(w.Body).Decode(&result)
	if result["retried"] != 1 {
		t.Errorf("expected retried 1, got %d", result["retried"])
	}

	// DLQ should be empty
	if dlq.Len() != 0 {
		t.Errorf("expected DLQ len 0 after retry, got %d", dlq.Len())
	}

	// Original queue should have the message
	q := b.GetQueue("retry-queue")
	if q.Len() != 1 {
		t.Errorf("expected queue len 1 after retry, got %d", q.Len())
	}
}

func TestDLQPurge(t *testing.T) {
	h, b := newTestHandler()
	dlq := b.GetOrCreateQueue("purge-queue.dlq")
	dlq.Enqueue(&types.Message{ID: "d1", Queue: "purge-queue.dlq", PublishedAt: time.Now(), VisibleAt: time.Now()})

	req := httptest.NewRequest("DELETE", "/api/v1/queues/purge-queue/dlq", nil)
	w := httptest.NewRecorder()
	h.handleQueueByName(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]int
	json.NewDecoder(w.Body).Decode(&result)
	if result["purged"] != 1 {
		t.Errorf("expected purged 1, got %d", result["purged"])
	}
}

func TestStats(t *testing.T) {
	h, _ := newTestHandler()
	req := httptest.NewRequest("GET", "/api/v1/stats", nil)
	w := httptest.NewRecorder()
	h.handleStats(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result statsResponse
	json.NewDecoder(w.Body).Decode(&result)
	if result.StartedAt == "" {
		t.Error("expected started_at to be set")
	}
	if result.Uptime == "" {
		t.Error("expected uptime to be set")
	}
	if result.NumGoroutine == 0 {
		t.Error("expected num_goroutine > 0")
	}
}

func TestConsumers_Empty(t *testing.T) {
	h, _ := newTestHandler()
	req := httptest.NewRequest("GET", "/api/v1/consumers", nil)
	w := httptest.NewRecorder()
	h.handleConsumers(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []consumerInfo
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 0 {
		t.Fatalf("expected 0 consumers, got %d", len(result))
	}
}

func TestSchemas_Empty(t *testing.T) {
	h, _ := newTestHandler()
	req := httptest.NewRequest("GET", "/api/v1/schemas", nil)
	w := httptest.NewRecorder()
	h.handleSchemas(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []schemaSummary
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 0 {
		t.Fatalf("expected 0 schemas, got %d", len(result))
	}
}

func TestSchemaByName_NotFound(t *testing.T) {
	h, _ := newTestHandler()
	req := httptest.NewRequest("GET", "/api/v1/schemas/nonexistent", nil)
	w := httptest.NewRecorder()
	h.handleSchemaByName(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h, _ := newTestHandler()

	tests := []struct {
		method string
		path   string
		handle http.HandlerFunc
	}{
		{"POST", "/api/v1/queues", h.handleQueues},
		{"POST", "/api/v1/schemas", h.handleSchemas},
		{"POST", "/api/v1/stats", h.handleStats},
		{"POST", "/api/v1/consumers", h.handleConsumers},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, tt.path, nil)
		w := httptest.NewRecorder()
		tt.handle(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: expected 405, got %d", tt.method, tt.path, w.Code)
		}
	}
}

func TestCORS(t *testing.T) {
	h, _ := newTestHandler()
	handler := h.cors(h.handleQueues)

	// OPTIONS preflight
	req := httptest.NewRequest("OPTIONS", "/api/v1/queues", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for OPTIONS, got %d", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("expected CORS Allow-Origin header")
	}

	// Regular GET
	req = httptest.NewRequest("GET", "/api/v1/queues", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("expected CORS Allow-Origin header on GET")
	}
}
