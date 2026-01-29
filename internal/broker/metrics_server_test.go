package broker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthEndpoint(t *testing.T) {
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "ok" {
		t.Errorf("expected status 'ok', got %s", resp.Status)
	}

	if resp.Time == "" {
		t.Error("expected time to be set")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	ms := NewMetricsServer(":0")

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	ms.server.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "go_") {
		t.Error("expected Go runtime metrics in /metrics output")
	}
}

func TestNewMetricsServer(t *testing.T) {
	ms := NewMetricsServer(":9877")
	if ms.Addr() != ":9877" {
		t.Errorf("expected addr :9877, got %s", ms.Addr())
	}
}
