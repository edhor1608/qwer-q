package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jonas/qwer-q/internal/dashboard"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsServer serves Prometheus metrics and health endpoints.
type MetricsServer struct {
	server *http.Server
	mux    *http.ServeMux
}

// HealthResponse is the JSON response for /health.
type HealthResponse struct {
	Status string `json:"status"`
	Time   string `json:"time"`
}

// NewMetricsServer creates a metrics server on the given address.
func NewMetricsServer(addr string) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", handleHealth)
	mux.Handle("/dashboard/", http.StripPrefix("/dashboard", dashboard.Handler()))

	return &MetricsServer{
		mux: mux,
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

// Mux returns the HTTP mux so additional routes can be registered.
func (m *MetricsServer) Mux() *http.ServeMux {
	return m.mux
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := HealthResponse{
		Status: "ok",
		Time:   time.Now().UTC().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ListenAndServe starts the metrics server.
func (m *MetricsServer) ListenAndServe() error {
	return m.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (m *MetricsServer) Shutdown(ctx context.Context) error {
	return m.server.Shutdown(ctx)
}

// Addr returns the configured address.
func (m *MetricsServer) Addr() string {
	return m.server.Addr
}
