package broker

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordPublish(t *testing.T) {
	RecordPublish("test-queue", 0.001)

	count := testutil.ToFloat64(messagesPublished.WithLabelValues("test-queue"))
	if count < 1 {
		t.Errorf("expected publish count >= 1, got %f", count)
	}
}

func TestRecordConsume(t *testing.T) {
	RecordConsume("test-queue")

	count := testutil.ToFloat64(messagesConsumed.WithLabelValues("test-queue"))
	if count < 1 {
		t.Errorf("expected consume count >= 1, got %f", count)
	}
}

func TestRecordAck(t *testing.T) {
	RecordAck("test-queue")

	count := testutil.ToFloat64(messagesAcked.WithLabelValues("test-queue"))
	if count < 1 {
		t.Errorf("expected ack count >= 1, got %f", count)
	}
}

func TestRecordNack(t *testing.T) {
	RecordNack("test-queue")

	count := testutil.ToFloat64(messagesNacked.WithLabelValues("test-queue"))
	if count < 1 {
		t.Errorf("expected nack count >= 1, got %f", count)
	}
}

func TestUpdateQueueDepth(t *testing.T) {
	UpdateQueueDepth("depth-queue", 42)

	depth := testutil.ToFloat64(queueDepth.WithLabelValues("depth-queue"))
	if depth != 42 {
		t.Errorf("expected queue depth 42, got %f", depth)
	}
}

func TestUpdateInFlightCount(t *testing.T) {
	UpdateInFlightCount("flight-queue", 10)

	count := testutil.ToFloat64(inFlightCount.WithLabelValues("flight-queue"))
	if count != 10 {
		t.Errorf("expected in-flight count 10, got %f", count)
	}
}

func TestMetricsRegistration(t *testing.T) {
	// Verify all metrics are registered
	metrics := []string{
		"qwerq_messages_published_total",
		"qwerq_messages_consumed_total",
		"qwerq_messages_acked_total",
		"qwerq_messages_nacked_total",
		"qwerq_queue_depth",
		"qwerq_in_flight_count",
		"qwerq_publish_latency_seconds",
	}

	gathered, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	registeredNames := make(map[string]bool)
	for _, mf := range gathered {
		registeredNames[mf.GetName()] = true
	}

	// Initialize metrics by calling them once
	RecordPublish("reg-test", 0.001)
	RecordConsume("reg-test")
	RecordAck("reg-test")
	RecordNack("reg-test")
	UpdateQueueDepth("reg-test", 1)
	UpdateInFlightCount("reg-test", 1)

	gathered, _ = prometheus.DefaultGatherer.Gather()
	for _, mf := range gathered {
		registeredNames[mf.GetName()] = true
	}

	for _, name := range metrics {
		found := false
		for regName := range registeredNames {
			if strings.HasPrefix(regName, name) || regName == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("metric %s not registered", name)
		}
	}
}
