package broker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	messagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_messages_published_total",
		Help: "Total number of messages published",
	}, []string{"queue"})

	messagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_messages_consumed_total",
		Help: "Total number of messages consumed (delivered to consumers)",
	}, []string{"queue"})

	messagesAcked = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_messages_acked_total",
		Help: "Total number of messages acknowledged",
	}, []string{"queue"})

	messagesNacked = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_messages_nacked_total",
		Help: "Total number of messages negatively acknowledged",
	}, []string{"queue"})

	queueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "qwerq_queue_depth",
		Help: "Current number of messages in queue (not in-flight)",
	}, []string{"queue"})

	inFlightCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "qwerq_in_flight_count",
		Help: "Current number of in-flight messages",
	}, []string{"queue"})

	publishLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "qwerq_publish_latency_seconds",
		Help:    "Latency of publish operations",
		Buckets: prometheus.DefBuckets,
	}, []string{"queue"})

	messagesDLQ = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_messages_dlq_total",
		Help: "Total number of messages sent to dead letter queue",
	}, []string{"queue"})

	duplicateMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_duplicate_messages_total",
		Help: "Total number of duplicate messages rejected",
	}, []string{"queue"})

	queueFullErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_queue_full_errors_total",
		Help: "Total number of queue full errors",
	}, []string{"queue"})

	callRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_call_requests_total",
		Help: "Total number of CALL requests",
	}, []string{"queue"})

	callTimeouts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qwerq_call_timeouts_total",
		Help: "Total number of CALL timeouts",
	}, []string{"queue"})
)

// RecordPublish records a message publish.
func RecordPublish(queue string, latencySeconds float64) {
	messagesPublished.WithLabelValues(queue).Inc()
	publishLatency.WithLabelValues(queue).Observe(latencySeconds)
}

// RecordConsume records a message consumption.
func RecordConsume(queue string) {
	messagesConsumed.WithLabelValues(queue).Inc()
}

// RecordAck records a message acknowledgment.
func RecordAck(queue string) {
	messagesAcked.WithLabelValues(queue).Inc()
}

// RecordNack records a message negative acknowledgment.
func RecordNack(queue string) {
	messagesNacked.WithLabelValues(queue).Inc()
}

// UpdateQueueDepth updates the queue depth gauge.
func UpdateQueueDepth(queue string, depth int) {
	queueDepth.WithLabelValues(queue).Set(float64(depth))
}

// UpdateInFlightCount updates the in-flight count gauge.
func UpdateInFlightCount(queue string, count int) {
	inFlightCount.WithLabelValues(queue).Set(float64(count))
}

// RecordDLQ records a message sent to dead letter queue.
func RecordDLQ(queue string) {
	messagesDLQ.WithLabelValues(queue).Inc()
}

// RecordDuplicateMessage records a duplicate message rejection.
func RecordDuplicateMessage(queue string) {
	duplicateMessages.WithLabelValues(queue).Inc()
}

// RecordQueueFull records a queue full error.
func RecordQueueFull(queue string) {
	queueFullErrors.WithLabelValues(queue).Inc()
}

// RecordCallRequest records a CALL request.
func RecordCallRequest(queue string) {
	callRequests.WithLabelValues(queue).Inc()
}

// RecordCallTimeout records a CALL timeout.
func RecordCallTimeout(queue string) {
	callTimeouts.WithLabelValues(queue).Inc()
}
