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
