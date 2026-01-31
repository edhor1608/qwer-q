package scenarios

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/harness"
)

// RunLatency runs a latency benchmark against the given adapter.
func RunLatency(ctx context.Context, adapter adapters.Adapter, cfg harness.Config) harness.LatencyResult {
	result := harness.LatencyResult{Name: adapter.Name()}

	if err := adapter.Setup(ctx); err != nil {
		result.Error = err
		return result
	}
	defer adapter.Teardown()

	queue := "bench-latency"
	hist := harness.NewHistogram()

	var consumed int64
	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()

	// Start consumer
	go func() {
		adapter.Consume(consCtx, queue, func(data []byte) error {
			if len(data) >= 8 {
				sentNano := int64(binary.BigEndian.Uint64(data[:8]))
				latency := time.Duration(time.Now().UnixNano() - sentNano)
				hist.Record(latency)
			}
			atomic.AddInt64(&consumed, 1)
			return nil
		})
	}()

	// Let consumer start
	time.Sleep(100 * time.Millisecond)

	// Publish at controlled rate
	payload := make([]byte, cfg.MessageSize)
	interval := time.Second / time.Duration(cfg.TargetRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var published int64
	deadline := time.Now().Add(cfg.Duration)
	for time.Now().Before(deadline) {
		<-ticker.C
		binary.BigEndian.PutUint64(payload[:8], uint64(time.Now().UnixNano()))
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			result.Error = err
			return result
		}
		published++
	}

	// Wait for consumer to catch up (max 5 seconds)
	waitDeadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt64(&consumed) < published && time.Now().Before(waitDeadline) {
		time.Sleep(10 * time.Millisecond)
	}

	result.P50 = hist.Percentile(50)
	result.P95 = hist.Percentile(95)
	result.P99 = hist.Percentile(99)
	result.Max = hist.Max()
	return result
}
