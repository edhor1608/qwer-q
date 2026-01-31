package scenarios

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/harness"
)

// RunThroughput runs a throughput benchmark against the given adapter.
func RunThroughput(ctx context.Context, adapter adapters.Adapter, cfg harness.Config) harness.ThroughputResult {
	result := harness.ThroughputResult{Name: adapter.Name()}

	if err := adapter.Setup(ctx); err != nil {
		result.Error = err
		return result
	}
	defer adapter.Teardown()

	queue := "bench-throughput"
	payload := make([]byte, cfg.MessageSize)

	var consumed int64
	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()

	// Start consumer
	go func() {
		adapter.Consume(consCtx, queue, func(data []byte) error {
			atomic.AddInt64(&consumed, 1)
			return nil
		})
	}()

	// Let consumer start
	time.Sleep(100 * time.Millisecond)

	// Publish as fast as possible
	var published int64
	deadline := time.Now().Add(cfg.Duration)
	for time.Now().Before(deadline) {
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

	result.Published = published
	result.MsgsPerSec = float64(published) / cfg.Duration.Seconds()
	return result
}
