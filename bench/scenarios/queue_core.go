package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/harness"
)

// QueueCoreConfig configures the first product-shaped benchmark suite.
type QueueCoreConfig struct {
	Config             harness.Config
	OrderingMessages   int
	OrderingTimeout    time.Duration
	RedeliveryMessages int
	RedeliveryHold     time.Duration
	RedeliveryTimeout  time.Duration
}

// RunQueueCore runs the first product-shaped queue benchmark.
// It combines the core queue dimensions QWER-Q wants to win:
// throughput, latency, ordering, and redelivery behavior.
func RunQueueCore(ctx context.Context, adapter adapters.Adapter, cfg QueueCoreConfig) harness.QueueCoreResult {
	result := harness.QueueCoreResult{Name: adapter.Name()}

	throughput := RunThroughput(ctx, adapter, cfg.Config)
	if throughput.Error != nil {
		result.Error = fmt.Errorf("throughput: %w", throughput.Error)
		return result
	}
	result.ThroughputMsgsPerSec = throughput.MsgsPerSec

	latency := RunLatency(ctx, adapter, cfg.Config)
	if latency.Error != nil {
		result.Error = fmt.Errorf("latency: %w", latency.Error)
		return result
	}
	result.LatencyP95 = latency.P95

	ordering, err := RunOrderingTest(ctx, adapter, cfg.OrderingMessages, cfg.OrderingTimeout)
	if err != nil {
		result.Error = fmt.Errorf("ordering: %w", err)
		return result
	}
	if ordering.Error != "" {
		result.Error = fmt.Errorf("ordering: %s", ordering.Error)
		return result
	}
	result.OrderingRate = ordering.OrderingRate

	redelivery, err := RunRedeliveryTest(ctx, adapter, cfg.RedeliveryMessages, cfg.RedeliveryHold, cfg.RedeliveryTimeout)
	if err != nil {
		result.Error = fmt.Errorf("redelivery: %w", err)
		return result
	}
	if redelivery.Error != "" {
		result.Error = fmt.Errorf("redelivery: %s", redelivery.Error)
		return result
	}
	result.RedeliveryRate = redelivery.RedeliveryRate
	result.SupportsRedelivery = redelivery.SupportsRedeliver
	result.Behavior = redelivery.Behavior

	return result
}
