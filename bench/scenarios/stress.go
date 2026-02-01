package scenarios

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/harness"
)

// StressConfig defines stress test parameters
type StressConfig struct {
	Duration       time.Duration
	MessageSize    int
	Producers      int
	Consumers      int
	TargetRate     int // 0 = unlimited
	QueueDepthTest int // messages to pre-fill for queue depth test
}

// sample holds a point-in-time measurement (internal)
type sample struct {
	timestamp time.Time
	published int64
	consumed  int64
	memAlloc  uint64
}

// RunSustainedLoad tests performance over extended period
func RunSustainedLoad(ctx context.Context, adapter adapters.Adapter, cfg StressConfig) (*harness.Result, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-sustained-%d", time.Now().UnixNano())
	payload := make([]byte, cfg.MessageSize)

	var published, consumed atomic.Int64
	var pubErrors, conErrors atomic.Int64

	// Track performance over time
	samples := make([]sample, 0)
	var sampleMu sync.Mutex

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	var wg sync.WaitGroup

	// Start consumers
	for i := 0; i < cfg.Consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			adapter.Consume(ctx, queue, func(msg []byte) error {
				consumed.Add(1)
				return nil
			})
		}()
	}

	// Start producers
	for i := 0; i < cfg.Producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := adapter.Publish(ctx, queue, payload); err != nil {
						pubErrors.Add(1)
					} else {
						published.Add(1)
					}
				}
			}
		}()
	}

	// Sample metrics every second
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				sampleMu.Lock()
				samples = append(samples, sample{
					timestamp: time.Now(),
					published: published.Load(),
					consumed:  consumed.Load(),
					memAlloc:  m.Alloc,
				})
				sampleMu.Unlock()
			}
		}
	}()

	wg.Wait()

	return &harness.Result{
		Queue:       adapter.Name(),
		Published:   published.Load(),
		Consumed:    consumed.Load(),
		Duration:    cfg.Duration,
		PubErrors:   pubErrors.Load(),
		ConErrors:   conErrors.Load(),
		Samples:     convertSamples(samples),
	}, nil
}

func convertSamples(samples []sample) []harness.Sample {
	result := make([]harness.Sample, len(samples))
	for i, s := range samples {
		result[i] = harness.Sample{
			Timestamp: s.timestamp,
			Published: s.published,
			Consumed:  s.consumed,
			MemAlloc:  s.memAlloc,
		}
	}
	return result
}

// RunHighConcurrency tests with many producers/consumers
func RunHighConcurrency(ctx context.Context, adapter adapters.Adapter, cfg StressConfig) (*harness.Result, error) {
	return RunSustainedLoad(ctx, adapter, cfg)
}

// RunMessageSizes tests various message sizes
func RunMessageSizes(ctx context.Context, adapter adapters.Adapter, duration time.Duration) ([]harness.SizeResult, error) {
	sizes := []int{64, 256, 1024, 4096, 16384, 65536, 262144} // 64B to 256KB
	results := make([]harness.SizeResult, 0, len(sizes))

	for _, size := range sizes {
		if err := adapter.Setup(ctx); err != nil {
			return nil, err
		}

		queue := fmt.Sprintf("bench-size-%d-%d", size, time.Now().UnixNano())
		payload := make([]byte, size)

		var published atomic.Int64
		testCtx, cancel := context.WithTimeout(ctx, duration)

		var wg sync.WaitGroup

		// Consumer
		wg.Add(1)
		go func() {
			defer wg.Done()
			adapter.Consume(testCtx, queue, func(msg []byte) error {
				return nil
			})
		}()

		// Producer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					if err := adapter.Publish(testCtx, queue, payload); err == nil {
						published.Add(1)
					}
				}
			}
		}()

		wg.Wait()
		cancel()
		adapter.Teardown()

		results = append(results, harness.SizeResult{
			Size:     size,
			MsgsPerSec: float64(published.Load()) / duration.Seconds(),
			MBPerSec: float64(published.Load()) * float64(size) / duration.Seconds() / 1024 / 1024,
		})
	}

	return results, nil
}

// RunQueueDepth tests performance with pre-filled queue
func RunQueueDepth(ctx context.Context, adapter adapters.Adapter, prefill int, consumeDuration time.Duration) (*harness.DepthResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-depth-%d", time.Now().UnixNano())
	payload := make([]byte, 1024)

	// Pre-fill queue
	fmt.Printf("  Pre-filling %d messages...\n", prefill)
	fillStart := time.Now()
	for i := 0; i < prefill; i++ {
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			return nil, fmt.Errorf("prefill failed at %d: %w", i, err)
		}
		if i > 0 && i%100000 == 0 {
			fmt.Printf("    %d messages filled...\n", i)
		}
	}
	fillDuration := time.Since(fillStart)
	fmt.Printf("  Filled in %v (%.0f msgs/sec)\n", fillDuration, float64(prefill)/fillDuration.Seconds())

	// Now consume and measure
	var consumed atomic.Int64
	consumeStart := time.Now()

	testCtx, cancel := context.WithTimeout(ctx, consumeDuration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		adapter.Consume(testCtx, queue, func(msg []byte) error {
			consumed.Add(1)
			return nil
		})
	}()

	wg.Wait()
	consumeTime := time.Since(consumeStart)

	return &harness.DepthResult{
		Prefilled:     prefill,
		Consumed:      consumed.Load(),
		FillTime:      fillDuration,
		ConsumeTime:   consumeTime,
		FillRate:      float64(prefill) / fillDuration.Seconds(),
		ConsumeRate:   float64(consumed.Load()) / consumeTime.Seconds(),
	}, nil
}

// RunBurstTest tests spiky traffic patterns
func RunBurstTest(ctx context.Context, adapter adapters.Adapter, burstSize int, burstInterval time.Duration, totalDuration time.Duration) (*harness.BurstResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-burst-%d", time.Now().UnixNano())
	payload := make([]byte, 1024)

	var totalPublished, totalConsumed atomic.Int64
	burstLatencies := make([]time.Duration, 0)
	var latMu sync.Mutex

	ctx, cancel := context.WithTimeout(ctx, totalDuration)
	defer cancel()

	var wg sync.WaitGroup

	// Consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		adapter.Consume(ctx, queue, func(msg []byte) error {
			totalConsumed.Add(1)
			return nil
		})
	}()

	// Burst producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(burstInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				burstStart := time.Now()
				for i := 0; i < burstSize; i++ {
					if err := adapter.Publish(ctx, queue, payload); err == nil {
						totalPublished.Add(1)
					}
				}
				latMu.Lock()
				burstLatencies = append(burstLatencies, time.Since(burstStart))
				latMu.Unlock()
			}
		}
	}()

	wg.Wait()

	// Calculate burst stats
	var totalBurstTime time.Duration
	for _, lat := range burstLatencies {
		totalBurstTime += lat
	}
	avgBurstTime := time.Duration(0)
	if len(burstLatencies) > 0 {
		avgBurstTime = totalBurstTime / time.Duration(len(burstLatencies))
	}

	return &harness.BurstResult{
		BurstSize:     burstSize,
		BurstInterval: burstInterval,
		TotalBursts:   len(burstLatencies),
		TotalPublished: totalPublished.Load(),
		TotalConsumed: totalConsumed.Load(),
		AvgBurstTime:  avgBurstTime,
	}, nil
}

// RunConsumerLag tests behavior when consumers can't keep up
func RunConsumerLag(ctx context.Context, adapter adapters.Adapter, producerRate int, consumerDelay time.Duration, duration time.Duration) (*harness.LagResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-lag-%d", time.Now().UnixNano())
	payload := make([]byte, 1024)

	var published, consumed atomic.Int64
	lagSamples := make([]int64, 0)
	var lagMu sync.Mutex

	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup

	// Slow consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		adapter.Consume(ctx, queue, func(msg []byte) error {
			time.Sleep(consumerDelay)
			consumed.Add(1)
			return nil
		})
	}()

	// Rate-limited producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		interval := time.Second / time.Duration(producerRate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := adapter.Publish(ctx, queue, payload); err == nil {
					published.Add(1)
				}
			}
		}
	}()

	// Sample lag every second
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lag := published.Load() - consumed.Load()
				lagMu.Lock()
				lagSamples = append(lagSamples, lag)
				lagMu.Unlock()
			}
		}
	}()

	wg.Wait()

	// Calculate max lag
	var maxLag int64
	for _, lag := range lagSamples {
		if lag > maxLag {
			maxLag = lag
		}
	}

	return &harness.LagResult{
		ProducerRate:  producerRate,
		ConsumerDelay: consumerDelay,
		Published:     published.Load(),
		Consumed:      consumed.Load(),
		MaxLag:        maxLag,
		FinalLag:      published.Load() - consumed.Load(),
	}, nil
}
