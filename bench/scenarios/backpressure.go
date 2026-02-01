package scenarios

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// BackpressureResult captures backpressure behavior test results
type BackpressureResult struct {
	Name             string
	Duration         time.Duration
	Published        int64
	Consumed         int64
	PubErrors        int64
	ConsumerDelay    time.Duration
	ProducerRate     float64 // actual msgs/sec
	ConsumerRate     float64 // actual msgs/sec
	QueueGrowth      int64   // published - consumed at end
	MemoryStartMB    float64
	MemoryEndMB      float64
	MemoryGrowthMB   float64
	ProducerBlocked  bool    // true if producer rate dropped significantly
	ErrorsUnderLoad  bool    // true if publish errors occurred
	Behavior         string  // summary of observed behavior
}

// RunBackpressureTest tests behavior when consumer can't keep up with producer
// Producer publishes as fast as possible, consumer has artificial delay
func RunBackpressureTest(ctx context.Context, adapter adapters.Adapter, duration time.Duration, consumerDelay time.Duration) (*BackpressureResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-backpressure-%d", time.Now().UnixNano())
	payload := make([]byte, 1024) // 1KB messages

	result := &BackpressureResult{
		Name:          adapter.Name(),
		ConsumerDelay: consumerDelay,
	}

	// Capture start memory
	var startMem runtime.MemStats
	runtime.ReadMemStats(&startMem)
	result.MemoryStartMB = float64(startMem.Alloc) / 1024 / 1024

	var published, consumed atomic.Int64
	var pubErrors atomic.Int64

	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	start := time.Now()
	var wg sync.WaitGroup

	// Start slow consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		adapter.Consume(ctx, queue, func(msg []byte) error {
			time.Sleep(consumerDelay) // Simulate slow processing
			consumed.Add(1)
			return nil
		})
	}()

	// Start fast producer
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

	// Wait for test duration
	<-ctx.Done()
	result.Duration = time.Since(start)

	// Give consumer a moment to finish in-flight work
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Capture end memory
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)
	result.MemoryEndMB = float64(endMem.Alloc) / 1024 / 1024
	result.MemoryGrowthMB = result.MemoryEndMB - result.MemoryStartMB

	result.Published = published.Load()
	result.Consumed = consumed.Load()
	result.PubErrors = pubErrors.Load()
	result.QueueGrowth = result.Published - result.Consumed
	result.ProducerRate = float64(result.Published) / result.Duration.Seconds()
	result.ConsumerRate = float64(result.Consumed) / result.Duration.Seconds()

	// Analyze behavior
	result.ErrorsUnderLoad = result.PubErrors > 0

	// If producer rate is much lower than expected (< 1000/s for 1KB messages), it's being blocked
	result.ProducerBlocked = result.ProducerRate < 1000

	// Determine behavior summary
	switch {
	case result.ErrorsUnderLoad:
		result.Behavior = "Rejects: Producer gets errors when queue is full"
	case result.ProducerBlocked:
		result.Behavior = "Blocks: Producer slows down when queue is full"
	case result.MemoryGrowthMB > 100:
		result.Behavior = "Unbounded: Queue grows without limit (memory risk)"
	default:
		result.Behavior = "Absorbs: Queue handles backlog efficiently"
	}

	return result, nil
}

// PrintBackpressureResults prints backpressure test results
func PrintBackpressureResults(results map[string]*BackpressureResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬─────────────────────────────────────┐")
	fmt.Println("│ System      │ Pub/sec  │ Con/sec  │ Errors   │ Backlog  │ Mem +MB  │ Behavior                            │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼─────────────────────────────────────┤")
	for name, r := range results {
		fmt.Printf("│ %-11s │ %8.0f │ %8.0f │ %8d │ %8d │ %8.1f │ %-35s │\n",
			name, r.ProducerRate, r.ConsumerRate, r.PubErrors, r.QueueGrowth, r.MemoryGrowthMB, r.Behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴─────────────────────────────────────┘")

	fmt.Println("\nBackpressure Behaviors:")
	fmt.Println("  - Rejects: Best for protecting system resources, producer must handle errors")
	fmt.Println("  - Blocks: Producer naturally slows to consumer rate, may cause timeouts")
	fmt.Println("  - Unbounded: Dangerous - can lead to OOM, but maximizes throughput short-term")
	fmt.Println("  - Absorbs: Queue handles pressure gracefully with bounded resources")
}
