package scenarios

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// FairnessResult captures multi-consumer fairness test results
type FairnessResult struct {
	Name            string
	NumConsumers    int
	TotalMessages   int
	Distribution    []int64   // messages per consumer
	MinReceived     int64
	MaxReceived     int64
	AvgReceived     float64
	StdDev          float64
	FairnessIndex   float64   // 0-100%, higher = more fair
	Behavior        string
	Error           string
}

// RunFairnessTest tests message distribution among multiple competing consumers
func RunFairnessTest(ctx context.Context, adapterFactory func() adapters.Adapter, numConsumers int, messageCount int, timeout time.Duration) (*FairnessResult, error) {
	// Create separate adapter for publisher
	pubAdapter := adapterFactory()
	if err := pubAdapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer pubAdapter.Teardown()

	queue := fmt.Sprintf("bench-fairness-%d", time.Now().UnixNano())
	result := &FairnessResult{
		Name:          pubAdapter.Name(),
		NumConsumers:  numConsumers,
		TotalMessages: messageCount,
		Distribution:  make([]int64, numConsumers),
	}

	// Track messages received by each consumer
	consumerCounts := make([]atomic.Int64, numConsumers)
	var totalReceived atomic.Int64

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup

	// Start N consumers
	for i := 0; i < numConsumers; i++ {
		consumerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each consumer needs its own adapter/connection
			conAdapter := adapterFactory()
			if err := conAdapter.Setup(ctx); err != nil {
				return
			}
			defer conAdapter.Teardown()

			conAdapter.Consume(ctx, queue, func(msg []byte) error {
				consumerCounts[consumerID].Add(1)
				if totalReceived.Add(1) >= int64(messageCount) {
					cancel() // All messages received
				}
				return nil
			})
		}()
	}

	// Give consumers time to connect
	time.Sleep(500 * time.Millisecond)

	// Publish all messages
	payload := make([]byte, 256)
	for i := 0; i < messageCount; i++ {
		if err := pubAdapter.Publish(ctx, queue, payload); err != nil {
			result.Error = fmt.Sprintf("publish error at %d: %v", i, err)
			break
		}
	}

	// Wait for completion or timeout
	<-ctx.Done()

	// Allow consumers to finish processing
	time.Sleep(500 * time.Millisecond)

	// Collect results
	var sum int64
	result.MinReceived = int64(messageCount)
	result.MaxReceived = 0

	for i := 0; i < numConsumers; i++ {
		count := consumerCounts[i].Load()
		result.Distribution[i] = count
		sum += count

		if count < result.MinReceived {
			result.MinReceived = count
		}
		if count > result.MaxReceived {
			result.MaxReceived = count
		}
	}

	result.AvgReceived = float64(sum) / float64(numConsumers)

	// Calculate standard deviation
	var variance float64
	for i := 0; i < numConsumers; i++ {
		diff := float64(result.Distribution[i]) - result.AvgReceived
		variance += diff * diff
	}
	result.StdDev = math.Sqrt(variance / float64(numConsumers))

	// Calculate Jain's Fairness Index: (sum(xi))^2 / (n * sum(xi^2))
	// 1.0 = perfectly fair, 1/n = completely unfair
	var sumSquared float64
	for i := 0; i < numConsumers; i++ {
		sumSquared += float64(result.Distribution[i]) * float64(result.Distribution[i])
	}
	if sumSquared > 0 {
		result.FairnessIndex = (float64(sum) * float64(sum)) / (float64(numConsumers) * sumSquared) * 100
	}

	// Determine behavior
	switch {
	case result.FairnessIndex >= 95:
		result.Behavior = "Excellent fairness (even distribution)"
	case result.FairnessIndex >= 80:
		result.Behavior = "Good fairness (minor variance)"
	case result.FairnessIndex >= 50:
		result.Behavior = "Moderate fairness (some consumers favored)"
	case result.MaxReceived > 0 && result.MinReceived == 0:
		result.Behavior = "Poor: Some consumers got nothing"
	default:
		result.Behavior = "Unfair distribution"
	}

	return result, nil
}

// PrintFairnessResults prints fairness test results
func PrintFairnessResults(results map[string]*FairnessResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬─────────────────────────────────┐")
	fmt.Println("│ System      │ Consumers│ Messages │ Min      │ Max      │ Fair %   │ Behavior                        │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼─────────────────────────────────┤")
	for name, r := range results {
		fmt.Printf("│ %-11s │ %8d │ %8d │ %8d │ %8d │ %6.1f%%  │ %-31s │\n",
			name, r.NumConsumers, r.TotalMessages, r.MinReceived, r.MaxReceived, r.FairnessIndex, r.Behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴─────────────────────────────────┘")

	// Print distribution details
	fmt.Println("\nDistribution per consumer:")
	for name, r := range results {
		fmt.Printf("  %s: %v (stddev: %.1f)\n", name, r.Distribution, r.StdDev)
	}

	fmt.Println("\nFairness Interpretation (Jain's Index):")
	fmt.Println("  - 100%: Perfect fairness (all consumers equal)")
	fmt.Println("  - 80-99%: Good fairness for production use")
	fmt.Println("  - <50%: Significant unfairness (some consumers starved)")
}
