package scenarios

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// ManyQueuesResult captures many queues scalability test results
type ManyQueuesResult struct {
	Name               string
	QueueCounts        []int           // queue counts tested
	CreateTimes        []time.Duration // time to create N queues
	PublishThroughputs []float64       // msgs/sec at each queue count
	ConsumeThroughputs []float64       // msgs/sec at each queue count
	MaxQueuesCreated   int             // highest successful queue count
	DegradationPercent float64         // throughput drop from 1 queue to max
	Behavior           string
	Error              string
}

// RunManyQueuesTest tests performance with increasing number of queues
func RunManyQueuesTest(ctx context.Context, adapter adapters.Adapter, msgsPerQueue int, timeout time.Duration) (*ManyQueuesResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	// Test with increasing queue counts
	queueCounts := []int{1, 10, 50, 100}

	result := &ManyQueuesResult{
		Name:               adapter.Name(),
		QueueCounts:        queueCounts,
		CreateTimes:        make([]time.Duration, len(queueCounts)),
		PublishThroughputs: make([]float64, len(queueCounts)),
		ConsumeThroughputs: make([]float64, len(queueCounts)),
	}

	payload := make([]byte, 256)
	baseQueueName := fmt.Sprintf("bench-many-%d", time.Now().UnixNano())

	for idx, numQueues := range queueCounts {
		fmt.Printf("  Testing with %d queues...\n", numQueues)

		// Generate queue names
		queues := make([]string, numQueues)
		for i := 0; i < numQueues; i++ {
			queues[i] = fmt.Sprintf("%s-%d", baseQueueName, i)
		}

		// Time queue "creation" (first publish to each)
		createStart := time.Now()
		for _, q := range queues {
			if err := adapter.Publish(ctx, q, payload); err != nil {
				result.Error = fmt.Sprintf("queue creation failed at %d queues: %v", numQueues, err)
				result.Behavior = fmt.Sprintf("Failed at %d queues", numQueues)
				return result, nil
			}
		}
		result.CreateTimes[idx] = time.Since(createStart)
		result.MaxQueuesCreated = numQueues

		// Publish throughput test - publish msgsPerQueue to each queue
		pubStart := time.Now()
		totalPub := 0
		for _, q := range queues {
			for i := 0; i < msgsPerQueue; i++ {
				if err := adapter.Publish(ctx, q, payload); err != nil {
					break
				}
				totalPub++
			}
		}
		pubDuration := time.Since(pubStart)
		result.PublishThroughputs[idx] = float64(totalPub) / pubDuration.Seconds()

		// Consume throughput test - consume from all queues concurrently
		var totalConsumed atomic.Int64
		var wg sync.WaitGroup

		conStart := time.Now()
		conCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		for _, q := range queues {
			queueName := q
			wg.Add(1)
			go func() {
				defer wg.Done()
				adapter.Consume(conCtx, queueName, func(msg []byte) error {
					totalConsumed.Add(1)
					return nil
				})
			}()
		}

		// Wait for context or completion
		<-conCtx.Done()
		cancel()
		wg.Wait()

		conDuration := time.Since(conStart)
		result.ConsumeThroughputs[idx] = float64(totalConsumed.Load()) / conDuration.Seconds()

		fmt.Printf("    Create: %v, Pub: %.0f msg/s, Con: %.0f msg/s\n",
			result.CreateTimes[idx].Round(time.Millisecond),
			result.PublishThroughputs[idx],
			result.ConsumeThroughputs[idx])
	}

	// Calculate degradation
	if len(result.PublishThroughputs) > 1 && result.PublishThroughputs[0] > 0 {
		first := result.PublishThroughputs[0]
		last := result.PublishThroughputs[len(result.PublishThroughputs)-1]
		result.DegradationPercent = (1 - last/first) * 100
	}

	// Determine behavior
	switch {
	case result.DegradationPercent < 10:
		result.Behavior = "Excellent scalability (< 10% degradation)"
	case result.DegradationPercent < 30:
		result.Behavior = "Good scalability (10-30% degradation)"
	case result.DegradationPercent < 50:
		result.Behavior = "Moderate scalability (30-50% degradation)"
	default:
		result.Behavior = fmt.Sprintf("Poor scalability (%.0f%% degradation)", result.DegradationPercent)
	}

	return result, nil
}

// PrintManyQueuesResults prints many queues test results
func PrintManyQueuesResults(results map[string]*ManyQueuesResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────────────────────────────────────┬─────────────────────────────────┐")
	fmt.Println("│ System      │ Max Q's  │ Throughput (msg/s) @ 1/10/50/100 queues  │ Behavior                        │")
	fmt.Println("├─────────────┼──────────┼──────────────────────────────────────────┼─────────────────────────────────┤")
	for name, r := range results {
		throughputs := ""
		for i, t := range r.PublishThroughputs {
			if i > 0 {
				throughputs += " / "
			}
			throughputs += fmt.Sprintf("%.0f", t)
		}
		behavior := r.Behavior
		if len(behavior) > 31 {
			behavior = behavior[:28] + "..."
		}
		fmt.Printf("│ %-11s │ %8d │ %-40s │ %-31s │\n",
			name, r.MaxQueuesCreated, throughputs, behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────────────────────────────────────┴─────────────────────────────────┘")

	// Print detailed per-count stats
	fmt.Println("\nDetailed creation times:")
	for name, r := range results {
		fmt.Printf("  %s: ", name)
		for i, count := range r.QueueCounts {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%d=%v", count, r.CreateTimes[i].Round(time.Millisecond))
		}
		fmt.Println()
	}

	fmt.Println("\nMany Queues Scalability:")
	fmt.Println("  - < 10% degradation: Handles queue growth efficiently")
	fmt.Println("  - 10-30% degradation: Acceptable for most use cases")
	fmt.Println("  - > 50% degradation: Consider queue consolidation or sharding")
	fmt.Println("  - Note: Real impact depends on access patterns (hot vs cold queues)")
}
