package scenarios

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// TTLResult captures message TTL/expiry test results
type TTLResult struct {
	Name             string
	Published        int
	WaitTime         time.Duration
	ConsumedAfter    int64
	ExpiredCount     int64 // Published - Consumed = Expired
	ExpirySupported  bool
	Behavior         string
	Error            string
}

// RunTTLTest tests message expiry behavior
// Note: Most adapters don't support TTL at the message level
// This test documents the behavior - messages typically don't expire
func RunTTLTest(ctx context.Context, adapter adapters.Adapter, messageCount int, waitTime time.Duration, consumeTimeout time.Duration) (*TTLResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-ttl-%d", time.Now().UnixNano())
	payload := make([]byte, 256)

	result := &TTLResult{
		Name:      adapter.Name(),
		Published: messageCount,
		WaitTime:  waitTime,
	}

	// Publish messages
	fmt.Printf("  Publishing %d messages...\n", messageCount)
	for i := 0; i < messageCount; i++ {
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			result.Error = fmt.Sprintf("publish error: %v", err)
			return result, nil
		}
	}

	// Wait for "expiry" time
	// In a real TTL-supporting system, messages would expire during this wait
	fmt.Printf("  Waiting %v for potential message expiry...\n", waitTime)
	time.Sleep(waitTime)

	// Try to consume messages
	fmt.Printf("  Consuming (timeout %v)...\n", consumeTimeout)
	var consumed atomic.Int64

	consumeCtx, cancel := context.WithTimeout(ctx, consumeTimeout)
	defer cancel()

	done := make(chan bool)
	go func() {
		adapter.Consume(consumeCtx, queue, func(msg []byte) error {
			if consumed.Add(1) >= int64(messageCount) {
				cancel()
			}
			return nil
		})
		done <- true
	}()

	<-done

	result.ConsumedAfter = consumed.Load()
	result.ExpiredCount = int64(messageCount) - result.ConsumedAfter
	result.ExpirySupported = result.ExpiredCount > 0

	// Determine behavior
	switch {
	case result.ConsumedAfter == int64(messageCount):
		result.Behavior = "No expiry - all messages delivered"
	case result.ConsumedAfter == 0:
		result.Behavior = "All messages expired (or queue issue)"
	case result.ExpiredCount > 0:
		result.Behavior = fmt.Sprintf("%d expired, %d delivered", result.ExpiredCount, result.ConsumedAfter)
	default:
		result.Behavior = "Unknown behavior"
	}

	return result, nil
}

// PrintTTLResults prints TTL test results
func PrintTTLResults(results map[string]*TTLResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬─────────────────────────────────┐")
	fmt.Println("│ System      │ Published│ Wait     │ Consumed │ Expired  │ Has TTL? │ Behavior                        │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼─────────────────────────────────┤")
	for name, r := range results {
		hasTTL := "No"
		if r.ExpirySupported {
			hasTTL = "Yes"
		}
		fmt.Printf("│ %-11s │ %8d │ %8s │ %8d │ %8d │ %-8s │ %-31s │\n",
			name, r.Published, r.WaitTime.String(), r.ConsumedAfter, r.ExpiredCount, hasTTL, r.Behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴─────────────────────────────────┘")

	fmt.Println("\nMessage TTL/Expiry:")
	fmt.Println("  - Most queues do NOT expire messages by default")
	fmt.Println("  - TTL typically requires explicit configuration per queue/message")
	fmt.Println("  - Without TTL, unconsumed messages accumulate until memory/disk exhaustion")
	fmt.Println("  - RabbitMQ supports queue-level TTL, Kafka uses retention policies")
}
