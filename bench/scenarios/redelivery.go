package scenarios

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// RedeliveryResult captures redelivery timeout test results
type RedeliveryResult struct {
	Name              string
	MessagesPublished int
	FirstDeliveries   int64
	Redeliveries      int64
	TotalDeliveries   int64
	RedeliveryRate    float64 // percentage of messages redelivered
	AvgRedeliveryTime time.Duration
	SupportsRedeliver bool
	Behavior          string
	Error             string
}

// RunRedeliveryTest tests message redelivery when consumer doesn't ACK in time
// Consumer fetches message but holds it without ACK - does it get redelivered?
func RunRedeliveryTest(ctx context.Context, adapter adapters.Adapter, messageCount int, holdTime time.Duration, timeout time.Duration) (*RedeliveryResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-redelivery-%d", time.Now().UnixNano())
	payload := make([]byte, 256)

	result := &RedeliveryResult{
		Name:              adapter.Name(),
		MessagesPublished: messageCount,
	}

	// Publish messages
	for i := 0; i < messageCount; i++ {
		payload[0] = byte(i)
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			result.Error = fmt.Sprintf("publish error: %v", err)
			return result, nil
		}
	}

	// Track deliveries
	deliveryCount := make(map[byte]int)
	var mu sync.Mutex
	var firstDeliveries, redeliveries atomic.Int64
	redeliveryTimes := make([]time.Duration, 0)
	firstDeliveryTime := make(map[byte]time.Time)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Consumer that holds messages without ACK (simulating slow/stuck processing)
	// Note: This behavior depends on adapter implementation
	// Some adapters auto-ACK, some require explicit ACK
	go func() {
		adapter.Consume(ctx, queue, func(msg []byte) error {
			if len(msg) == 0 {
				return nil
			}
			msgID := msg[0]

			mu.Lock()
			count := deliveryCount[msgID]
			deliveryCount[msgID]++

			if count == 0 {
				firstDeliveries.Add(1)
				firstDeliveryTime[msgID] = time.Now()
			} else {
				redeliveries.Add(1)
				if firstTime, ok := firstDeliveryTime[msgID]; ok {
					redeliveryTimes = append(redeliveryTimes, time.Since(firstTime))
				}
			}
			mu.Unlock()

			// Hold message without acknowledging it.
			// Adapters that support at-least-once semantics can use this to trigger redelivery.
			time.Sleep(holdTime)
			return adapters.ErrSkipAck
		})
	}()

	// Wait for test duration
	<-ctx.Done()

	result.FirstDeliveries = firstDeliveries.Load()
	result.Redeliveries = redeliveries.Load()
	result.TotalDeliveries = result.FirstDeliveries + result.Redeliveries

	if result.FirstDeliveries > 0 {
		result.RedeliveryRate = float64(result.Redeliveries) / float64(result.FirstDeliveries) * 100
	}

	// Calculate average redelivery time
	if len(redeliveryTimes) > 0 {
		var total time.Duration
		for _, t := range redeliveryTimes {
			total += t
		}
		result.AvgRedeliveryTime = total / time.Duration(len(redeliveryTimes))
	}

	result.SupportsRedeliver = result.Redeliveries > 0

	// Determine behavior
	switch {
	case result.Redeliveries > 0:
		result.Behavior = fmt.Sprintf("Redelivers after ~%v (at-least-once)", result.AvgRedeliveryTime.Round(time.Millisecond))
	case result.FirstDeliveries == int64(messageCount):
		result.Behavior = "No redelivery (auto-ACK or no visibility timeout)"
	default:
		result.Behavior = "Unknown behavior"
	}

	return result, nil
}

// PrintRedeliveryResults prints redelivery test results
func PrintRedeliveryResults(results map[string]*RedeliveryResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬─────────────────────────────────────┐")
	fmt.Println("│ System      │ Published│ 1st Dlvr │ Redelvr  │ Rate %   │ Behavior                            │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼─────────────────────────────────────┤")
	for name, r := range results {
		fmt.Printf("│ %-11s │ %8d │ %8d │ %8d │ %6.1f%%  │ %-35s │\n",
			name, r.MessagesPublished, r.FirstDeliveries, r.Redeliveries, r.RedeliveryRate, r.Behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴─────────────────────────────────────┘")

	fmt.Println("\nRedelivery Interpretation:")
	fmt.Println("  - Redelivers: System has visibility timeout, supports at-least-once delivery")
	fmt.Println("  - No redelivery: Auto-ACK on receive, or no timeout mechanism")
	fmt.Println("  - Note: High redelivery rate can cause duplicate processing")
}
