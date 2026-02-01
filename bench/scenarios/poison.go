package scenarios

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// PoisonResult captures poison message handling test results
type PoisonResult struct {
	Name             string
	PoisonDeliveries int64  // how many times poison msg was delivered
	NormalDelivered  int64  // normal messages delivered after poison
	QueueBlocked     bool   // did poison block subsequent messages?
	MaxRetries       int64  // max times any message was retried
	Behavior         string // summary of observed behavior
	Error            string
}

var errPoisonMessage = errors.New("simulated poison message failure")

// RunPoisonMessageTest tests behavior when a message consistently fails processing
// Does the system retry forever? Move to DLQ? Block the queue?
func RunPoisonMessageTest(ctx context.Context, adapter adapters.Adapter, normalMsgCount int, timeout time.Duration) (*PoisonResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-poison-%d", time.Now().UnixNano())
	result := &PoisonResult{
		Name: adapter.Name(),
	}

	// Publish: 1 poison message first, then normal messages
	poisonPayload := []byte("POISON")
	normalPayload := []byte("NORMAL")

	// Publish poison message first
	if err := adapter.Publish(ctx, queue, poisonPayload); err != nil {
		result.Error = fmt.Sprintf("publish poison failed: %v", err)
		return result, nil
	}

	// Publish normal messages after
	for i := 0; i < normalMsgCount; i++ {
		if err := adapter.Publish(ctx, queue, normalPayload); err != nil {
			result.Error = fmt.Sprintf("publish normal failed: %v", err)
			return result, nil
		}
	}

	// Consume - always fail on poison, succeed on normal
	var poisonDeliveries, normalDelivered atomic.Int64

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	go func() {
		adapter.Consume(ctx, queue, func(msg []byte) error {
			if string(msg) == "POISON" {
				poisonDeliveries.Add(1)
				// Always fail on poison message
				return errPoisonMessage
			}
			// Normal message - process successfully
			normalDelivered.Add(1)
			return nil
		})
	}()

	// Wait for test duration
	<-ctx.Done()

	result.PoisonDeliveries = poisonDeliveries.Load()
	result.NormalDelivered = normalDelivered.Load()
	result.MaxRetries = result.PoisonDeliveries

	// Determine if queue was blocked
	// If we got normal messages, queue wasn't fully blocked
	result.QueueBlocked = result.NormalDelivered == 0 && result.PoisonDeliveries > 0

	// Determine behavior
	switch {
	case result.PoisonDeliveries == 0:
		result.Behavior = "No delivery attempted"
	case result.PoisonDeliveries == 1 && result.NormalDelivered == int64(normalMsgCount):
		result.Behavior = "Failed once, moved on (possible DLQ)"
	case result.PoisonDeliveries > 1 && result.NormalDelivered == int64(normalMsgCount):
		result.Behavior = fmt.Sprintf("Retried %dx then moved on", result.PoisonDeliveries)
	case result.QueueBlocked:
		result.Behavior = fmt.Sprintf("BLOCKED QUEUE after %d retries", result.PoisonDeliveries)
	case result.PoisonDeliveries > 10 && result.NormalDelivered < int64(normalMsgCount):
		result.Behavior = fmt.Sprintf("Infinite retry loop (%d+ attempts)", result.PoisonDeliveries)
	default:
		result.Behavior = fmt.Sprintf("Partial: %d poison, %d normal", result.PoisonDeliveries, result.NormalDelivered)
	}

	return result, nil
}

// PrintPoisonResults prints poison message test results
func PrintPoisonResults(results map[string]*PoisonResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬─────────────────────────────────────────────┐")
	fmt.Println("│ System      │ Poison   │ Normal   │ Blocked? │ Behavior                                    │")
	fmt.Println("│             │ Attempts │ Delivered│          │                                             │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼─────────────────────────────────────────────┤")
	for name, r := range results {
		blocked := "No"
		if r.QueueBlocked {
			blocked = "YES"
		}
		fmt.Printf("│ %-11s │ %8d │ %8d │ %-8s │ %-43s │\n",
			name, r.PoisonDeliveries, r.NormalDelivered, blocked, r.Behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴─────────────────────────────────────────────┘")

	fmt.Println("\nPoison Message Handling:")
	fmt.Println("  - 'Moved on': System has max retry limit or DLQ (safe)")
	fmt.Println("  - 'BLOCKED': Poison message prevents all subsequent processing (dangerous)")
	fmt.Println("  - 'Infinite retry': System keeps retrying forever (resource waste)")
	fmt.Println("  - Best practice: Limited retries + Dead Letter Queue")
}
