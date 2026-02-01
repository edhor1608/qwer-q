package scenarios

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// LargeMsgResult captures large message handling test results
type LargeMsgResult struct {
	Name          string
	SizesTested   []int
	SizesSuccess  []bool
	MaxSuccessful int    // largest size that worked
	FirstFailure  int    // smallest size that failed (0 = none failed)
	FailureError  string // error message at first failure
	Behavior      string
}

// LargeMsgSizeResult holds result for a single size test
type LargeMsgSizeResult struct {
	Size       int
	Success    bool
	Throughput float64 // msgs/sec
	Error      string
}

// RunLargeMessageTest tests handling of increasingly large messages
func RunLargeMessageTest(ctx context.Context, adapter adapters.Adapter, timeout time.Duration) (*LargeMsgResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	// Test sizes: 10KB, 100KB, 500KB, 1MB, 5MB, 10MB
	sizes := []int{
		10 * 1024,        // 10KB
		100 * 1024,       // 100KB
		500 * 1024,       // 500KB
		1 * 1024 * 1024,  // 1MB
		5 * 1024 * 1024,  // 5MB
		10 * 1024 * 1024, // 10MB
	}

	result := &LargeMsgResult{
		Name:         adapter.Name(),
		SizesTested:  sizes,
		SizesSuccess: make([]bool, len(sizes)),
	}

	queue := fmt.Sprintf("bench-largemsg-%d", time.Now().UnixNano())

	for i, size := range sizes {
		fmt.Printf("  Testing %s message...\n", formatSize(size))

		payload := make([]byte, size)
		// Fill with pattern for verification
		for j := range payload {
			payload[j] = byte(j % 256)
		}

		// Try to publish
		pubCtx, pubCancel := context.WithTimeout(ctx, timeout)
		err := adapter.Publish(pubCtx, queue, payload)
		pubCancel()

		if err != nil {
			result.SizesSuccess[i] = false
			if result.FirstFailure == 0 {
				result.FirstFailure = size
				result.FailureError = err.Error()
			}
			fmt.Printf("    FAILED: %v\n", err)
			continue
		}

		// Try to consume
		var received atomic.Int64
		var receivedSize int

		conCtx, conCancel := context.WithTimeout(ctx, timeout)
		done := make(chan bool, 1)

		go func() {
			adapter.Consume(conCtx, queue, func(msg []byte) error {
				received.Add(1)
				receivedSize = len(msg)
				conCancel()
				return nil
			})
			done <- true
		}()

		<-done

		if received.Load() > 0 && receivedSize == size {
			result.SizesSuccess[i] = true
			result.MaxSuccessful = size
			fmt.Printf("    OK: Published and consumed successfully\n")
		} else if received.Load() > 0 {
			result.SizesSuccess[i] = false
			if result.FirstFailure == 0 {
				result.FirstFailure = size
				result.FailureError = fmt.Sprintf("size mismatch: sent %d, got %d", size, receivedSize)
			}
			fmt.Printf("    FAILED: Size mismatch (sent %d, got %d)\n", size, receivedSize)
		} else {
			result.SizesSuccess[i] = false
			if result.FirstFailure == 0 {
				result.FirstFailure = size
				result.FailureError = "message not received"
			}
			fmt.Printf("    FAILED: Message not received\n")
		}
	}

	// Determine behavior
	if result.FirstFailure == 0 {
		result.Behavior = fmt.Sprintf("All sizes up to %s supported", formatSize(sizes[len(sizes)-1]))
	} else if result.MaxSuccessful > 0 {
		result.Behavior = fmt.Sprintf("Max size: %s (failed at %s)", formatSize(result.MaxSuccessful), formatSize(result.FirstFailure))
	} else {
		result.Behavior = fmt.Sprintf("Failed at smallest size (%s)", formatSize(sizes[0]))
	}

	return result, nil
}

func formatSize(bytes int) string {
	switch {
	case bytes >= 1024*1024:
		return fmt.Sprintf("%dMB", bytes/(1024*1024))
	case bytes >= 1024:
		return fmt.Sprintf("%dKB", bytes/1024)
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

// PrintLargeMsgResults prints large message test results
func PrintLargeMsgResults(results map[string]*LargeMsgResult) {
	fmt.Println("\n┌─────────────┬────────┬────────┬────────┬────────┬────────┬────────┬─────────────────────────────────┐")
	fmt.Println("│ System      │ 10KB   │ 100KB  │ 500KB  │ 1MB    │ 5MB    │ 10MB   │ Max Successful                  │")
	fmt.Println("├─────────────┼────────┼────────┼────────┼────────┼────────┼────────┼─────────────────────────────────┤")
	for name, r := range results {
		row := fmt.Sprintf("│ %-11s │", name)
		for _, success := range r.SizesSuccess {
			if success {
				row += "   ✓    │"
			} else {
				row += "   ✗    │"
			}
		}
		row += fmt.Sprintf(" %-31s │", r.Behavior)
		fmt.Println(row)
	}
	fmt.Println("└─────────────┴────────┴────────┴────────┴────────┴────────┴────────┴─────────────────────────────────┘")

	// Print failure details
	fmt.Println("\nFailure details:")
	for name, r := range results {
		if r.FailureError != "" {
			fmt.Printf("  %s: %s\n", name, r.FailureError)
		}
	}

	fmt.Println("\nLarge Message Handling:")
	fmt.Println("  - Many systems have max message size limits (often 1MB default)")
	fmt.Println("  - Kafka default: 1MB, RabbitMQ: 128MB, NATS: 1MB")
	fmt.Println("  - Large messages impact memory and network performance")
	fmt.Println("  - Consider chunking for very large payloads")
}
