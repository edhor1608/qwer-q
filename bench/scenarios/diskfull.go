package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// DiskFullResult captures disk full handling test results
type DiskFullResult struct {
	Name              string
	MessagesPublished int64
	FailedAt          int64  // message number where publish failed
	ErrorMessage      string // the error returned
	GracefulError     bool   // did it return clean error vs crash?
	RecoveredAfter    bool   // could it resume after space freed?
	Behavior          string
}

// RunDiskFullTest tests behavior when storage fills up
// Note: This requires Docker volume limits or tmpfs with size constraint
// For now, this tests the error handling path by publishing until failure
func RunDiskFullTest(ctx context.Context, adapter adapters.Adapter, timeout time.Duration) (*DiskFullResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-diskfull-%d", time.Now().UnixNano())
	result := &DiskFullResult{
		Name: adapter.Name(),
	}

	// Use large payload to fill storage faster
	// 1MB per message
	payload := make([]byte, 1024*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fmt.Printf("  Publishing 1MB messages until failure or timeout...\n")

	var published int64
	var lastErr error

	for {
		select {
		case <-ctx.Done():
			// Timeout - didn't hit disk full
			result.MessagesPublished = published
			result.Behavior = fmt.Sprintf("Published %d MB without hitting limit", published)
			return result, nil
		default:
		}

		if err := adapter.Publish(ctx, queue, payload); err != nil {
			lastErr = err
			result.FailedAt = published + 1
			result.ErrorMessage = err.Error()
			break
		}
		published++

		// Progress indicator every 100 messages
		if published%100 == 0 {
			fmt.Printf("    Published %d MB...\n", published)
		}
	}

	result.MessagesPublished = published

	// Analyze the error
	if lastErr != nil {
		errStr := strings.ToLower(lastErr.Error())
		// Check if it's a graceful "disk full" type error vs crash/panic
		isGraceful := strings.Contains(errStr, "disk") ||
			strings.Contains(errStr, "space") ||
			strings.Contains(errStr, "quota") ||
			strings.Contains(errStr, "full") ||
			strings.Contains(errStr, "limit") ||
			strings.Contains(errStr, "memory") ||
			strings.Contains(errStr, "backpressure")

		result.GracefulError = isGraceful

		if isGraceful {
			result.Behavior = fmt.Sprintf("Graceful error after %d MB: %s", published, result.ErrorMessage)
		} else {
			result.Behavior = fmt.Sprintf("Unknown error after %d MB: %s", published, result.ErrorMessage)
		}
	}

	return result, nil
}

// PrintDiskFullResults prints disk full test results
func PrintDiskFullResults(results map[string]*DiskFullResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬─────────────────────────────────────────────┐")
	fmt.Println("│ System      │ MB Wrote │ Failed@  │ Graceful │ Behavior                                    │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼─────────────────────────────────────────────┤")
	for name, r := range results {
		graceful := "?"
		if r.FailedAt > 0 {
			if r.GracefulError {
				graceful = "Yes"
			} else {
				graceful = "No"
			}
		}
		failedAt := "-"
		if r.FailedAt > 0 {
			failedAt = fmt.Sprintf("%d", r.FailedAt)
		}
		behavior := r.Behavior
		if len(behavior) > 43 {
			behavior = behavior[:40] + "..."
		}
		fmt.Printf("│ %-11s │ %8d │ %8s │ %-8s │ %-43s │\n",
			name, r.MessagesPublished, failedAt, graceful, behavior)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴─────────────────────────────────────────────┘")

	fmt.Println("\nDisk Full Handling:")
	fmt.Println("  - Graceful error: Returns clean error, system stays operational")
	fmt.Println("  - Non-graceful: Crash, hang, or corruption")
	fmt.Println("  - Best practice: Backpressure before disk full, clear error messages")
	fmt.Println("  - Note: Test with Docker volume limits for real disk full simulation")
}
