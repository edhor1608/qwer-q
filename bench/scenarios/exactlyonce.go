package scenarios

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// ExactlyOnceResult captures exactly-once/idempotency test results
type ExactlyOnceResult struct {
	Name             string
	UniqueMessages   int
	PublishAttempts  int
	ReceivedTotal    int
	ReceivedUnique   int
	Duplicates       int
	DeduplicationRate float64 // percentage of duplicates prevented
	Duration         time.Duration
	Error            string
}

// RunExactlyOnceTest verifies deduplication/idempotency behavior
// Publishes same message ID multiple times, verifies only delivered once
func RunExactlyOnceTest(ctx context.Context, adapter adapters.Adapter, uniqueMessages, duplicatesPerMsg int, timeout time.Duration) (*ExactlyOnceResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-exactlyonce-%d", time.Now().UnixNano())
	result := &ExactlyOnceResult{
		Name:            adapter.Name(),
		UniqueMessages:  uniqueMessages,
		PublishAttempts: uniqueMessages * duplicatesPerMsg,
	}

	start := time.Now()

	// Generate unique message IDs and publish each multiple times
	messageIDs := make([]string, uniqueMessages)
	for i := 0; i < uniqueMessages; i++ {
		id := make([]byte, 16)
		rand.Read(id)
		messageIDs[i] = hex.EncodeToString(id)
	}

	// Publish each message multiple times (simulating retries)
	for _, msgID := range messageIDs {
		payload := []byte(msgID)
		for j := 0; j < duplicatesPerMsg; j++ {
			if err := adapter.Publish(ctx, queue, payload); err != nil {
				result.Error = fmt.Sprintf("publish error: %v", err)
				return result, nil
			}
		}
	}

	// Consume and count unique vs duplicates
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var received atomic.Int64
	seen := make(map[string]int)
	var seenMu sync.Mutex

	done := make(chan struct{})
	expectedTotal := uniqueMessages * duplicatesPerMsg

	go func() {
		adapter.Consume(ctx, queue, func(msg []byte) error {
			msgID := string(msg)

			seenMu.Lock()
			seen[msgID]++
			seenMu.Unlock()

			count := received.Add(1)
			// Stop after receiving expected number (or more if no dedup)
			if count >= int64(expectedTotal) {
				close(done)
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	result.Duration = time.Since(start)
	result.ReceivedTotal = int(received.Load())

	// Count unique messages and duplicates
	seenMu.Lock()
	result.ReceivedUnique = len(seen)
	for _, count := range seen {
		if count > 1 {
			result.Duplicates += count - 1
		}
	}
	seenMu.Unlock()

	// Calculate deduplication rate
	// If system has perfect dedup: ReceivedTotal == UniqueMessages
	// If system has no dedup: ReceivedTotal == PublishAttempts
	if result.PublishAttempts > result.UniqueMessages {
		expectedDupes := result.PublishAttempts - result.UniqueMessages
		actualDupes := result.ReceivedTotal - result.ReceivedUnique
		prevented := expectedDupes - actualDupes
		if expectedDupes > 0 {
			result.DeduplicationRate = float64(prevented) / float64(expectedDupes) * 100
		}
	}

	return result, nil
}

// PrintExactlyOnceResults prints exactly-once test results
func PrintExactlyOnceResults(results map[string]*ExactlyOnceResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐")
	fmt.Println("│ System      │ Unique   │ Attempts │ Received │ Rx Uniq  │ Dupes    │ Dedup %  │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤")
	for name, r := range results {
		fmt.Printf("│ %-11s │ %8d │ %8d │ %8d │ %8d │ %8d │ %6.1f%%  │\n",
			name, r.UniqueMessages, r.PublishAttempts, r.ReceivedTotal, r.ReceivedUnique, r.Duplicates, r.DeduplicationRate)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘")

	fmt.Println("\nInterpretation:")
	fmt.Println("  - Dedup 100%: System prevents all duplicate deliveries (exactly-once)")
	fmt.Println("  - Dedup 0%: System delivers all messages including duplicates (at-least-once)")
	fmt.Println("  - Note: Most queues are at-least-once by design. Exactly-once requires app-level dedup.")
}
