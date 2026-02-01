package scenarios

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
)

// OrderingResult captures ordering test results
type OrderingResult struct {
	Name           string
	TotalMessages  int
	InOrder        int
	OutOfOrder     int
	Duplicates     int
	Missing        int
	OrderingRate   float64 // percentage in order
	Duration       time.Duration
	Error          string
}

// RunOrderingTest verifies FIFO ordering guarantees
// Publishes messages with sequence numbers, verifies order on consume
func RunOrderingTest(ctx context.Context, adapter adapters.Adapter, messageCount int, timeout time.Duration) (*OrderingResult, error) {
	if err := adapter.Setup(ctx); err != nil {
		return nil, err
	}
	defer adapter.Teardown()

	queue := fmt.Sprintf("bench-ordering-%d", time.Now().UnixNano())
	result := &OrderingResult{
		Name:          adapter.Name(),
		TotalMessages: messageCount,
	}

	start := time.Now()

	// Publish messages with sequence numbers
	for i := 0; i < messageCount; i++ {
		payload := make([]byte, 8)
		binary.BigEndian.PutUint64(payload, uint64(i))
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			result.Error = fmt.Sprintf("publish error at seq %d: %v", i, err)
			return result, nil
		}
	}

	// Consume and verify order
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var received atomic.Int64
	var lastSeq int64 = -1
	var outOfOrder, duplicates atomic.Int64
	seen := make(map[uint64]bool)
	var seenMu sync.Mutex

	done := make(chan struct{})
	go func() {
		adapter.Consume(ctx, queue, func(msg []byte) error {
			if len(msg) < 8 {
				return nil
			}
			seq := binary.BigEndian.Uint64(msg)

			seenMu.Lock()
			if seen[seq] {
				duplicates.Add(1)
			} else {
				seen[seq] = true
				current := atomic.LoadInt64(&lastSeq)
				if int64(seq) != current+1 && current != -1 {
					outOfOrder.Add(1)
				}
				atomic.StoreInt64(&lastSeq, int64(seq))
			}
			seenMu.Unlock()

			count := received.Add(1)
			if count >= int64(messageCount) {
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
	result.InOrder = int(received.Load()) - int(outOfOrder.Load())
	result.OutOfOrder = int(outOfOrder.Load())
	result.Duplicates = int(duplicates.Load())
	result.Missing = messageCount - int(received.Load())
	if result.TotalMessages > 0 {
		result.OrderingRate = float64(result.InOrder) / float64(result.TotalMessages) * 100
	}

	return result, nil
}

// PrintOrderingResults prints ordering test results
func PrintOrderingResults(results map[string]*OrderingResult) {
	fmt.Println("\n┌─────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐")
	fmt.Println("│ System      │ Total    │ In Order │ Out/Order│ Dupes    │ Missing  │ Order %  │")
	fmt.Println("├─────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤")
	for name, r := range results {
		fmt.Printf("│ %-11s │ %8d │ %8d │ %8d │ %8d │ %8d │ %6.1f%%  │\n",
			name, r.TotalMessages, r.InOrder, r.OutOfOrder, r.Duplicates, r.Missing, r.OrderingRate)
	}
	fmt.Println("└─────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘")
}
