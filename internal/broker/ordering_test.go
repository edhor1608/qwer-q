package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/types"
)

// TestQueueOrderingWithConcurrentAcks tests FIFO ordering with concurrent ACK processing.
// This is a regression test for W-010: out-of-order delivery with concurrent ACKs.
func TestQueueOrderingWithConcurrentAcks(t *testing.T) {
	const N = 500

	q := NewQueue("test-ordering")

	// Enqueue N messages in sequence
	for i := 0; i < N; i++ {
		msg := &types.Message{
			ID:        NewULID(),
			Queue:     "test-ordering",
			Payload:   []byte{byte(i >> 8), byte(i)},
			VisibleAt: time.Now(),
		}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed at %d: %v", i, err)
		}
	}

	ch := q.Dequeue(30 * time.Second)
	received := make([]int, 0, N)
	var mu sync.Mutex
	var acked atomic.Int32

	// Consumer goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range ch {
			seq := int(msg.Payload[0])<<8 | int(msg.Payload[1])
			mu.Lock()
			received = append(received, seq)
			mu.Unlock()
			// ACK concurrently to simulate high-throughput scenario
			go func(id string) {
				q.Ack(id)
				acked.Add(1)
			}(msg.ID)
		}
	}()

	// Wait for all ACKs
	deadline := time.Now().Add(10 * time.Second)
	for acked.Load() < N && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	q.RemoveConsumer(ch)
	<-done

	if len(received) != N {
		t.Errorf("Expected %d messages, got %d", N, len(received))
	}

	// Count out-of-order messages
	outOfOrder := 0
	for i := 1; i < len(received); i++ {
		if received[i] < received[i-1] {
			outOfOrder++
			if outOfOrder <= 5 {
				t.Logf("Out of order at pos %d: got %d after %d", i, received[i], received[i-1])
			}
		}
	}

	if outOfOrder > 0 {
		t.Errorf("Out of order: %d/%d (%.1f%%)", outOfOrder, N, float64(outOfOrder)/float64(N)*100)
	}
}

// TestQueueOrderingSequentialAcks tests FIFO ordering with sequential ACKs.
// This should always pass - it's the baseline.
func TestQueueOrderingSequentialAcks(t *testing.T) {
	const N = 500

	q := NewQueue("test-ordering-seq")

	// Enqueue N messages in sequence
	for i := 0; i < N; i++ {
		msg := &types.Message{
			ID:        NewULID(),
			Queue:     "test-ordering-seq",
			Payload:   []byte{byte(i >> 8), byte(i)},
			VisibleAt: time.Now(),
		}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed at %d: %v", i, err)
		}
	}

	ch := q.Dequeue(30 * time.Second)
	received := make([]int, 0, N)

	// Consume with sequential ACKs
	for i := 0; i < N; i++ {
		select {
		case msg := <-ch:
			seq := int(msg.Payload[0])<<8 | int(msg.Payload[1])
			received = append(received, seq)
			q.Ack(msg.ID) // Sequential ACK
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i)
		}
	}

	q.RemoveConsumer(ch)

	// Count out-of-order messages
	outOfOrder := 0
	for i := 1; i < len(received); i++ {
		if received[i] < received[i-1] {
			outOfOrder++
			t.Logf("Out of order at pos %d: got %d after %d", i, received[i], received[i-1])
		}
	}

	if outOfOrder > 0 {
		t.Errorf("Out of order: %d/%d (%.1f%%)", outOfOrder, N, float64(outOfOrder)/float64(N)*100)
	}
}
