package broker

import (
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
)

func TestDLQName(t *testing.T) {
	tests := []struct {
		queue    string
		expected string
	}{
		{"orders", "orders.dlq"},
		{"events", "events.dlq"},
	}
	for _, tt := range tests {
		if got := DLQName(tt.queue); got != tt.expected {
			t.Errorf("DLQName(%q) = %q, want %q", tt.queue, got, tt.expected)
		}
	}
}

func TestIsDLQ(t *testing.T) {
	tests := []struct {
		queue    string
		expected bool
	}{
		{"orders.dlq", true},
		{"orders", false},
		{".dlq", false}, // Edge case: just the suffix
		{"dlq", false},
	}
	for _, tt := range tests {
		if got := IsDLQ(tt.queue); got != tt.expected {
			t.Errorf("IsDLQ(%q) = %v, want %v", tt.queue, got, tt.expected)
		}
	}
}

func TestQueueNackToDLQ(t *testing.T) {
	q := NewQueue("test")
	q.SetMaxRetries(3)
	q.SetFailurePolicy(FailurePolicyDLQ)

	msg := &Message{ID: NewULID(), Payload: []byte("data")}
	q.Enqueue(msg)

	// Consume the message
	ch := q.Dequeue(30 * time.Second)
	<-ch

	// Simulate retries by nacking with requeue multiple times
	for i := uint32(1); i <= 3; i++ {
		// Get message back
		if i < 3 {
			result := q.Nack(msg.ID, true)
			if !result.Found {
				t.Fatalf("nack %d: message not found", i)
			}
			if result.ToDLQ {
				t.Fatalf("nack %d: unexpected DLQ before max retries", i)
			}
			// Re-consume
			<-ch
		}
	}

	// This nack should trigger DLQ (attempt 3 >= maxRetries 3)
	result := q.Nack(msg.ID, true)
	if !result.Found {
		t.Fatal("final nack: message not found")
	}
	if !result.ToDLQ {
		t.Fatal("expected message to go to DLQ after max retries")
	}
}

func TestQueueNackDrop(t *testing.T) {
	q := NewQueue("test")
	q.SetMaxRetries(2)
	q.SetFailurePolicy(FailurePolicyDrop)

	msg := &Message{ID: NewULID(), Payload: []byte("data")}
	q.Enqueue(msg)

	ch := q.Dequeue(30 * time.Second)
	<-ch

	// First nack - requeue
	result := q.Nack(msg.ID, true)
	if result.ToDLQ {
		t.Fatal("unexpected DLQ on first nack")
	}

	<-ch

	// Second nack - should drop (not DLQ)
	result = q.Nack(msg.ID, true)
	if !result.Found {
		t.Fatal("message not found")
	}
	if result.ToDLQ {
		t.Fatal("expected drop, not DLQ")
	}
}

func TestQueueNackInfinite(t *testing.T) {
	q := NewQueue("test")
	q.SetMaxRetries(2)
	q.SetFailurePolicy(FailurePolicyInfinite)

	msg := &Message{ID: NewULID(), Payload: []byte("data")}
	q.Enqueue(msg)

	ch := q.Dequeue(30 * time.Second)

	// Nack many times - should always requeue
	for i := 0; i < 10; i++ {
		<-ch
		result := q.Nack(msg.ID, true)
		if !result.Found {
			t.Fatalf("nack %d: message not found", i)
		}
		if result.ToDLQ {
			t.Fatalf("nack %d: unexpected DLQ with infinite policy", i)
		}
	}
}

func TestBrokerDLQIntegration(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Set up queue with low retry limit
	q := b.GetOrCreateQueue("test-queue")
	q.SetMaxRetries(2)
	q.SetFailurePolicy(FailurePolicyDLQ)

	// Publish a message
	req := &protocol.PublishRequest{
		Queue:   "test-queue",
		Payload: []byte("important data"),
	}
	resp, err := b.HandlePublish(req)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	msgID := resp.MessageId

	// Consume and nack until DLQ
	ch := q.Dequeue(30 * time.Second)
	<-ch

	// First nack - requeue
	b.HandleNack(&protocol.NackRequest{MessageId: msgID, Requeue: true}, "test-queue")
	<-ch

	// Second nack - should go to DLQ
	b.HandleNack(&protocol.NackRequest{MessageId: msgID, Requeue: true}, "test-queue")

	// Verify DLQ exists and has the message
	dlq := b.GetQueue("test-queue.dlq")
	if dlq == nil {
		t.Fatal("DLQ not created")
	}
	if dlq.Len() != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", dlq.Len())
	}

	// Original queue should be empty
	if q.Len() != 0 {
		t.Fatalf("expected 0 messages in original queue, got %d", q.Len())
	}
}

func TestNackWithoutRequeue(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	q := b.GetOrCreateQueue("test-queue")
	q.SetFailurePolicy(FailurePolicyDLQ)

	// Publish a message
	resp, _ := b.HandlePublish(&protocol.PublishRequest{
		Queue:   "test-queue",
		Payload: []byte("data"),
	})

	// Consume
	ch := q.Dequeue(30 * time.Second)
	<-ch

	// Nack without requeue - should go directly to DLQ
	b.HandleNack(&protocol.NackRequest{MessageId: resp.MessageId, Requeue: false}, "test-queue")

	dlq := b.GetQueue("test-queue.dlq")
	if dlq == nil || dlq.Len() != 1 {
		t.Fatal("expected message in DLQ on nack without requeue")
	}
}
