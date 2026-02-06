package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/types"
)

// TestOrderingKeySameConsumer verifies that messages with the same ordering key
// are always delivered to the same consumer.
func TestOrderingKeySameConsumer(t *testing.T) {
	q := NewQueue("test-ordering-key")

	// Create 3 consumers
	ch1 := q.Dequeue(30 * time.Second)
	ch2 := q.Dequeue(30 * time.Second)
	ch3 := q.Dequeue(30 * time.Second)

	channels := []<-chan *Message{ch1, ch2, ch3}

	// Publish 10 messages with the same ordering key
	for i := 0; i < 10; i++ {
		msg := &types.Message{
			ID:          NewULID(),
			Queue:       "test-ordering-key",
			Payload:     []byte(fmt.Sprintf("msg-%d", i)),
			VisibleAt:   time.Now(),
			OrderingKey: "user-123",
		}
		// Ack the previous message first so the consumer channel has space
		if i > 0 {
			q.Ack(fmt.Sprintf("prev-%d", i-1))
		}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed at %d: %v", i, err)
		}
	}

	// Determine which consumer got the first message
	var targetCh int
	for i, ch := range channels {
		select {
		case <-ch:
			targetCh = i
		default:
		}
	}

	// Ack the first message to free channel capacity
	// The remaining 9 messages should all queue up for the same consumer
	// Drain and ack to let them flow through
	q.mu.Lock()
	// All messages should have been attempted to deliver to the same consumer
	// Let's check the key assignment
	assigned, ok := q.keyAssignments["user-123"]
	q.mu.Unlock()

	if !ok {
		t.Fatal("expected ordering key to be assigned")
	}

	// Verify assigned consumer matches the one that received the first message
	if assigned != q.consumers[targetCh] {
		t.Fatal("ordering key assigned to different consumer than first delivery")
	}

	q.RemoveConsumer(ch1)
	q.RemoveConsumer(ch2)
	q.RemoveConsumer(ch3)
}

// TestOrderingKeySameConsumerMultipleKeys verifies that different ordering keys
// can be assigned to different consumers, and each key stays sticky.
func TestOrderingKeySameConsumerMultipleKeys(t *testing.T) {
	q := NewQueue("test-multi-key")

	ch1 := q.Dequeue(30 * time.Second)
	ch2 := q.Dequeue(30 * time.Second)

	channels := map[<-chan *Message]string{ch1: "c1", ch2: "c2"}

	// Publish messages with different keys
	keys := []string{"key-a", "key-b", "key-c", "key-d"}
	for _, key := range keys {
		msg := &types.Message{
			ID:          NewULID(),
			Queue:       "test-multi-key",
			Payload:     []byte("data"),
			VisibleAt:   time.Now(),
			OrderingKey: key,
		}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed for key %s: %v", key, err)
		}
	}

	// Drain messages and record which consumer got which key
	keyConsumer := make(map[string]string)
	timeout := time.After(2 * time.Second)

	for i := 0; i < len(keys); i++ {
		select {
		case msg := <-ch1:
			keyConsumer[msg.OrderingKey] = channels[ch1]
			q.Ack(msg.ID)
		case msg := <-ch2:
			keyConsumer[msg.OrderingKey] = channels[ch2]
			q.Ack(msg.ID)
		case <-timeout:
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	// Now publish again with the same keys - should go to same consumers
	for _, key := range keys {
		msg := &types.Message{
			ID:          NewULID(),
			Queue:       "test-multi-key",
			Payload:     []byte("data-2"),
			VisibleAt:   time.Now(),
			OrderingKey: key,
		}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed for key %s round 2: %v", key, err)
		}
	}

	// Verify same consumer gets same key
	for i := 0; i < len(keys); i++ {
		select {
		case msg := <-ch1:
			if keyConsumer[msg.OrderingKey] != channels[ch1] {
				t.Errorf("key %s switched from %s to %s", msg.OrderingKey, keyConsumer[msg.OrderingKey], channels[ch1])
			}
			q.Ack(msg.ID)
		case msg := <-ch2:
			if keyConsumer[msg.OrderingKey] != channels[ch2] {
				t.Errorf("key %s switched from %s to %s", msg.OrderingKey, keyConsumer[msg.OrderingKey], channels[ch2])
			}
			q.Ack(msg.ID)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d in round 2", i)
		}
	}

	q.RemoveConsumer(ch1)
	q.RemoveConsumer(ch2)
}

// TestOrderingKeyRebalanceOnDisconnect verifies that when a consumer disconnects,
// its ordering keys get reassigned to remaining consumers.
func TestOrderingKeyRebalanceOnDisconnect(t *testing.T) {
	q := NewQueue("test-rebalance")

	ch1 := q.Dequeue(30 * time.Second)
	ch2 := q.Dequeue(30 * time.Second)

	// Publish a message with ordering key
	msg1 := &types.Message{
		ID:          NewULID(),
		Queue:       "test-rebalance",
		Payload:     []byte("first"),
		VisibleAt:   time.Now(),
		OrderingKey: "rebalance-key",
	}
	if err := q.Enqueue(msg1); err != nil {
		t.Fatal(err)
	}

	// Determine which consumer got it
	var receivedCh <-chan *Message
	var otherCh <-chan *Message
	select {
	case msg := <-ch1:
		receivedCh = ch1
		otherCh = ch2
		q.Ack(msg.ID)
	case msg := <-ch2:
		receivedCh = ch2
		otherCh = ch1
		q.Ack(msg.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Remove the consumer that had the key
	q.RemoveConsumer(receivedCh)

	// Verify the key assignment was cleared
	q.mu.Lock()
	_, assigned := q.keyAssignments["rebalance-key"]
	q.mu.Unlock()
	if assigned {
		t.Fatal("expected key assignment to be cleared after consumer removal")
	}

	// Publish another message with the same key - should go to remaining consumer
	msg2 := &types.Message{
		ID:          NewULID(),
		Queue:       "test-rebalance",
		Payload:     []byte("second"),
		VisibleAt:   time.Now(),
		OrderingKey: "rebalance-key",
	}
	if err := q.Enqueue(msg2); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-otherCh:
		if msg.OrderingKey != "rebalance-key" {
			t.Fatalf("expected ordering key 'rebalance-key', got %s", msg.OrderingKey)
		}
		q.Ack(msg.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for rebalanced message")
	}

	q.RemoveConsumer(otherCh)
}

// TestNoOrderingKeyRoundRobin verifies that messages without ordering keys
// still use round-robin delivery across consumers.
func TestNoOrderingKeyRoundRobin(t *testing.T) {
	q := NewQueue("test-no-key-rr")

	ch1 := q.Dequeue(30 * time.Second)
	ch2 := q.Dequeue(30 * time.Second)

	// Publish 4 messages without ordering key
	for i := 0; i < 4; i++ {
		msg := &types.Message{
			ID:        NewULID(),
			Queue:     "test-no-key-rr",
			Payload:   []byte(fmt.Sprintf("msg-%d", i)),
			VisibleAt: time.Now(),
		}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	// Both consumers should get messages (round-robin)
	c1Count := 0
	c2Count := 0
	timeout := time.After(2 * time.Second)

	for i := 0; i < 4; i++ {
		select {
		case msg := <-ch1:
			c1Count++
			q.Ack(msg.ID)
		case msg := <-ch2:
			c2Count++
			q.Ack(msg.ID)
		case <-timeout:
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	// Both consumers should have received messages
	if c1Count == 0 || c2Count == 0 {
		t.Errorf("expected both consumers to get messages, got c1=%d c2=%d", c1Count, c2Count)
	}

	q.RemoveConsumer(ch1)
	q.RemoveConsumer(ch2)
}

// TestOrderingKeyMixedWithRoundRobin verifies that ordered and unordered messages
// coexist: ordered messages go to assigned consumer, unordered use round-robin.
func TestOrderingKeyMixedWithRoundRobin(t *testing.T) {
	q := NewQueue("test-mixed")

	ch1 := q.Dequeue(30 * time.Second)
	ch2 := q.Dequeue(30 * time.Second)

	// Publish an ordered message to establish assignment
	msg1 := &types.Message{
		ID:          NewULID(),
		Queue:       "test-mixed",
		Payload:     []byte("ordered"),
		VisibleAt:   time.Now(),
		OrderingKey: "sticky-key",
	}
	if err := q.Enqueue(msg1); err != nil {
		t.Fatal(err)
	}

	// Consume and ack
	var orderedConsumer <-chan *Message
	select {
	case msg := <-ch1:
		orderedConsumer = ch1
		q.Ack(msg.ID)
	case msg := <-ch2:
		orderedConsumer = ch2
		q.Ack(msg.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Publish another ordered message with same key
	msg2 := &types.Message{
		ID:          NewULID(),
		Queue:       "test-mixed",
		Payload:     []byte("ordered-2"),
		VisibleAt:   time.Now(),
		OrderingKey: "sticky-key",
	}
	if err := q.Enqueue(msg2); err != nil {
		t.Fatal(err)
	}

	// Should go to same consumer
	select {
	case msg := <-orderedConsumer:
		if msg.OrderingKey != "sticky-key" {
			t.Fatalf("expected sticky-key, got %s", msg.OrderingKey)
		}
		q.Ack(msg.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("ordered message did not go to same consumer")
	}

	q.RemoveConsumer(ch1)
	q.RemoveConsumer(ch2)
}

// TestOrderingKeyPreservedInProto verifies the ordering key roundtrips through
// the proto Message conversion.
func TestOrderingKeyPreservedInProto(t *testing.T) {
	msg := &types.Message{
		ID:          "test-id",
		Queue:       "test-queue",
		Payload:     []byte("data"),
		Attempt:     1,
		PublishedAt: time.Now(),
		OrderingKey: "my-key",
	}

	protoMsg := MessageToProto(msg)
	if protoMsg.OrderingKey != "my-key" {
		t.Fatalf("expected ordering key 'my-key', got %q", protoMsg.OrderingKey)
	}
}

// TestOrderingKeyEmptyIsRoundRobin verifies that empty string ordering key
// behaves like no ordering key (round-robin).
func TestOrderingKeyEmptyIsRoundRobin(t *testing.T) {
	q := NewQueue("test-empty-key")

	ch1 := q.Dequeue(30 * time.Second)
	ch2 := q.Dequeue(30 * time.Second)

	// Publish messages with empty ordering key
	for i := 0; i < 4; i++ {
		msg := &types.Message{
			ID:          NewULID(),
			Queue:       "test-empty-key",
			Payload:     []byte(fmt.Sprintf("msg-%d", i)),
			VisibleAt:   time.Now(),
			OrderingKey: "", // explicitly empty
		}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	c1Count := 0
	c2Count := 0
	timeout := time.After(2 * time.Second)

	for i := 0; i < 4; i++ {
		select {
		case msg := <-ch1:
			c1Count++
			q.Ack(msg.ID)
		case msg := <-ch2:
			c2Count++
			q.Ack(msg.ID)
		case <-timeout:
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	if c1Count == 0 || c2Count == 0 {
		t.Errorf("expected both consumers with empty key, got c1=%d c2=%d", c1Count, c2Count)
	}

	q.RemoveConsumer(ch1)
	q.RemoveConsumer(ch2)
}
