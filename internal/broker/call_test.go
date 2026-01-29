package broker

import (
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
)

func TestCallManagerBasic(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	// Create call manager
	cm := NewCallManager(broker, "test-conn")
	defer cm.Close()

	// Set up a "service" that responds to requests
	serviceQ := broker.GetOrCreateQueue("echo-service")
	serviceCh := serviceQ.Dequeue(30 * time.Second)

	// Start service goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case msg := <-serviceCh:
			// Echo back the payload with "reply:" prefix
			replyTo := msg.Headers["reply_to"]
			correlationID := msg.Headers["correlation_id"]

			replyQ := broker.GetOrCreateQueue(replyTo)
			replyMsg := &Message{
				ID:      NewULID(),
				Queue:   replyTo,
				Payload: append([]byte("reply:"), msg.Payload...),
				Headers: map[string]string{
					"correlation_id": correlationID,
				},
			}
			replyQ.Enqueue(replyMsg)

			// Ack the original message
			serviceQ.Ack(msg.ID)
		case <-time.After(5 * time.Second):
			t.Error("service timeout waiting for message")
		}
	}()

	// Make a call
	req := &protocol.CallRequest{
		Queue:     "echo-service",
		Payload:   []byte("hello"),
		TimeoutMs: 5000,
	}

	resp, err := cm.Call(req)
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}

	expected := "reply:hello"
	if string(resp.Payload) != expected {
		t.Fatalf("expected %q, got %q", expected, string(resp.Payload))
	}

	<-done
}

func TestCallManagerTimeout(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	cm := NewCallManager(broker, "test-conn")
	defer cm.Close()

	// Call to non-existent service (no one will reply)
	req := &protocol.CallRequest{
		Queue:     "non-existent-service",
		Payload:   []byte("hello"),
		TimeoutMs: 100, // Short timeout
	}

	_, err := cm.Call(req)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if _, ok := err.(ErrCallTimeout); !ok {
		t.Fatalf("expected ErrCallTimeout, got %T: %v", err, err)
	}
}

func TestCallManagerMultipleCalls(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	cm := NewCallManager(broker, "test-conn")
	defer cm.Close()

	// Set up echo service
	serviceQ := broker.GetOrCreateQueue("multi-echo")
	serviceCh := serviceQ.Dequeue(30 * time.Second)

	// Service handles multiple requests
	serviceDone := make(chan struct{})
	go func() {
		defer close(serviceDone)
		for i := 0; i < 3; i++ {
			select {
			case msg := <-serviceCh:
				replyTo := msg.Headers["reply_to"]
				correlationID := msg.Headers["correlation_id"]

				replyQ := broker.GetOrCreateQueue(replyTo)
				replyQ.Enqueue(&Message{
					ID:      NewULID(),
					Queue:   replyTo,
					Payload: msg.Payload,
					Headers: map[string]string{"correlation_id": correlationID},
				})
				serviceQ.Ack(msg.ID)
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	// Make multiple calls
	for i := 0; i < 3; i++ {
		req := &protocol.CallRequest{
			Queue:     "multi-echo",
			Payload:   []byte("msg"),
			TimeoutMs: 5000,
		}
		resp, err := cm.Call(req)
		if err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
		if string(resp.Payload) != "msg" {
			t.Fatalf("call %d: expected 'msg', got %q", i, string(resp.Payload))
		}
	}

	<-serviceDone
}

func TestReplyQueueNaming(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	cm := NewCallManager(broker, "client-123")
	defer cm.Close()

	expected := ReplyQueuePrefix + "client-123"
	if cm.ReplyQueue() != expected {
		t.Fatalf("expected reply queue %q, got %q", expected, cm.ReplyQueue())
	}
}
