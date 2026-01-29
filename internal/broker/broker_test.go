package broker

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"github.com/jonas/qwer-q/internal/storage"
	"google.golang.org/protobuf/proto"
)

func TestIntegration(t *testing.T) {
	// Start broker and server
	broker := NewBroker()
	defer broker.Close()

	server := NewServer(broker)
	go server.ListenAndServe("127.0.0.1:0")
	defer server.Close()

	// Wait for server to start
	time.Sleep(50 * time.Millisecond)
	addr := server.Addr()
	if addr == nil {
		t.Fatal("server failed to start")
	}

	// Connect client
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Test publish
	pubReq := &protocol.PublishRequest{
		Queue:   "test-queue",
		Payload: []byte("hello world"),
	}
	pubData, _ := proto.Marshal(pubReq)
	conn.Write(protocol.EncodeFrame(protocol.OpPublish, pubData))

	// Read publish ack
	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("failed to read publish ack: %v", err)
	}
	if frame.OpCode != protocol.OpPublishAck {
		t.Fatalf("expected OpPublishAck, got %v", frame.OpCode)
	}

	var pubResp protocol.PublishResponse
	if err := proto.Unmarshal(frame.Payload, &pubResp); err != nil {
		t.Fatalf("failed to unmarshal publish response: %v", err)
	}
	if pubResp.MessageId == "" {
		t.Fatal("expected message ID in response")
	}
	msgID := pubResp.MessageId

	// Start consuming
	consumeReq := &protocol.ConsumeRequest{
		Queue:             "test-queue",
		VisibilityTimeout: 30,
	}
	consumeData, _ := proto.Marshal(consumeReq)
	conn.Write(protocol.EncodeFrame(protocol.OpConsume, consumeData))

	// Read delivered message
	frame, err = protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}
	if frame.OpCode != protocol.OpMessage {
		t.Fatalf("expected OpMessage, got %v", frame.OpCode)
	}

	var msg protocol.Message
	if err := proto.Unmarshal(frame.Payload, &msg); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}
	if msg.MessageId != msgID {
		t.Fatalf("expected message ID %s, got %s", msgID, msg.MessageId)
	}
	if string(msg.Payload) != "hello world" {
		t.Fatalf("expected payload 'hello world', got %s", string(msg.Payload))
	}
	if msg.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", msg.Attempt)
	}

	// Ack the message
	ackReq := &protocol.AckRequest{MessageId: msgID}
	ackData, _ := proto.Marshal(ackReq)
	conn.Write(protocol.EncodeFrame(protocol.OpAck, ackData))

	// Give time for ack to process
	time.Sleep(50 * time.Millisecond)

	// Verify queue is empty
	q := broker.GetQueue("test-queue")
	if q == nil {
		t.Fatal("queue not found")
	}
	if q.Len() != 0 {
		t.Fatalf("expected queue length 0, got %d", q.Len())
	}
	if q.InFlightLen() != 0 {
		t.Fatalf("expected in-flight length 0, got %d", q.InFlightLen())
	}
}

func TestNackRequeue(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	server := NewServer(broker)
	go server.ListenAndServe("127.0.0.1:0")
	defer server.Close()

	time.Sleep(50 * time.Millisecond)
	addr := server.Addr()

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Publish
	pubReq := &protocol.PublishRequest{Queue: "nack-queue", Payload: []byte("test")}
	pubData, _ := proto.Marshal(pubReq)
	conn.Write(protocol.EncodeFrame(protocol.OpPublish, pubData))

	frame, _ := protocol.DecodeFrame(conn)
	var pubResp protocol.PublishResponse
	proto.Unmarshal(frame.Payload, &pubResp)
	msgID := pubResp.MessageId

	// Consume
	consumeReq := &protocol.ConsumeRequest{Queue: "nack-queue", VisibilityTimeout: 30}
	consumeData, _ := proto.Marshal(consumeReq)
	conn.Write(protocol.EncodeFrame(protocol.OpConsume, consumeData))

	// Get message
	frame, _ = protocol.DecodeFrame(conn)
	var msg protocol.Message
	proto.Unmarshal(frame.Payload, &msg)

	if msg.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", msg.Attempt)
	}

	// Nack with requeue
	nackReq := &protocol.NackRequest{MessageId: msgID, Requeue: true}
	nackData, _ := proto.Marshal(nackReq)
	conn.Write(protocol.EncodeFrame(protocol.OpNack, nackData))

	// Should get redelivered
	frame, err = protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("failed to read redelivered message: %v", err)
	}

	proto.Unmarshal(frame.Payload, &msg)
	if msg.Attempt != 2 {
		t.Fatalf("expected attempt 2 after requeue, got %d", msg.Attempt)
	}
}

func TestQueueOperations(t *testing.T) {
	q := NewQueue("test")

	msg := &Message{
		ID:      NewULID(),
		Queue:   "test",
		Payload: []byte("data"),
	}
	q.Enqueue(msg)

	if q.Len() != 1 {
		t.Fatalf("expected length 1, got %d", q.Len())
	}

	// Dequeue with consumer
	ch := q.Dequeue(30 * time.Second)

	select {
	case received := <-ch:
		if received.ID != msg.ID {
			t.Fatalf("expected message ID %s, got %s", msg.ID, received.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}

	if q.InFlightLen() != 1 {
		t.Fatalf("expected in-flight 1, got %d", q.InFlightLen())
	}

	// Ack
	if !q.Ack(msg.ID) {
		t.Fatal("ack failed")
	}

	if q.InFlightLen() != 0 {
		t.Fatalf("expected in-flight 0 after ack, got %d", q.InFlightLen())
	}
}

func TestULIDGeneration(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := NewULID()
		if ids[id] {
			t.Fatalf("duplicate ULID: %s", id)
		}
		ids[id] = true
	}
}

func TestBrokerQueues(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// GetOrCreateQueue creates queue
	q1 := b.GetOrCreateQueue("alpha")
	if q1 == nil {
		t.Fatal("expected queue")
	}

	// GetOrCreateQueue returns existing
	q2 := b.GetOrCreateQueue("alpha")
	if q1 != q2 {
		t.Fatal("expected same queue instance")
	}

	// GetQueue returns nil for non-existent
	if b.GetQueue("beta") != nil {
		t.Fatal("expected nil for non-existent queue")
	}

	// ListQueues
	b.GetOrCreateQueue("beta")
	b.GetOrCreateQueue("gamma")
	names := b.ListQueues()
	if len(names) != 3 {
		t.Fatalf("expected 3 queues, got %d", len(names))
	}
	if names[0] != "alpha" || names[1] != "beta" || names[2] != "gamma" {
		t.Fatalf("expected sorted names, got %v", names)
	}
}

func TestBrokerWithStorage(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create storage and broker
	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Save queue config first (required for LoadFromStorage)
	store.SaveQueue("persist-queue", storage.QueueConfig{})

	b := NewBroker(WithStorage(store))

	// Publish a message
	req := &protocol.PublishRequest{
		Queue:   "persist-queue",
		Payload: []byte("persisted data"),
	}
	resp, err := b.HandlePublish(req)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}
	msgID := resp.MessageId

	// Verify message was saved to storage
	messages, err := store.LoadMessages("persist-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message in storage, got %d", len(messages))
	}
	if messages[0].ID != msgID {
		t.Fatalf("message ID mismatch in storage")
	}

	// Close broker (also closes storage)
	b.Close()

	// Reopen storage and create new broker
	store2, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}

	b2 := NewBroker(WithStorage(store2))
	defer b2.Close()

	// Load from storage
	if err := b2.LoadFromStorage(); err != nil {
		t.Fatalf("LoadFromStorage failed: %v", err)
	}

	// Verify queue was restored with message
	q := b2.GetQueue("persist-queue")
	if q == nil {
		t.Fatal("queue not restored")
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 message in queue, got %d", q.Len())
	}
}

func TestBrokerAckDeletesFromStorage(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-ack-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	b := NewBroker(WithStorage(store))
	defer b.Close()

	// Publish
	req := &protocol.PublishRequest{
		Queue:   "ack-queue",
		Payload: []byte("to be acked"),
	}
	resp, _ := b.HandlePublish(req)
	msgID := resp.MessageId

	// Start consumer and get message
	q := b.GetQueue("ack-queue")
	ch := q.Dequeue(30 * time.Second)
	<-ch // receive message

	// Ack
	ackReq := &protocol.AckRequest{MessageId: msgID}
	if !b.HandleAck(ackReq, "ack-queue") {
		t.Fatal("ack failed")
	}

	// Verify message deleted from storage
	messages, _ := store.LoadMessages("ack-queue")
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages in storage after ack, got %d", len(messages))
	}
}
