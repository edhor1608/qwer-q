package broker

import (
	"errors"
	"fmt"
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
	ok, err := b.HandleAck(ackReq, "ack-queue", "")
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	if !ok {
		t.Fatal("ack failed")
	}

	// Verify message deleted from storage
	messages, _ := store.LoadMessages("ack-queue")
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages in storage after ack, got %d", len(messages))
	}
}

func TestBrokerAckKeepsMessageInFlightWhenStorageDeleteFails(t *testing.T) {
	deleteErr := errors.New("delete failed")
	store := &retryDLQFailingStorage{deleteRollbackErr: deleteErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	resp, err := b.HandlePublish(&protocol.PublishRequest{
		Queue:   "ack-delete-fail",
		Payload: []byte("acked but not deleted"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	q := b.GetQueue("ack-delete-fail")
	ch := q.Dequeue(30 * time.Second)
	<-ch

	ok, err := b.HandleAck(&protocol.AckRequest{MessageId: resp.MessageId}, "ack-delete-fail", "")
	if ok {
		t.Fatal("expected ack to fail")
	}
	if !errors.Is(err, deleteErr) {
		t.Fatalf("expected delete error, got %v", err)
	}
	if q.InFlightLen() != 1 {
		t.Fatalf("expected message to stay in-flight, got %d", q.InFlightLen())
	}
}

func TestBrokerDropPolicyNackDoesNotRecoverAfterRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-drop-nack-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveQueue("drop-queue", storage.QueueConfig{FailurePolicy: string(FailurePolicyDrop)}); err != nil {
		t.Fatal(err)
	}

	b := NewBroker(WithStorage(store))
	q := b.GetOrCreateQueue("drop-queue")
	q.SetFailurePolicy(FailurePolicyDrop)

	resp, err := b.HandlePublish(&protocol.PublishRequest{
		Queue:   "drop-queue",
		Payload: []byte("drop me"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	ch := q.Dequeue(30 * time.Second)
	<-ch
	ok, err := b.HandleNack(&protocol.NackRequest{MessageId: resp.MessageId, Requeue: false}, "drop-queue", "")
	if err != nil {
		t.Fatalf("nack failed: %v", err)
	}
	if !ok {
		t.Fatal("nack failed")
	}
	b.Close()

	store2, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	b2 := NewBroker(WithStorage(store2))
	defer b2.Close()
	if err := b2.LoadFromStorage(); err != nil {
		t.Fatalf("load from storage: %v", err)
	}

	if recovered := b2.GetQueue("drop-queue"); recovered != nil && recovered.Len() != 0 {
		t.Fatalf("expected dropped message not to recover, got %d messages", recovered.Len())
	}
}

func TestBrokerDropPolicyNackKeepsMessageInFlightWhenStorageDeleteFails(t *testing.T) {
	deleteErr := errors.New("delete failed")
	store := &retryDLQFailingStorage{deleteRollbackErr: deleteErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	resp, err := b.HandlePublish(&protocol.PublishRequest{
		Queue:   "drop-delete-fail",
		Payload: []byte("drop me"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	q := b.GetQueue("drop-delete-fail")
	q.SetFailurePolicy(FailurePolicyDrop)
	ch := q.Dequeue(30 * time.Second)
	<-ch

	ok, err := b.HandleNack(&protocol.NackRequest{MessageId: resp.MessageId, Requeue: false}, "drop-delete-fail", "")
	if ok {
		t.Fatal("expected nack to fail")
	}
	if !errors.Is(err, deleteErr) {
		t.Fatalf("expected delete error, got %v", err)
	}
	if q.InFlightLen() != 1 {
		t.Fatalf("expected message to stay in-flight, got %d", q.InFlightLen())
	}
}

func TestBrokerPublishFailsWhenQueueMetadataSaveFails(t *testing.T) {
	saveQueueErr := errors.New("save queue failed")
	store := &retryDLQFailingStorage{saveQueueErr: saveQueueErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	_, err := b.HandlePublish(&protocol.PublishRequest{
		Queue:   "metadata-fail",
		Payload: []byte("data"),
	})
	if !errors.Is(err, saveQueueErr) {
		t.Fatalf("expected queue metadata error, got %v", err)
	}
	if q := b.GetQueue("metadata-fail"); q != nil {
		t.Fatal("expected queue not to exist after metadata save failure")
	}
}

func TestBrokerLoadFromStorageRestoresQueueConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-queue-config-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveQueue("configured", storage.QueueConfig{
		MaxSize:       17,
		MaxRetries:    3,
		FailurePolicy: string(FailurePolicyDrop),
	}); err != nil {
		t.Fatal(err)
	}

	b := NewBroker(WithStorage(store))
	defer b.Close()
	if err := b.LoadFromStorage(); err != nil {
		t.Fatalf("load from storage: %v", err)
	}

	q := b.GetQueue("configured")
	if q == nil {
		t.Fatal("queue not restored")
	}
	if q.MaxSize() != 17 {
		t.Fatalf("expected max size 17, got %d", q.MaxSize())
	}
	if q.MaxRetries() != 3 {
		t.Fatalf("expected max retries 3, got %d", q.MaxRetries())
	}
	if q.FailurePolicy() != FailurePolicyDrop {
		t.Fatalf("expected failure policy drop, got %s", q.FailurePolicy())
	}
}

func TestBrokerNackRequeuePersistsAttemptAcrossRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-retry-attempt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveQueue("retry-attempt", storage.QueueConfig{}); err != nil {
		t.Fatal(err)
	}

	b := NewBroker(WithStorage(store))
	q := b.GetOrCreateQueue("retry-attempt")
	resp, err := b.HandlePublish(&protocol.PublishRequest{
		Queue:   "retry-attempt",
		Payload: []byte("retry me"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	ch := q.Dequeue(30 * time.Second)
	first := <-ch
	if first.Attempt != 1 {
		t.Fatalf("expected first delivery attempt 1, got %d", first.Attempt)
	}
	q.RemoveConsumer(ch)
	ok, err := b.HandleNack(&protocol.NackRequest{MessageId: resp.MessageId, Requeue: true}, "retry-attempt", "")
	if err != nil {
		t.Fatalf("nack failed: %v", err)
	}
	if !ok {
		t.Fatal("nack failed")
	}
	b.Close()

	store2, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	b2 := NewBroker(WithStorage(store2))
	defer b2.Close()
	if err := b2.LoadFromStorage(); err != nil {
		t.Fatalf("load from storage: %v", err)
	}

	recovered := b2.GetQueue("retry-attempt")
	if recovered == nil {
		t.Fatal("queue not recovered")
	}
	next := <-recovered.Dequeue(30 * time.Second)
	if next.Attempt != 2 {
		t.Fatalf("expected retry attempt to survive restart and deliver attempt 2, got %d", next.Attempt)
	}
}

func TestBrokerRetryDLQDoesNotEnqueueWhenStorageSaveFails(t *testing.T) {
	saveErr := errors.New("save failed")
	store := &retryDLQFailingStorage{saveErr: saveErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	dlq := b.GetOrCreateQueue(DLQName("retry-fail"))
	msg := &Message{ID: "msg-1", Queue: DLQName("retry-fail"), Payload: []byte("poison")}
	if err := dlq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue dlq message: %v", err)
	}

	retried, err := b.RetryDLQ("retry-fail")
	if !errors.Is(err, saveErr) {
		t.Fatalf("expected save error, got %v", err)
	}
	if retried != 0 {
		t.Fatalf("expected 0 retried messages, got %d", retried)
	}
	if q := b.GetQueue("retry-fail"); q != nil && q.Len() != 0 {
		t.Fatalf("expected original queue to stay empty, got %d messages", q.Len())
	}
	if dlq.Len() != 1 {
		t.Fatalf("expected dlq message to stay retryable, got %d messages", dlq.Len())
	}
}

func TestBrokerPublishDoesNotDeliverWhenStorageSaveFails(t *testing.T) {
	saveErr := errors.New("save failed")
	store := &retryDLQFailingStorage{saveErr: saveErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	q := b.GetOrCreateQueue("publish-fail")
	ch := q.Dequeue(30 * time.Second)

	_, err := b.HandlePublish(&protocol.PublishRequest{
		Queue:   "publish-fail",
		Payload: []byte("not durable"),
	})
	if !errors.Is(err, saveErr) {
		t.Fatalf("expected save error, got %v", err)
	}

	select {
	case msg := <-ch:
		t.Fatalf("expected no delivery after storage failure, got %s", msg.ID)
	case <-time.After(50 * time.Millisecond):
	}
	if q.Len() != 0 {
		t.Fatalf("expected queue to stay empty, got %d messages", q.Len())
	}
	if q.InFlightLen() != 0 {
		t.Fatalf("expected no in-flight messages, got %d", q.InFlightLen())
	}
}

func TestBrokerPublishRollsBackStorageWhenEnqueueFails(t *testing.T) {
	store := &retryDLQFailingStorage{}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	q := b.GetOrCreateQueue("publish-full")
	q.SetMaxSize(1)
	if err := q.Enqueue(&Message{ID: "existing", Queue: "publish-full", Payload: []byte("full")}); err != nil {
		t.Fatalf("enqueue existing message: %v", err)
	}

	_, err := b.HandlePublish(&protocol.PublishRequest{
		MessageId: proto.String("msg-1"),
		Queue:     "publish-full",
		Payload:   []byte("overflow"),
	})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected queue full error, got %v", err)
	}
	if _, ok := store.saved["publish-full/msg-1"]; ok {
		t.Fatal("expected failed publish storage write to be rolled back")
	}
	if q.Len() != 1 {
		t.Fatalf("expected queue to keep only existing message, got %d messages", q.Len())
	}
}

func TestBrokerPublishReturnsRollbackErrorWhenEnqueueRollbackFails(t *testing.T) {
	rollbackErr := errors.New("rollback failed")
	store := &retryDLQFailingStorage{deleteRollbackErr: rollbackErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	q := b.GetOrCreateQueue("publish-rollback-fail")
	q.SetMaxSize(1)
	if err := q.Enqueue(&Message{ID: "existing", Queue: "publish-rollback-fail", Payload: []byte("full")}); err != nil {
		t.Fatalf("enqueue existing message: %v", err)
	}

	_, err := b.HandlePublish(&protocol.PublishRequest{
		MessageId: proto.String("msg-1"),
		Queue:     "publish-rollback-fail",
		Payload:   []byte("overflow"),
	})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected queue full error, got %v", err)
	}
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("expected rollback error, got %v", err)
	}
}

func TestBrokerRetryDLQDoesNotEnqueueWhenStorageDeleteFails(t *testing.T) {
	deleteErr := errors.New("delete failed")
	store := &retryDLQFailingStorage{deleteDLQErr: deleteErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	dlq := b.GetOrCreateQueue(DLQName("delete-fail"))
	msg := &Message{ID: "msg-1", Queue: DLQName("delete-fail"), Payload: []byte("poison")}
	if err := dlq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue dlq message: %v", err)
	}

	retried, err := b.RetryDLQ("delete-fail")
	if !errors.Is(err, deleteErr) {
		t.Fatalf("expected delete error, got %v", err)
	}
	if retried != 0 {
		t.Fatalf("expected 0 retried messages, got %d", retried)
	}
	if q := b.GetQueue("delete-fail"); q != nil && q.Len() != 0 {
		t.Fatalf("expected original queue to stay empty, got %d messages", q.Len())
	}
	if dlq.Len() != 1 {
		t.Fatalf("expected dlq message to stay retryable, got %d messages", dlq.Len())
	}
	if _, ok := store.saved["delete-fail/msg-1"]; ok {
		t.Fatal("expected saved original message to be rolled back")
	}
}

func TestBrokerRetryDLQReturnsRollbackError(t *testing.T) {
	deleteErr := errors.New("delete failed")
	rollbackErr := errors.New("rollback failed")
	store := &retryDLQFailingStorage{deleteDLQErr: deleteErr, deleteRollbackErr: rollbackErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	dlq := b.GetOrCreateQueue(DLQName("rollback-fail"))
	msg := &Message{ID: "msg-1", Queue: DLQName("rollback-fail"), Payload: []byte("poison")}
	if err := dlq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue dlq message: %v", err)
	}

	retried, err := b.RetryDLQ("rollback-fail")
	if !errors.Is(err, deleteErr) {
		t.Fatalf("expected delete error, got %v", err)
	}
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("expected combined delete and rollback error, got %v", err)
	}
	if retried != 0 {
		t.Fatalf("expected 0 retried messages, got %d", retried)
	}
}

func TestBrokerRetryDLQPartialFailureDoesNotDuplicateOnRetry(t *testing.T) {
	deleteErr := errors.New("delete failed")
	store := &retryDLQFailingStorage{deleteDLQFailAt: 2, deleteDLQErr: deleteErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	dlq := b.GetOrCreateQueue(DLQName("partial-retry"))
	for i := 1; i <= 3; i++ {
		msg := &Message{ID: fmt.Sprintf("msg-%d", i), Queue: DLQName("partial-retry"), Payload: []byte("poison")}
		if err := dlq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue dlq message %d: %v", i, err)
		}
	}

	retried, err := b.RetryDLQ("partial-retry")
	if !errors.Is(err, deleteErr) {
		t.Fatalf("expected delete error, got %v", err)
	}
	if retried != 1 {
		t.Fatalf("expected 1 retried message, got %d", retried)
	}
	q := b.GetQueue("partial-retry")
	if q == nil || q.Len() != 1 {
		t.Fatalf("expected original queue to contain 1 message, got %v", q)
	}
	if dlq.Len() != 2 {
		t.Fatalf("expected dlq to retain 2 messages, got %d", dlq.Len())
	}

	retried, err = b.RetryDLQ("partial-retry")
	if err != nil {
		t.Fatalf("second retry failed: %v", err)
	}
	if retried != 2 {
		t.Fatalf("expected 2 retried messages, got %d", retried)
	}
	if q.Len() != 3 {
		t.Fatalf("expected original queue to contain 3 unique messages, got %d", q.Len())
	}
	if dlq.Len() != 0 {
		t.Fatalf("expected empty dlq, got %d messages", dlq.Len())
	}
}

func TestBrokerRetryDLQRollsBackStorageWhenEnqueueFails(t *testing.T) {
	store := &retryDLQFailingStorage{}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	q := b.GetOrCreateQueue("enqueue-fail")
	q.SetMaxSize(1)
	if err := q.Enqueue(&Message{ID: "existing", Queue: "enqueue-fail", Payload: []byte("full")}); err != nil {
		t.Fatalf("enqueue existing message: %v", err)
	}
	dlq := b.GetOrCreateQueue(DLQName("enqueue-fail"))
	msg := &Message{ID: "msg-1", Queue: DLQName("enqueue-fail"), Payload: []byte("poison")}
	if err := dlq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue dlq message: %v", err)
	}

	retried, err := b.RetryDLQ("enqueue-fail")
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected queue full error, got %v", err)
	}
	if retried != 0 {
		t.Fatalf("expected 0 retried messages, got %d", retried)
	}
	if q.Len() != 1 {
		t.Fatalf("expected original queue to keep only existing message, got %d messages", q.Len())
	}
	if dlq.Len() != 1 {
		t.Fatalf("expected dlq message to stay retryable, got %d messages", dlq.Len())
	}
	if _, ok := store.saved["enqueue-fail/msg-1"]; ok {
		t.Fatal("expected original queue storage write to be rolled back")
	}
	if _, ok := store.saved[DLQName("enqueue-fail")+"/msg-1"]; !ok {
		t.Fatal("expected dlq storage entry to be restored")
	}
}

func TestBrokerPurgeQueueDoesNotPurgeRuntimeWhenStorageFails(t *testing.T) {
	deleteErr := errors.New("delete queue failed")
	store := &retryDLQFailingStorage{deleteQueueErr: deleteErr}
	b := NewBroker(WithStorage(store))
	defer b.Close()

	q := b.GetOrCreateQueue("purge-fail")
	if err := q.Enqueue(&Message{ID: "msg-1", Queue: "purge-fail", Payload: []byte("keep")}); err != nil {
		t.Fatalf("enqueue message: %v", err)
	}

	count, err := b.PurgeQueue("purge-fail")
	if !errors.Is(err, deleteErr) {
		t.Fatalf("expected delete queue error, got %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 purged messages, got %d", count)
	}
	if q.Len() != 1 {
		t.Fatalf("expected runtime message to stay queued, got %d messages", q.Len())
	}
}

type retryDLQFailingStorage struct {
	saveErr           error
	saveQueueErr      error
	deleteDLQErr      error
	deleteDLQFailAt   int
	deleteDLQCalls    int
	deleteRollbackErr error
	deleteQueueErr    error
	saved             map[string]*storage.Message
}

func (s *retryDLQFailingStorage) SaveMessage(msg *storage.Message) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.saved == nil {
		s.saved = make(map[string]*storage.Message)
	}
	msgCopy := *msg
	s.saved[msg.Queue+"/"+msg.ID] = &msgCopy
	return nil
}

func (s *retryDLQFailingStorage) DeleteMessage(queue, id string) error {
	if IsDLQ(queue) && s.deleteDLQErr != nil {
		s.deleteDLQCalls++
		if s.deleteDLQFailAt > 0 && s.deleteDLQCalls != s.deleteDLQFailAt {
			return nil
		}
		return s.deleteDLQErr
	}
	if !IsDLQ(queue) && s.deleteRollbackErr != nil {
		return s.deleteRollbackErr
	}
	delete(s.saved, queue+"/"+id)
	return nil
}

func (s *retryDLQFailingStorage) DeleteQueueMessages(_ string) error {
	return s.deleteQueueErr
}

func (s *retryDLQFailingStorage) LoadMessages(_ string) ([]*storage.Message, error) {
	return nil, nil
}

func (s *retryDLQFailingStorage) SaveQueue(_ string, _ storage.QueueConfig) error {
	return s.saveQueueErr
}

func (s *retryDLQFailingStorage) LoadQueues() (map[string]storage.QueueConfig, error) {
	return nil, nil
}

func (s *retryDLQFailingStorage) Close() error {
	return nil
}

func TestQueueBackpressure(t *testing.T) {
	q := NewQueue("test")
	q.SetMaxSize(3) // Small limit for testing

	// Fill the queue
	for i := 0; i < 3; i++ {
		msg := &Message{ID: NewULID(), Payload: []byte("msg")}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}

	// Next enqueue should fail
	msg := &Message{ID: NewULID(), Payload: []byte("overflow")}
	if err := q.Enqueue(msg); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	// Verify queue length
	if q.Len() != 3 {
		t.Fatalf("expected length 3, got %d", q.Len())
	}
}

func TestQueueBackpressureUnlimited(t *testing.T) {
	q := NewQueue("test")
	q.SetMaxSize(0) // Unlimited

	// Should be able to enqueue many messages
	for i := 0; i < 100; i++ {
		msg := &Message{ID: NewULID(), Payload: []byte("msg")}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}
}

func TestExtendVisibility(t *testing.T) {
	q := NewQueue("test")

	msg := &Message{ID: NewULID(), Payload: []byte("data")}
	q.Enqueue(msg)

	// Consume - sets initial visibility
	ch := q.Dequeue(30 * time.Second)
	<-ch

	// Get initial visibility
	initialVisible := time.Now().Add(30 * time.Second)

	// Extend by 60 seconds
	newVisible := q.ExtendVisibility(msg.ID, 60*time.Second)
	if newVisible.IsZero() {
		t.Fatal("extend visibility failed")
	}

	// New visibility should be roughly 60 seconds from now
	expected := time.Now().Add(60 * time.Second)
	if newVisible.Before(expected.Add(-time.Second)) || newVisible.After(expected.Add(time.Second)) {
		t.Fatalf("expected ~%v, got %v", expected, newVisible)
	}

	// Should be later than initial
	if !newVisible.After(initialVisible.Add(-32 * time.Second)) {
		t.Fatal("new visibility should be later than initial")
	}
}

func TestExtendVisibilityNotFound(t *testing.T) {
	q := NewQueue("test")

	// Try to extend non-existent message
	result := q.ExtendVisibility("non-existent", 60*time.Second)
	if !result.IsZero() {
		t.Fatal("expected zero time for non-existent message")
	}
}

func TestBrokerIdempotency(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	key := "unique-key-123"
	req := &protocol.PublishRequest{
		Queue:          "test-queue",
		Payload:        []byte("data"),
		IdempotencyKey: &key,
	}

	// First publish should succeed
	resp, err := b.HandlePublish(req)
	if err != nil {
		t.Fatalf("first publish failed: %v", err)
	}
	if resp.MessageId == "" {
		t.Fatal("expected message ID")
	}

	// Second publish with same key should fail
	_, err = b.HandlePublish(req)
	if err == nil {
		t.Fatal("expected duplicate error")
	}
	if _, ok := err.(ErrDuplicateMessage); !ok {
		t.Fatalf("expected ErrDuplicateMessage, got %T: %v", err, err)
	}

	// Different key should succeed
	key2 := "different-key"
	req2 := &protocol.PublishRequest{
		Queue:          "test-queue",
		Payload:        []byte("data2"),
		IdempotencyKey: &key2,
	}
	if _, err := b.HandlePublish(req2); err != nil {
		t.Fatalf("different key publish failed: %v", err)
	}

	// No idempotency key should always succeed
	req3 := &protocol.PublishRequest{
		Queue:   "test-queue",
		Payload: []byte("data3"),
	}
	if _, err := b.HandlePublish(req3); err != nil {
		t.Fatalf("no-key publish 1 failed: %v", err)
	}
	if _, err := b.HandlePublish(req3); err != nil {
		t.Fatalf("no-key publish 2 failed: %v", err)
	}
}
