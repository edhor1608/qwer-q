package test

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/broker"
	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// --- helpers ---

// testBroker starts a broker+server on a random port and returns a cleanup func.
func testBroker(t *testing.T) (*broker.Broker, *broker.Server, string) {
	t.Helper()
	b := broker.NewBroker(broker.WithMemoryLimit(0)) // disable memory checks in tests
	s := broker.NewServer(b)
	go s.ListenAndServe("127.0.0.1:0")
	s.WaitReady()
	addr := s.Addr().String()
	t.Cleanup(func() {
		s.Close()
		b.Close()
	})
	return b, s, addr
}

// dial opens a TCP connection to the broker.
func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// publish sends a PublishRequest and reads back the PublishResponse.
func publish(t *testing.T, conn net.Conn, queue string, payload []byte) string {
	t.Helper()
	return publishWithKey(t, conn, queue, payload, "")
}

// publishWithKey publishes with an optional idempotency key.
func publishWithKey(t *testing.T, conn net.Conn, queue string, payload []byte, idempotencyKey string) string {
	t.Helper()
	req := &protocol.PublishRequest{Queue: queue, Payload: payload}
	if idempotencyKey != "" {
		req.IdempotencyKey = &idempotencyKey
	}
	data, _ := proto.Marshal(req)
	if _, err := conn.Write(protocol.EncodeFrame(protocol.OpPublish, data)); err != nil {
		t.Fatalf("write publish: %v", err)
	}
	frame := readFrame(t, conn)
	if frame.OpCode == protocol.OpError {
		var errResp protocol.ErrorResponse
		proto.Unmarshal(frame.Payload, &errResp)
		t.Fatalf("publish error: code=%d msg=%s", errResp.Code, errResp.Message)
	}
	if frame.OpCode != protocol.OpPublishAck {
		t.Fatalf("expected OpPublishAck, got %v", frame.OpCode)
	}
	var resp protocol.PublishResponse
	if err := proto.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("unmarshal publish response: %v", err)
	}
	return resp.MessageId
}

// publishExpectError sends a PublishRequest and expects an OpError back.
func publishExpectError(t *testing.T, conn net.Conn, queue string, payload []byte) *protocol.ErrorResponse {
	t.Helper()
	req := &protocol.PublishRequest{Queue: queue, Payload: payload}
	data, _ := proto.Marshal(req)
	conn.Write(protocol.EncodeFrame(protocol.OpPublish, data))
	frame := readFrame(t, conn)
	if frame.OpCode != protocol.OpError {
		t.Fatalf("expected OpError, got %v", frame.OpCode)
	}
	var errResp protocol.ErrorResponse
	proto.Unmarshal(frame.Payload, &errResp)
	return &errResp
}

// publishWithKeyExpectError publishes with an idempotency key and expects error.
func publishWithKeyExpectError(t *testing.T, conn net.Conn, queue string, payload []byte, key string) *protocol.ErrorResponse {
	t.Helper()
	req := &protocol.PublishRequest{Queue: queue, Payload: payload, IdempotencyKey: &key}
	data, _ := proto.Marshal(req)
	conn.Write(protocol.EncodeFrame(protocol.OpPublish, data))
	frame := readFrame(t, conn)
	if frame.OpCode != protocol.OpError {
		t.Fatalf("expected OpError, got %v", frame.OpCode)
	}
	var errResp protocol.ErrorResponse
	proto.Unmarshal(frame.Payload, &errResp)
	return &errResp
}

// subscribe sends a ConsumeRequest. Messages will arrive asynchronously.
func subscribe(t *testing.T, conn net.Conn, queue string, visibilityTimeout uint32) {
	t.Helper()
	req := &protocol.ConsumeRequest{Queue: queue, VisibilityTimeout: visibilityTimeout}
	data, _ := proto.Marshal(req)
	if _, err := conn.Write(protocol.EncodeFrame(protocol.OpConsume, data)); err != nil {
		t.Fatalf("write consume: %v", err)
	}
}

// receiveMessage reads the next OpMessage from the connection.
func receiveMessage(t *testing.T, conn net.Conn) *protocol.Message {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	frame := readFrame(t, conn)
	if frame.OpCode != protocol.OpMessage {
		t.Fatalf("expected OpMessage, got %v", frame.OpCode)
	}
	var msg protocol.Message
	if err := proto.Unmarshal(frame.Payload, &msg); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	return &msg
}

// receiveMessageWithTimeout reads the next OpMessage with a custom timeout.
func receiveMessageWithTimeout(t *testing.T, conn net.Conn, timeout time.Duration) (*protocol.Message, bool) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})
	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		return nil, false // timeout or error
	}
	if frame.OpCode != protocol.OpMessage {
		t.Fatalf("expected OpMessage, got %v", frame.OpCode)
	}
	var msg protocol.Message
	if err := proto.Unmarshal(frame.Payload, &msg); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	return &msg, true
}

// ack sends an AckRequest for the given message ID.
func ack(t *testing.T, conn net.Conn, messageID string) {
	t.Helper()
	req := &protocol.AckRequest{MessageId: messageID}
	data, _ := proto.Marshal(req)
	if _, err := conn.Write(protocol.EncodeFrame(protocol.OpAck, data)); err != nil {
		t.Fatalf("write ack: %v", err)
	}
}

// nack sends a NackRequest with requeue=true.
func nack(t *testing.T, conn net.Conn, messageID string, requeue bool) {
	t.Helper()
	req := &protocol.NackRequest{MessageId: messageID, Requeue: requeue}
	data, _ := proto.Marshal(req)
	if _, err := conn.Write(protocol.EncodeFrame(protocol.OpNack, data)); err != nil {
		t.Fatalf("write nack: %v", err)
	}
}

// readFrame reads a single frame from a connection.
func readFrame(t *testing.T, conn net.Conn) *protocol.Frame {
	t.Helper()
	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return frame
}

// --- tests ---

func TestFullPublishConsumeCycle(t *testing.T) {
	b, _, addr := testBroker(t)
	conn := dial(t, addr)

	const n = 10
	queue := "cycle-test"
	ids := make([]string, n)

	// Publish N messages
	for i := 0; i < n; i++ {
		ids[i] = publish(t, conn, queue, []byte(fmt.Sprintf("msg-%d", i)))
	}

	// Subscribe
	subscribe(t, conn, queue, 30)

	// Consume and ack all N messages
	received := make(map[string]string) // id -> payload
	for i := 0; i < n; i++ {
		msg := receiveMessage(t, conn)
		received[msg.MessageId] = string(msg.Payload)
		ack(t, conn, msg.MessageId)
	}

	// Verify all messages received
	for i := 0; i < n; i++ {
		payload, ok := received[ids[i]]
		if !ok {
			t.Fatalf("message %s not received", ids[i])
		}
		expected := fmt.Sprintf("msg-%d", i)
		if payload != expected {
			t.Fatalf("expected payload %q, got %q", expected, payload)
		}
	}

	// Give acks time to process, then verify queue empty
	time.Sleep(100 * time.Millisecond)
	q := b.GetQueue(queue)
	if q == nil {
		t.Fatal("queue not found")
	}
	if q.Len() != 0 {
		t.Fatalf("expected queue length 0, got %d", q.Len())
	}
	if q.InFlightLen() != 0 {
		t.Fatalf("expected in-flight 0, got %d", q.InFlightLen())
	}
}

func TestVisibilityTimeout(t *testing.T) {
	_, _, addr := testBroker(t)
	conn := dial(t, addr)

	queue := "visibility-test"
	msgID := publish(t, conn, queue, []byte("timeout-msg"))

	// Subscribe with a very short visibility timeout (1 second)
	subscribe(t, conn, queue, 1)

	// Receive message, do NOT ack
	msg := receiveMessage(t, conn)
	if msg.MessageId != msgID {
		t.Fatalf("expected message %s, got %s", msgID, msg.MessageId)
	}
	if msg.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", msg.Attempt)
	}

	// Wait for visibility timeout + reaper interval (reaper runs every 1s)
	time.Sleep(2500 * time.Millisecond)

	// Message should be redelivered with attempt 2
	msg2 := receiveMessage(t, conn)
	if msg2.MessageId != msgID {
		t.Fatalf("expected redelivered message %s, got %s", msgID, msg2.MessageId)
	}
	if msg2.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", msg2.Attempt)
	}

	// Now ack it
	ack(t, conn, msg2.MessageId)
}

func TestDLQFlow(t *testing.T) {
	b, _, addr := testBroker(t)
	conn := dial(t, addr)

	queue := "dlq-test"

	// Configure queue with low max retries for faster test
	q := b.GetOrCreateQueue(queue)
	q.SetMaxRetries(3)

	msgID := publish(t, conn, queue, []byte("dlq-msg"))

	// Subscribe
	subscribe(t, conn, queue, 30)

	// Nack with requeue until max retries exceeded
	for i := uint32(1); i <= 3; i++ {
		msg := receiveMessage(t, conn)
		if msg.MessageId != msgID {
			t.Fatalf("attempt %d: expected message %s, got %s", i, msgID, msg.MessageId)
		}
		if msg.Attempt != i {
			t.Fatalf("expected attempt %d, got %d", i, msg.Attempt)
		}
		nack(t, conn, msg.MessageId, true) // requeue
	}

	// After max retries, message should go to DLQ.
	// Give some time for the DLQ routing.
	time.Sleep(200 * time.Millisecond)

	// Verify message is in the DLQ
	dlqName := broker.DLQName(queue)
	dlq := b.GetQueue(dlqName)
	if dlq == nil {
		t.Fatal("DLQ not created")
	}
	if dlq.Len() != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", dlq.Len())
	}

	// Original queue should be empty
	if q.Len() != 0 {
		t.Fatalf("expected original queue empty, got %d", q.Len())
	}
}

func TestSchemaValidation(t *testing.T) {
	_, srv, addr := testBroker(t)

	queue := "schema-test"

	// Build a test descriptor for "test.Event" with fields: id (int32), name (string)
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("test"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Event"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:   proto.String("id"),
								Number: proto.Int32(1),
								Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
								Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
							},
							{
								Name:   proto.String("name"),
								Number: proto.Int32(2),
								Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
								Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
							},
						},
					},
				},
				Syntax: proto.String("proto3"),
			},
		},
	}
	descBytes, _ := proto.Marshal(fds)

	// Register schema via TCP
	conn := dial(t, addr)
	regReq := &protocol.SchemaRegisterRequest{
		Queue:       queue,
		Descriptor_: descBytes,
		MessageType: "test.Event",
	}
	regData, _ := proto.Marshal(regReq)
	conn.Write(protocol.EncodeFrame(protocol.OpSchemaRegister, regData))

	frame := readFrame(t, conn)
	if frame.OpCode != protocol.OpSchemaResponse {
		t.Fatalf("expected OpSchemaResponse, got %v", frame.OpCode)
	}
	var regResp protocol.SchemaRegisterResponse
	proto.Unmarshal(frame.Payload, &regResp)
	if regResp.Version != 1 {
		t.Fatalf("expected version 1, got %d", regResp.Version)
	}

	// Verify schema is registered in registry
	_, err := srv.Registry().Get(queue)
	if err != nil {
		t.Fatalf("schema not found in registry: %v", err)
	}

	// Publish valid protobuf payload: Field 1 (id)=42, Field 2 (name)="test"
	validPayload := []byte{0x08, 0x2a, 0x12, 0x04, 't', 'e', 's', 't'}
	publish(t, conn, queue, validPayload)

	// Publish invalid payload (truncated varint)
	invalidPayload := []byte{0x08, 0x80}
	errResp := publishExpectError(t, conn, queue, invalidPayload)
	if errResp.Code != 5 {
		t.Fatalf("expected error code 5 (schema validation), got %d", errResp.Code)
	}
}

func TestRequestReplyCall(t *testing.T) {
	b, _, addr := testBroker(t)

	// Set up a "service" consumer that echoes back the payload.
	serviceQueue := "rpc-service"
	serviceQ := b.GetOrCreateQueue(serviceQueue)
	serviceCh := serviceQ.Dequeue(30 * time.Second)

	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case msg := <-serviceCh:
			replyTo := msg.Headers["reply_to"]
			corrID := msg.Headers["correlation_id"]

			replyQ := b.GetOrCreateQueue(replyTo)
			replyQ.Enqueue(&broker.Message{
				ID:      broker.NewULID(),
				Queue:   replyTo,
				Payload: append([]byte("echo:"), msg.Payload...),
				Headers: map[string]string{"correlation_id": corrID},
			})
			serviceQ.Ack(msg.ID)
		case <-time.After(5 * time.Second):
			t.Error("service timed out")
		}
	}()

	// Client sends CALL via TCP
	conn := dial(t, addr)
	callReq := &protocol.CallRequest{
		Queue:     serviceQueue,
		Payload:   []byte("ping"),
		TimeoutMs: 5000,
	}
	callData, _ := proto.Marshal(callReq)
	conn.Write(protocol.EncodeFrame(protocol.OpCall, callData))

	frame := readFrame(t, conn)
	if frame.OpCode == protocol.OpError {
		var errResp protocol.ErrorResponse
		proto.Unmarshal(frame.Payload, &errResp)
		t.Fatalf("call error: %s", errResp.Message)
	}
	if frame.OpCode != protocol.OpCallResponse {
		t.Fatalf("expected OpCallResponse, got %v", frame.OpCode)
	}

	var callResp protocol.CallResponse
	proto.Unmarshal(frame.Payload, &callResp)
	expected := "echo:ping"
	if string(callResp.Payload) != expected {
		t.Fatalf("expected %q, got %q", expected, string(callResp.Payload))
	}

	<-done
}

func TestBackpressure(t *testing.T) {
	b, _, addr := testBroker(t)
	conn := dial(t, addr)

	queue := "backpressure-test"
	q := b.GetOrCreateQueue(queue)
	q.SetMaxSize(5)

	// Fill queue to max
	for i := 0; i < 5; i++ {
		publish(t, conn, queue, []byte(fmt.Sprintf("msg-%d", i)))
	}

	// Next publish should fail
	errResp := publishExpectError(t, conn, queue, []byte("overflow"))
	if errResp.Code != 3 {
		t.Fatalf("expected error code 3, got %d: %s", errResp.Code, errResp.Message)
	}

	// Queue length should still be 5
	if q.Len() != 5 {
		t.Fatalf("expected queue length 5, got %d", q.Len())
	}
}

func TestMultipleConsumersRoundRobin(t *testing.T) {
	_, _, addr := testBroker(t)

	queue := "roundrobin-test"

	// Create two consumer connections and subscribe
	conn1 := dial(t, addr)
	conn2 := dial(t, addr)
	subscribe(t, conn1, queue, 30)
	subscribe(t, conn2, queue, 30)

	// Give consumers time to register
	time.Sleep(100 * time.Millisecond)

	// Publish 6 messages from a separate connection
	pubConn := dial(t, addr)
	for i := 0; i < 6; i++ {
		publish(t, pubConn, queue, []byte(fmt.Sprintf("rr-%d", i)))
	}

	// Each consumer should get some messages (round-robin distribution).
	// With channel buffer=1, delivery depends on ack timing.
	// Consumer 1: receive and ack
	c1Msgs := 0
	c2Msgs := 0

	// Collect messages from both consumers concurrently
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)

	collect := func(conn net.Conn, count *int) {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			msg, ok := receiveMessageWithTimeout(t, conn, 3*time.Second)
			if !ok {
				return
			}
			ack(t, conn, msg.MessageId)
			mu.Lock()
			*count++
			mu.Unlock()
		}
	}

	go collect(conn1, &c1Msgs)
	go collect(conn2, &c2Msgs)
	wg.Wait()

	total := c1Msgs + c2Msgs
	if total != 6 {
		t.Fatalf("expected 6 total messages, got %d (c1=%d, c2=%d)", total, c1Msgs, c2Msgs)
	}
	// Both consumers should have received at least 1 message
	if c1Msgs == 0 || c2Msgs == 0 {
		t.Fatalf("expected both consumers to get messages, c1=%d c2=%d", c1Msgs, c2Msgs)
	}
}

func TestIdempotency(t *testing.T) {
	b, _, addr := testBroker(t)
	conn := dial(t, addr)

	queue := "idempotency-test"
	key := "unique-key-abc"

	// First publish with idempotency key should succeed
	publishWithKey(t, conn, queue, []byte("data"), key)

	// Second publish with same key should fail
	publishWithKeyExpectError(t, conn, queue, []byte("data"), key)

	// Verify only one message in queue
	q := b.GetQueue(queue)
	if q == nil {
		t.Fatal("queue not found")
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 message, got %d", q.Len())
	}

	// Different key should succeed
	publishWithKey(t, conn, queue, []byte("data2"), "different-key")
	if q.Len() != 2 {
		t.Fatalf("expected 2 messages, got %d", q.Len())
	}
}

func TestConcurrentStress(t *testing.T) {
	b, _, addr := testBroker(t)

	queue := "stress-test"
	const goroutines = 10
	const msgsPerGoroutine = 100
	const total = goroutines * msgsPerGoroutine

	// Publish concurrently from 10 goroutines
	var publishWg sync.WaitGroup
	var publishErrors atomic.Int64
	for g := 0; g < goroutines; g++ {
		publishWg.Add(1)
		go func(id int) {
			defer publishWg.Done()
			conn := dial(t, addr)
			for i := 0; i < msgsPerGoroutine; i++ {
				payload := []byte(fmt.Sprintf("g%d-m%d", id, i))
				req := &protocol.PublishRequest{Queue: queue, Payload: payload}
				data, _ := proto.Marshal(req)
				if _, err := conn.Write(protocol.EncodeFrame(protocol.OpPublish, data)); err != nil {
					publishErrors.Add(1)
					return
				}
				// Read ack
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				frame, err := protocol.DecodeFrame(conn)
				if err != nil {
					publishErrors.Add(1)
					return
				}
				conn.SetReadDeadline(time.Time{})
				if frame.OpCode != protocol.OpPublishAck {
					publishErrors.Add(1)
				}
			}
		}(g)
	}
	publishWg.Wait()

	if errs := publishErrors.Load(); errs > 0 {
		t.Fatalf("had %d publish errors", errs)
	}

	// Verify all messages are in the queue
	q := b.GetQueue(queue)
	if q == nil {
		t.Fatal("queue not found")
	}
	if q.Len() != total {
		t.Fatalf("expected %d messages, got %d", total, q.Len())
	}

	// Consume all messages with multiple consumers
	var consumed atomic.Int64
	var consumeWg sync.WaitGroup
	for c := 0; c < 5; c++ {
		consumeWg.Add(1)
		go func() {
			defer consumeWg.Done()
			conn := dial(t, addr)
			subscribe(t, conn, queue, 30)
			for {
				msg, ok := receiveMessageWithTimeout(t, conn, 2*time.Second)
				if !ok {
					return
				}
				ack(t, conn, msg.MessageId)
				consumed.Add(1)
			}
		}()
	}
	consumeWg.Wait()

	if consumed.Load() != total {
		t.Fatalf("expected %d consumed, got %d", total, consumed.Load())
	}
}

func TestConnectionLifecycle(t *testing.T) {
	b, _, addr := testBroker(t)

	queue := "lifecycle-test"

	// Use separate connections for publishing and consuming to avoid frame interleaving
	pubConn := dial(t, addr)

	// First consumer connection: subscribe then disconnect abruptly
	conn1 := dial(t, addr)
	subscribe(t, conn1, queue, 30)
	time.Sleep(50 * time.Millisecond) // let consumer register

	// Publish a message via the publish connection
	publish(t, pubConn, queue, []byte("before-disconnect"))

	// Consumer receives the message
	msg := receiveMessage(t, conn1)
	if msg.MessageId == "" {
		t.Fatal("expected message")
	}

	// Disconnect consumer abruptly (close without ack)
	conn1.Close()

	// Give time for connection cleanup
	time.Sleep(300 * time.Millisecond)

	// Reconnect with a new consumer connection and subscribe
	conn2 := dial(t, addr)
	subscribe(t, conn2, queue, 30)
	time.Sleep(50 * time.Millisecond) // let consumer register

	// Publish a new message
	publish(t, pubConn, queue, []byte("after-reconnect"))

	// The new consumer should receive the new message
	msg2 := receiveMessage(t, conn2)
	if string(msg2.Payload) != "after-reconnect" {
		t.Fatalf("expected 'after-reconnect', got %q", string(msg2.Payload))
	}
	ack(t, conn2, msg2.MessageId)

	// Give ack time to process
	time.Sleep(100 * time.Millisecond)

	// Verify the queue state is sane
	q := b.GetQueue(queue)
	if q == nil {
		t.Fatal("queue not found")
	}
	// Should have 0 ready messages and 1 in-flight (the unacked message from conn1, still within 30s visibility)
	if q.Len() != 0 {
		t.Fatalf("expected 0 ready messages, got %d", q.Len())
	}
	if q.InFlightLen() != 1 {
		t.Fatalf("expected 1 in-flight (unacked from conn1), got %d", q.InFlightLen())
	}
}

func TestNackWithoutRequeue(t *testing.T) {
	b, _, addr := testBroker(t)
	conn := dial(t, addr)

	queue := "nack-norequeue-test"
	publish(t, conn, queue, []byte("reject-me"))

	subscribe(t, conn, queue, 30)
	msg := receiveMessage(t, conn)

	// Nack without requeue - should go to DLQ immediately
	nack(t, conn, msg.MessageId, false)

	time.Sleep(200 * time.Millisecond)

	// Original queue should be empty
	q := b.GetQueue(queue)
	if q.Len() != 0 || q.InFlightLen() != 0 {
		t.Fatalf("expected empty queue, got len=%d inflight=%d", q.Len(), q.InFlightLen())
	}

	// DLQ should have the message
	dlq := b.GetQueue(broker.DLQName(queue))
	if dlq == nil {
		t.Fatal("DLQ not created")
	}
	if dlq.Len() != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", dlq.Len())
	}
}

func TestQueueList(t *testing.T) {
	_, _, addr := testBroker(t)
	conn := dial(t, addr)

	// Create some queues by publishing
	publish(t, conn, "alpha", []byte("a"))
	publish(t, conn, "beta", []byte("b"))

	// Request queue list
	conn.Write(protocol.EncodeFrame(protocol.OpQueueList, nil))
	frame := readFrame(t, conn)
	if frame.OpCode != protocol.OpQueueListResp {
		t.Fatalf("expected OpQueueListResp, got %v", frame.OpCode)
	}

	var resp protocol.QueueListResponse
	proto.Unmarshal(frame.Payload, &resp)
	if len(resp.Queues) < 2 {
		t.Fatalf("expected at least 2 queues, got %d", len(resp.Queues))
	}

	names := make(map[string]bool)
	for _, q := range resp.Queues {
		names[q.Name] = true
	}
	if !names["alpha"] || !names["beta"] {
		t.Fatalf("expected alpha and beta in queue list, got %v", names)
	}
}
