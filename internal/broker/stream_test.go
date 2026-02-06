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

func newTestStreamBroker(t *testing.T) (*Broker, *storage.BadgerStorage, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "stream-broker-*")
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	b := NewBroker(WithStorage(store), WithMemoryLimit(0))
	return b, store, func() {
		b.Close()
		os.RemoveAll(dir)
	}
}

func TestStreamQueue_PublishAndConsume(t *testing.T) {
	b, _, cleanup := newTestStreamBroker(t)
	defer cleanup()

	sq := b.GetOrCreateStreamQueue("events")

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		msg := &Message{
			ID:          NewULID(),
			Queue:       "events",
			Payload:     []byte("event"),
			PublishedAt: time.Now(),
		}
		seq, err := sq.Publish(msg)
		if err != nil {
			t.Fatalf("publish failed: %v", err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("expected sequence %d, got %d", i+1, seq)
		}
	}

	// Subscribe from beginning
	sc := sq.Subscribe("group-a", 1)
	defer func() { sq.RemoveConsumer(sc.Ch) }()

	// Should receive all 5 messages in order
	for i := 1; i <= 5; i++ {
		select {
		case msg := <-sc.Ch:
			if msg.Sequence != uint64(i) {
				t.Fatalf("expected sequence %d, got %d", i, msg.Sequence)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

func TestStreamQueue_ConsumeFromEnd(t *testing.T) {
	b, _, cleanup := newTestStreamBroker(t)
	defer cleanup()

	sq := b.GetOrCreateStreamQueue("events")

	// Publish 3 messages before consumer
	for i := 0; i < 3; i++ {
		msg := &Message{
			ID:          NewULID(),
			Queue:       "events",
			Payload:     []byte("old"),
			PublishedAt: time.Now(),
		}
		sq.Publish(msg)
	}

	// Subscribe from end (offset 0 = tail)
	sc := sq.Subscribe("group-b", 0)
	defer func() { sq.RemoveConsumer(sc.Ch) }()

	// Should NOT receive old messages
	select {
	case <-sc.Ch:
		t.Fatal("should not receive old messages when subscribing from end")
	case <-time.After(100 * time.Millisecond):
		// expected
	}

	// Publish new message
	msg := &Message{
		ID:          NewULID(),
		Queue:       "events",
		Payload:     []byte("new"),
		PublishedAt: time.Now(),
	}
	sq.Publish(msg)

	// Should receive new message
	select {
	case received := <-sc.Ch:
		if received.Sequence != 4 {
			t.Fatalf("expected sequence 4, got %d", received.Sequence)
		}
		if string(received.Payload) != "new" {
			t.Fatalf("expected payload 'new', got '%s'", received.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for new message")
	}
}

func TestStreamQueue_OffsetCommit(t *testing.T) {
	b, _, cleanup := newTestStreamBroker(t)
	defer cleanup()

	sq := b.GetOrCreateStreamQueue("events")

	// Publish messages
	for i := 0; i < 5; i++ {
		msg := &Message{
			ID:          NewULID(),
			Queue:       "events",
			Payload:     []byte("data"),
			PublishedAt: time.Now(),
		}
		sq.Publish(msg)
	}

	// Commit offset
	if err := sq.CommitOffset("group-a", 3); err != nil {
		t.Fatal(err)
	}

	// Read it back
	offset, err := sq.GetCommittedOffset("group-a")
	if err != nil {
		t.Fatal(err)
	}
	if offset != 3 {
		t.Fatalf("expected committed offset 3, got %d", offset)
	}
}

func TestStreamQueue_MultipleConsumerGroups(t *testing.T) {
	b, _, cleanup := newTestStreamBroker(t)
	defer cleanup()

	sq := b.GetOrCreateStreamQueue("events")

	// Publish 3 messages
	for i := 0; i < 3; i++ {
		msg := &Message{
			ID:          NewULID(),
			Queue:       "events",
			Payload:     []byte("data"),
			PublishedAt: time.Now(),
		}
		sq.Publish(msg)
	}

	// Two consumer groups, both from beginning
	scA := sq.Subscribe("group-a", 1)
	scB := sq.Subscribe("group-b", 1)
	defer func() {
		sq.RemoveConsumer(scA.Ch)
		sq.RemoveConsumer(scB.Ch)
	}()

	// Both should receive all 3 messages independently
	for i := 1; i <= 3; i++ {
		select {
		case msg := <-scA.Ch:
			if msg.Sequence != uint64(i) {
				t.Fatalf("group-a: expected seq %d, got %d", i, msg.Sequence)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("group-a: timeout on message %d", i)
		}
	}
	for i := 1; i <= 3; i++ {
		select {
		case msg := <-scB.Ch:
			if msg.Sequence != uint64(i) {
				t.Fatalf("group-b: expected seq %d, got %d", i, msg.Sequence)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("group-b: timeout on message %d", i)
		}
	}
}

func TestStreamQueue_MessageRetained(t *testing.T) {
	b, store, cleanup := newTestStreamBroker(t)
	defer cleanup()

	sq := b.GetOrCreateStreamQueue("events")

	// Publish a message
	msg := &Message{
		ID:          NewULID(),
		Queue:       "events",
		Payload:     []byte("retained"),
		PublishedAt: time.Now(),
	}
	seq, _ := sq.Publish(msg)

	// Consume and "ack" it
	sc := sq.Subscribe("group-a", 1)
	<-sc.Ch
	sq.RemoveConsumer(sc.Ch)

	// Message should still be in storage (not deleted like queue mode)
	msgs, err := store.LoadStreamMessages("events", seq, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected message still in storage after consume, got %d", len(msgs))
	}
}

func TestStreamQueue_SequenceRecoversAfterRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "stream-recover-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Phase 1: Publish messages
	store1, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	b1 := NewBroker(WithStorage(store1), WithMemoryLimit(0))
	sq1 := b1.GetOrCreateStreamQueue("events")

	for i := 0; i < 5; i++ {
		msg := &Message{
			ID:          NewULID(),
			Queue:       "events",
			Payload:     []byte("data"),
			PublishedAt: time.Now(),
		}
		sq1.Publish(msg)
	}
	b1.Close()

	// Phase 2: Reopen and verify sequence continues
	store2, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	b2 := NewBroker(WithStorage(store2), WithMemoryLimit(0))
	defer b2.Close()

	// Must load from storage to restore stream queues
	if err := b2.LoadFromStorage(); err != nil {
		t.Fatal(err)
	}

	sq2 := b2.GetStreamQueue("events")
	if sq2 == nil {
		t.Fatal("stream queue not restored")
	}

	// Next publish should get sequence 6
	msg := &Message{
		ID:          NewULID(),
		Queue:       "events",
		Payload:     []byte("after-restart"),
		PublishedAt: time.Now(),
	}
	seq, err := sq2.Publish(msg)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 {
		t.Fatalf("expected sequence 6 after restart, got %d", seq)
	}
}

func TestStreamQueue_IsStreamQueue(t *testing.T) {
	b, _, cleanup := newTestStreamBroker(t)
	defer cleanup()

	b.GetOrCreateQueue("regular-queue")
	b.GetOrCreateStreamQueue("stream-queue")

	if b.IsStreamQueue("regular-queue") {
		t.Fatal("regular queue should not be stream")
	}
	if !b.IsStreamQueue("stream-queue") {
		t.Fatal("stream queue should be detected")
	}
}

// Integration test: full wire protocol flow with stream mode
func TestStreamIntegration(t *testing.T) {
	dir, err := os.MkdirTemp("", "stream-integration-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	b := NewBroker(WithStorage(store), WithMemoryLimit(0))
	defer b.Close()

	// Pre-create stream queue so publish routes correctly
	b.GetOrCreateStreamQueue("stream-test")

	server := NewServer(b)
	go server.ListenAndServe("127.0.0.1:0")
	defer server.Close()

	time.Sleep(50 * time.Millisecond)
	addr := server.Addr()
	if addr == nil {
		t.Fatal("server failed to start")
	}

	// Connect two clients: one publisher, one consumer
	pubConn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect publisher: %v", err)
	}
	defer pubConn.Close()

	consConn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect consumer: %v", err)
	}
	defer consConn.Close()

	// Publish a message to stream queue
	pubReq := &protocol.PublishRequest{
		Queue:   "stream-test",
		Payload: []byte("stream hello"),
	}
	pubData, _ := proto.Marshal(pubReq)
	pubConn.Write(protocol.EncodeFrame(protocol.OpPublish, pubData))

	// Read publish ack
	frame, err := protocol.DecodeFrame(pubConn)
	if err != nil {
		t.Fatalf("failed to read publish ack: %v", err)
	}
	if frame.OpCode != protocol.OpPublishAck {
		t.Fatalf("expected OpPublishAck, got %v", frame.OpCode)
	}

	// Consumer: seek to beginning
	seekReq := &protocol.SeekRequest{
		Queue:         "stream-test",
		ConsumerGroup: "test-group",
		Position:      protocol.SeekPosition_SEEK_BEGINNING,
	}
	seekData, _ := proto.Marshal(seekReq)
	consConn.Write(protocol.EncodeFrame(protocol.OpSeek, seekData))

	// Read seek ack
	frame, err = protocol.DecodeFrame(consConn)
	if err != nil {
		t.Fatalf("failed to read seek ack: %v", err)
	}
	if frame.OpCode != protocol.OpSeekAck {
		t.Fatalf("expected OpSeekAck, got %v", frame.OpCode)
	}
	var seekResp protocol.SeekResponse
	if err := proto.Unmarshal(frame.Payload, &seekResp); err != nil {
		t.Fatal(err)
	}
	if seekResp.Offset != 1 {
		t.Fatalf("expected seek offset 1, got %d", seekResp.Offset)
	}

	// Read stream message
	frame, err = protocol.DecodeFrame(consConn)
	if err != nil {
		t.Fatalf("failed to read stream message: %v", err)
	}
	if frame.OpCode != protocol.OpStreamMessage {
		t.Fatalf("expected OpStreamMessage, got %v", frame.OpCode)
	}

	var smsg protocol.StreamMessage
	if err := proto.Unmarshal(frame.Payload, &smsg); err != nil {
		t.Fatal(err)
	}
	if string(smsg.Payload) != "stream hello" {
		t.Fatalf("expected payload 'stream hello', got '%s'", smsg.Payload)
	}
	if smsg.Sequence != 1 {
		t.Fatalf("expected sequence 1, got %d", smsg.Sequence)
	}

	// Commit offset
	commitReq := &protocol.CommitOffsetRequest{
		Queue:         "stream-test",
		ConsumerGroup: "test-group",
		Offset:        smsg.Sequence,
	}
	commitData, _ := proto.Marshal(commitReq)
	consConn.Write(protocol.EncodeFrame(protocol.OpCommitOffset, commitData))

	// Read commit ack
	frame, err = protocol.DecodeFrame(consConn)
	if err != nil {
		t.Fatalf("failed to read commit ack: %v", err)
	}
	if frame.OpCode != protocol.OpCommitOffsetAck {
		t.Fatalf("expected OpCommitOffsetAck, got %v", frame.OpCode)
	}

	var commitResp protocol.CommitOffsetResponse
	if err := proto.Unmarshal(frame.Payload, &commitResp); err != nil {
		t.Fatal(err)
	}
	if commitResp.Offset != 1 {
		t.Fatalf("expected committed offset 1, got %d", commitResp.Offset)
	}

	// Verify message still in storage (not deleted like queue mode)
	msgs, _ := store.LoadStreamMessages("stream-test", 1, 10)
	if len(msgs) != 1 {
		t.Fatalf("stream message should be retained after consume, got %d", len(msgs))
	}
}

// Test that regular queue mode still works alongside stream mode
func TestQueueAndStreamCoexist(t *testing.T) {
	b, _, cleanup := newTestStreamBroker(t)
	defer cleanup()

	// Create both types
	q := b.GetOrCreateQueue("regular")
	sq := b.GetOrCreateStreamQueue("stream")

	// Publish to queue
	msg1 := &Message{
		ID:          NewULID(),
		Queue:       "regular",
		Payload:     []byte("queue msg"),
		PublishedAt: time.Now(),
		VisibleAt:   time.Now(),
	}
	q.Enqueue(msg1)

	// Publish to stream
	msg2 := &Message{
		ID:          NewULID(),
		Queue:       "stream",
		Payload:     []byte("stream msg"),
		PublishedAt: time.Now(),
	}
	sq.Publish(msg2)

	// Queue has 1 message
	if q.Len() != 1 {
		t.Fatalf("queue: expected 1 message, got %d", q.Len())
	}

	// Stream has 1 message
	if sq.Len() != 1 {
		t.Fatalf("stream: expected 1 message, got %d", sq.Len())
	}

	// List should show both
	names := b.ListQueues()
	if len(names) != 2 {
		t.Fatalf("expected 2 queues in list, got %d: %v", len(names), names)
	}
}
