package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/jonas/qwer-q/internal/types"
)

func TestBadgerStorage_SaveAndLoadMessages(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	msg := &Message{
		ID:          "msg-001",
		Queue:       "test-queue",
		Payload:     []byte("hello"),
		Headers:     map[string]string{"x-foo": "bar"},
		Attempt:     1,
		PublishedAt: time.Now().Truncate(time.Millisecond),
		VisibleAt:   time.Now().Truncate(time.Millisecond),
	}

	if err := s.SaveMessage(msg); err != nil {
		t.Fatalf("SaveMessage failed: %v", err)
	}

	messages, err := s.LoadMessages("test-queue")
	if err != nil {
		t.Fatalf("LoadMessages failed: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	loaded := messages[0]
	if loaded.ID != msg.ID {
		t.Errorf("ID mismatch: got %s, want %s", loaded.ID, msg.ID)
	}
	if loaded.Queue != msg.Queue {
		t.Errorf("Queue mismatch: got %s, want %s", loaded.Queue, msg.Queue)
	}
	if string(loaded.Payload) != string(msg.Payload) {
		t.Errorf("Payload mismatch: got %s, want %s", loaded.Payload, msg.Payload)
	}
	if loaded.Headers["x-foo"] != "bar" {
		t.Errorf("Headers mismatch: got %v, want %v", loaded.Headers, msg.Headers)
	}
	if loaded.Attempt != msg.Attempt {
		t.Errorf("Attempt mismatch: got %d, want %d", loaded.Attempt, msg.Attempt)
	}

	s.Close()
}

func TestBadgerStorage_DeleteMessage(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	msg := &Message{
		ID:      "msg-delete",
		Queue:   "test-queue",
		Payload: []byte("to-delete"),
	}

	if err := s.SaveMessage(msg); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteMessage(msg.Queue, msg.ID); err != nil {
		t.Fatalf("DeleteMessage failed: %v", err)
	}

	messages, err := s.LoadMessages("test-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages after delete, got %d", len(messages))
	}
}

func TestBadgerStorage_DeleteQueueMessages(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 1200; i++ {
		if err := s.SaveMessage(&Message{
			ID:      fmt.Sprintf("msg-%04d", i),
			Queue:   "delete-queue",
			Payload: []byte("delete"),
		}); err != nil {
			t.Fatalf("SaveMessage failed: %v", err)
		}
	}
	if err := s.SaveMessage(&Message{ID: "keep", Queue: "delete-queue-other", Payload: []byte("keep")}); err != nil {
		t.Fatalf("SaveMessage failed: %v", err)
	}

	if err := s.DeleteQueueMessages("delete-queue"); err != nil {
		t.Fatalf("DeleteQueueMessages failed: %v", err)
	}
	messages, err := s.LoadMessages("delete-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages after queue delete, got %d", len(messages))
	}
	kept, err := s.LoadMessages("delete-queue-other")
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 1 {
		t.Fatalf("expected unrelated queue message to remain, got %d", len(kept))
	}
}

func TestBadgerStorage_LoadMessages_LegacyJSON(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	msg := &Message{
		ID:          "legacy-json",
		Queue:       "legacy-queue",
		Payload:     []byte("legacy"),
		PublishedAt: time.Now().Truncate(time.Millisecond),
		VisibleAt:   time.Now().Truncate(time.Millisecond),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(msgKey(msg.Queue, msg.ID), data)
	}); err != nil {
		t.Fatal(err)
	}

	messages, err := s.LoadMessages(msg.Queue)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if messages[0].ID != msg.ID {
		t.Fatalf("legacy message ID mismatch: got %s want %s", messages[0].ID, msg.ID)
	}
}

func TestBadgerStorage_QueueConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	cfg := QueueConfig{
		MaxSize:       1000,
		MaxRetries:    5,
		FailurePolicy: "dlq",
	}

	if err := s.SaveQueue("my-queue", cfg); err != nil {
		t.Fatalf("SaveQueue failed: %v", err)
	}

	queues, err := s.LoadQueues()
	if err != nil {
		t.Fatalf("LoadQueues failed: %v", err)
	}

	if len(queues) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(queues))
	}

	loaded := queues["my-queue"]
	if loaded.MaxSize != cfg.MaxSize {
		t.Errorf("MaxSize mismatch: got %d, want %d", loaded.MaxSize, cfg.MaxSize)
	}
	if loaded.MaxRetries != cfg.MaxRetries {
		t.Errorf("MaxRetries mismatch: got %d, want %d", loaded.MaxRetries, cfg.MaxRetries)
	}
	if loaded.FailurePolicy != cfg.FailurePolicy {
		t.Errorf("FailurePolicy mismatch: got %s, want %s", loaded.FailurePolicy, cfg.FailurePolicy)
	}

	s.Close()
}

func TestBadgerStorage_CrashRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Phase 1: Save data then close
	s1, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	msg1 := &Message{
		ID:      "persist-1",
		Queue:   "recovery-queue",
		Payload: []byte("data1"),
	}
	msg2 := &Message{
		ID:      "persist-2",
		Queue:   "recovery-queue",
		Payload: []byte("data2"),
	}
	msg3 := &Message{
		ID:      "persist-3",
		Queue:   "other-queue",
		Payload: []byte("data3"),
	}

	s1.SaveMessage(msg1)
	s1.SaveMessage(msg2)
	s1.SaveMessage(msg3)

	cfg := QueueConfig{MaxSize: 500, MaxRetries: 3, FailurePolicy: "drop"}
	s1.SaveQueue("recovery-queue", cfg)

	s1.Close()

	// Phase 2: Reopen and verify
	s2, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}
	defer s2.Close()

	messages, err := s2.LoadMessages("recovery-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages in recovery-queue, got %d", len(messages))
	}

	otherMessages, err := s2.LoadMessages("other-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(otherMessages) != 1 {
		t.Fatalf("expected 1 message in other-queue, got %d", len(otherMessages))
	}

	queues, err := s2.LoadQueues()
	if err != nil {
		t.Fatal(err)
	}
	if queues["recovery-queue"].MaxSize != 500 {
		t.Errorf("queue config not persisted correctly")
	}
}

func TestBadgerStorage_MultipleQueues(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Save messages to different queues
	for i := 0; i < 5; i++ {
		s.SaveMessage(&Message{
			ID:      types.NewULID(),
			Queue:   "queue-a",
			Payload: []byte("a"),
		})
	}
	for i := 0; i < 3; i++ {
		s.SaveMessage(&Message{
			ID:      types.NewULID(),
			Queue:   "queue-b",
			Payload: []byte("b"),
		})
	}

	msgsA, _ := s.LoadMessages("queue-a")
	msgsB, _ := s.LoadMessages("queue-b")
	msgsC, _ := s.LoadMessages("queue-c") // non-existent

	if len(msgsA) != 5 {
		t.Errorf("expected 5 messages in queue-a, got %d", len(msgsA))
	}
	if len(msgsB) != 3 {
		t.Errorf("expected 3 messages in queue-b, got %d", len(msgsB))
	}
	if len(msgsC) != 0 {
		t.Errorf("expected 0 messages in queue-c, got %d", len(msgsC))
	}
}
