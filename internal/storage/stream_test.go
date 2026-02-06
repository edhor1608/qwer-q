package storage

import (
	"os"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/types"
)

func newTestStorage(t *testing.T) (*BadgerStorage, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "stream-test-*")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewBadgerStorage(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return s, func() {
		s.Close()
		os.RemoveAll(dir)
	}
}

func TestStreamStorage_SaveAndLoad(t *testing.T) {
	s, cleanup := newTestStorage(t)
	defer cleanup()

	now := time.Now().Truncate(time.Millisecond)
	msg := &StreamMessage{
		Message: types.Message{
			ID:          "stream-msg-1",
			Queue:       "events",
			Payload:     []byte("event data"),
			Headers:     map[string]string{"type": "click"},
			PublishedAt: now,
		},
		Sequence: 1,
	}

	if err := s.SaveStreamMessage("events", 1, msg); err != nil {
		t.Fatalf("SaveStreamMessage failed: %v", err)
	}

	msgs, err := s.LoadStreamMessages("events", 1, 10)
	if err != nil {
		t.Fatalf("LoadStreamMessages failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ID != "stream-msg-1" {
		t.Fatalf("expected ID stream-msg-1, got %s", msgs[0].ID)
	}
	if msgs[0].Sequence != 1 {
		t.Fatalf("expected sequence 1, got %d", msgs[0].Sequence)
	}
	if string(msgs[0].Payload) != "event data" {
		t.Fatalf("payload mismatch")
	}
}

func TestStreamStorage_SequenceOrdering(t *testing.T) {
	s, cleanup := newTestStorage(t)
	defer cleanup()

	now := time.Now()
	for i := uint64(1); i <= 10; i++ {
		msg := &StreamMessage{
			Message: types.Message{
				ID:          types.NewULID(),
				Queue:       "ordered",
				Payload:     []byte("data"),
				PublishedAt: now.Add(time.Duration(i) * time.Second),
			},
			Sequence: i,
		}
		if err := s.SaveStreamMessage("ordered", i, msg); err != nil {
			t.Fatal(err)
		}
	}

	// Load from offset 5
	msgs, err := s.LoadStreamMessages("ordered", 5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 6 { // seq 5,6,7,8,9,10
		t.Fatalf("expected 6 messages from offset 5, got %d", len(msgs))
	}
	if msgs[0].Sequence != 5 {
		t.Fatalf("expected first message sequence 5, got %d", msgs[0].Sequence)
	}
	if msgs[5].Sequence != 10 {
		t.Fatalf("expected last message sequence 10, got %d", msgs[5].Sequence)
	}

	// Load with limit
	msgs, err = s.LoadStreamMessages("ordered", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages with limit, got %d", len(msgs))
	}
}

func TestStreamStorage_GetSequence(t *testing.T) {
	s, cleanup := newTestStorage(t)
	defer cleanup()

	// Empty queue
	seq, err := s.GetStreamSequence("empty")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 {
		t.Fatalf("expected 0 for empty stream, got %d", seq)
	}

	// Add messages
	for i := uint64(1); i <= 5; i++ {
		msg := &StreamMessage{
			Message: types.Message{
				ID:    types.NewULID(),
				Queue: "seq-test",
			},
			Sequence: i,
		}
		s.SaveStreamMessage("seq-test", i, msg)
	}

	seq, err = s.GetStreamSequence("seq-test")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 5 {
		t.Fatalf("expected sequence 5, got %d", seq)
	}
}

func TestStreamStorage_ConsumerOffsets(t *testing.T) {
	s, cleanup := newTestStorage(t)
	defer cleanup()

	// No offset committed yet
	offset, err := s.LoadConsumerOffset("events", "group-a")
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatalf("expected 0 for uncommitted offset, got %d", offset)
	}

	// Commit offset
	if err := s.SaveConsumerOffset("events", "group-a", 42); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveConsumerOffset("events", "group-b", 100); err != nil {
		t.Fatal(err)
	}

	offset, err = s.LoadConsumerOffset("events", "group-a")
	if err != nil {
		t.Fatal(err)
	}
	if offset != 42 {
		t.Fatalf("expected offset 42, got %d", offset)
	}

	// Load all offsets
	offsets, err := s.LoadAllConsumerOffsets("events")
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(offsets))
	}
	if offsets["group-a"] != 42 || offsets["group-b"] != 100 {
		t.Fatalf("offset mismatch: %v", offsets)
	}
}

func TestStreamStorage_DeleteBefore(t *testing.T) {
	s, cleanup := newTestStorage(t)
	defer cleanup()

	for i := uint64(1); i <= 10; i++ {
		msg := &StreamMessage{
			Message: types.Message{
				ID:    types.NewULID(),
				Queue: "retention",
			},
			Sequence: i,
		}
		s.SaveStreamMessage("retention", i, msg)
	}

	// Delete messages before sequence 6
	if err := s.DeleteStreamMessagesBefore("retention", 6); err != nil {
		t.Fatal(err)
	}

	msgs, err := s.LoadStreamMessages("retention", 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 5 { // seq 6,7,8,9,10
		t.Fatalf("expected 5 messages after retention, got %d", len(msgs))
	}
	if msgs[0].Sequence != 6 {
		t.Fatalf("expected first remaining sequence 6, got %d", msgs[0].Sequence)
	}
}

func TestStreamStorage_GetStats(t *testing.T) {
	s, cleanup := newTestStorage(t)
	defer cleanup()

	for i := uint64(1); i <= 5; i++ {
		msg := &StreamMessage{
			Message: types.Message{
				ID:    types.NewULID(),
				Queue: "stats",
			},
			Sequence: i,
		}
		s.SaveStreamMessage("stats", i, msg)
	}

	oldest, newest, err := s.GetStreamStats("stats")
	if err != nil {
		t.Fatal(err)
	}
	if oldest != 1 {
		t.Fatalf("expected oldest 1, got %d", oldest)
	}
	if newest != 5 {
		t.Fatalf("expected newest 5, got %d", newest)
	}
}

func TestStreamStorage_GetByTimestamp(t *testing.T) {
	s, cleanup := newTestStorage(t)
	defer cleanup()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := uint64(1); i <= 5; i++ {
		msg := &StreamMessage{
			Message: types.Message{
				ID:          types.NewULID(),
				Queue:       "ts-test",
				PublishedAt: baseTime.Add(time.Duration(i) * time.Hour),
			},
			Sequence: i,
		}
		s.SaveStreamMessage("ts-test", i, msg)
	}

	// Find message at or after 3 hours
	targetMs := baseTime.Add(3 * time.Hour).UnixMilli()
	seq, err := s.GetStreamMessageByTimestamp("ts-test", targetMs)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 3 {
		t.Fatalf("expected sequence 3 for timestamp search, got %d", seq)
	}
}

func TestStreamStorage_Persistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "stream-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Phase 1: Write data
	s1, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	msg := &StreamMessage{
		Message: types.Message{
			ID:      "persist-stream-1",
			Queue:   "durable",
			Payload: []byte("persisted"),
		},
		Sequence: 1,
	}
	s1.SaveStreamMessage("durable", 1, msg)
	s1.SaveConsumerOffset("durable", "mygroup", 1)
	s1.Close()

	// Phase 2: Reopen and verify
	s2, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	msgs, err := s2.LoadStreamMessages("durable", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != "persist-stream-1" {
		t.Fatalf("stream message not persisted correctly")
	}

	offset, err := s2.LoadConsumerOffset("durable", "mygroup")
	if err != nil {
		t.Fatal(err)
	}
	if offset != 1 {
		t.Fatalf("consumer offset not persisted, expected 1 got %d", offset)
	}

	seq, err := s2.GetStreamSequence("durable")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("expected sequence 1, got %d", seq)
	}
}
