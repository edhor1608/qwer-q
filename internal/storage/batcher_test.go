package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/types"
)

func TestBatcher_BasicWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "batcher-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir, WithBatchInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	msg := &Message{
		ID:          "batch-001",
		Queue:       "test-queue",
		Payload:     []byte("hello batched"),
		PublishedAt: time.Now().Truncate(time.Millisecond),
		VisibleAt:   time.Now().Truncate(time.Millisecond),
	}

	if err := s.SaveMessage(msg); err != nil {
		t.Fatalf("SaveMessage with batcher failed: %v", err)
	}

	messages, err := s.LoadMessages("test-queue")
	if err != nil {
		t.Fatalf("LoadMessages failed: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if messages[0].ID != "batch-001" {
		t.Errorf("ID mismatch: got %s", messages[0].ID)
	}
}

func TestBatcher_ConcurrentWrites(t *testing.T) {
	dir, err := os.MkdirTemp("", "batcher-concurrent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir, WithBatchInterval(5*time.Millisecond), WithBatchMaxSize(50))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const numWriters = 10
	const msgsPerWriter = 100
	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for i := 0; i < msgsPerWriter; i++ {
				msg := &Message{
					ID:      fmt.Sprintf("w%d-msg%d", writer, i),
					Queue:   "concurrent-queue",
					Payload: []byte(fmt.Sprintf("data-%d-%d", writer, i)),
				}
				if err := s.SaveMessage(msg); err != nil {
					t.Errorf("writer %d msg %d: %v", writer, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	messages, err := s.LoadMessages("concurrent-queue")
	if err != nil {
		t.Fatalf("LoadMessages failed: %v", err)
	}
	expected := numWriters * msgsPerWriter
	if len(messages) != expected {
		t.Fatalf("expected %d messages, got %d", expected, len(messages))
	}
}

func TestBatcher_FlushOnMaxSize(t *testing.T) {
	dir, err := os.MkdirTemp("", "batcher-maxsize-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Large interval so flush only happens via max size
	s, err := NewBadgerStorage(dir, WithBatchInterval(10*time.Second), WithBatchMaxSize(5))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Write exactly maxSize messages -- should flush immediately
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := &Message{
				ID:      fmt.Sprintf("maxsize-%d", i),
				Queue:   "maxsize-queue",
				Payload: []byte("data"),
			}
			if err := s.SaveMessage(msg); err != nil {
				t.Errorf("msg %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	messages, err := s.LoadMessages("maxsize-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(messages))
	}
}

func TestBatcher_FlushOnClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "batcher-close-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir, WithBatchInterval(10*time.Second), WithBatchMaxSize(1000))
	if err != nil {
		t.Fatal(err)
	}

	// Write a few messages (won't trigger interval or max size flush)
	for i := 0; i < 3; i++ {
		go func(i int) {
			s.SaveMessage(&Message{
				ID:      fmt.Sprintf("close-%d", i),
				Queue:   "close-queue",
				Payload: []byte("data"),
			})
		}(i)
	}

	// Brief pause for writes to enter the batcher
	time.Sleep(10 * time.Millisecond)

	// Close should flush remaining
	s.Close()

	// Reopen and verify
	s2, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer s2.Close()

	messages, err := s2.LoadMessages("close-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 3 {
		t.Fatalf("expected 3 messages after close+reopen, got %d", len(messages))
	}
}

func TestBatcher_DisabledByDefault(t *testing.T) {
	dir, err := os.MkdirTemp("", "batcher-disabled-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// No batch interval = no batcher
	s, err := NewBadgerStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.batcher != nil {
		t.Fatal("batcher should be nil when batch interval is 0")
	}

	// Should still work via direct writes
	msg := &Message{ID: "direct-001", Queue: "q", Payload: []byte("x")}
	if err := s.SaveMessage(msg); err != nil {
		t.Fatalf("direct SaveMessage failed: %v", err)
	}
}

// BenchmarkSaveMessage_NoBatch measures parallel write throughput without batching.
func BenchmarkSaveMessage_NoBatch(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-nobatch-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir, WithSyncInterval(100*time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	payload := make([]byte, 256)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := &Message{
				ID:      types.NewULID(),
				Queue:   "bench-queue",
				Payload: payload,
			}
			if err := s.SaveMessage(msg); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSaveMessage_Batched measures batched write throughput.
func BenchmarkSaveMessage_Batched(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-batched-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewBadgerStorage(dir,
		WithSyncInterval(100*time.Millisecond),
		WithBatchInterval(5*time.Millisecond),
		WithBatchMaxSize(100),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	payload := make([]byte, 256)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := &Message{
				ID:      types.NewULID(),
				Queue:   "bench-queue",
				Payload: payload,
			}
			if err := s.SaveMessage(msg); err != nil {
				b.Fatal(err)
			}
		}
	})
}
