package broker

import (
	"testing"
	"time"
)

func TestIdempotencyTrackerBasic(t *testing.T) {
	tracker := NewIdempotencyTracker(time.Minute)
	defer tracker.Close()

	// First check should succeed
	if err := tracker.Check("key1"); err != nil {
		t.Fatalf("first check failed: %v", err)
	}

	// Second check with same key should fail
	err := tracker.Check("key1")
	if err == nil {
		t.Fatal("expected duplicate error")
	}
	if _, ok := err.(ErrDuplicateMessage); !ok {
		t.Fatalf("expected ErrDuplicateMessage, got %T", err)
	}

	// Different key should succeed
	if err := tracker.Check("key2"); err != nil {
		t.Fatalf("different key check failed: %v", err)
	}
}

func TestIdempotencyTrackerEmptyKey(t *testing.T) {
	tracker := NewIdempotencyTracker(time.Minute)
	defer tracker.Close()

	// Empty key should always succeed (no idempotency check)
	if err := tracker.Check(""); err != nil {
		t.Fatalf("empty key check failed: %v", err)
	}
	if err := tracker.Check(""); err != nil {
		t.Fatalf("second empty key check failed: %v", err)
	}
}

func TestIdempotencyTrackerTTL(t *testing.T) {
	// Use short TTL for testing
	tracker := NewIdempotencyTracker(50 * time.Millisecond)
	defer tracker.Close()

	// First check should succeed
	if err := tracker.Check("key1"); err != nil {
		t.Fatalf("first check failed: %v", err)
	}

	// Immediately should fail
	if err := tracker.Check("key1"); err == nil {
		t.Fatal("expected duplicate error immediately")
	}

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// Should succeed after TTL
	if err := tracker.Check("key1"); err != nil {
		t.Fatalf("check after TTL failed: %v", err)
	}
}

func TestIdempotencyTrackerCleanup(t *testing.T) {
	tracker := NewIdempotencyTracker(50 * time.Millisecond)
	defer tracker.Close()

	// Add some keys
	tracker.Check("key1")
	tracker.Check("key2")
	tracker.Check("key3")

	if tracker.Len() != 3 {
		t.Fatalf("expected 3 keys, got %d", tracker.Len())
	}

	// Wait for TTL and manually trigger cleanup
	time.Sleep(60 * time.Millisecond)
	tracker.cleanup()

	if tracker.Len() != 0 {
		t.Fatalf("expected 0 keys after cleanup, got %d", tracker.Len())
	}
}
