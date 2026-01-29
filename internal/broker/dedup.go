package broker

import (
	"sync"
	"time"
)

// DefaultIdempotencyTTL is the default time-to-live for idempotency keys.
const DefaultIdempotencyTTL = 5 * time.Minute

// ErrDuplicateMessage indicates a message with the same idempotency key was already processed.
type ErrDuplicateMessage struct {
	Key string
}

func (e ErrDuplicateMessage) Error() string {
	return "duplicate message: " + e.Key
}

// IdempotencyTracker tracks idempotency keys with TTL.
type IdempotencyTracker struct {
	mu      sync.Mutex
	keys    map[string]time.Time
	ttl     time.Duration
	done    chan struct{}
	stopped bool
}

// NewIdempotencyTracker creates a new tracker with the given TTL.
func NewIdempotencyTracker(ttl time.Duration) *IdempotencyTracker {
	if ttl <= 0 {
		ttl = DefaultIdempotencyTTL
	}
	t := &IdempotencyTracker{
		keys: make(map[string]time.Time),
		ttl:  ttl,
		done: make(chan struct{}),
	}
	go t.cleaner()
	return t
}

// Check returns an error if the key was already seen within TTL.
// If the key is new, it is recorded and nil is returned.
func (t *IdempotencyTracker) Check(key string) error {
	if key == "" {
		return nil // No idempotency key provided, allow through
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	if expiry, ok := t.keys[key]; ok && now.Before(expiry) {
		return ErrDuplicateMessage{Key: key}
	}

	t.keys[key] = now.Add(t.ttl)
	return nil
}

// cleaner periodically removes expired keys.
func (t *IdempotencyTracker) cleaner() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.cleanup()
		case <-t.done:
			return
		}
	}
}

// cleanup removes expired keys.
func (t *IdempotencyTracker) cleanup() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	for key, expiry := range t.keys {
		if now.After(expiry) {
			delete(t.keys, key)
		}
	}
}

// Close stops the cleaner goroutine.
func (t *IdempotencyTracker) Close() {
	t.mu.Lock()
	if !t.stopped {
		t.stopped = true
		close(t.done)
	}
	t.mu.Unlock()
}

// Len returns the number of tracked keys (for testing).
func (t *IdempotencyTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.keys)
}
