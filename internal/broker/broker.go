package broker

import (
	"errors"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/internal/storage"
)

// ErrMemoryPressure is returned when memory usage exceeds the limit.
var ErrMemoryPressure = errors.New("memory pressure: server under load, try again later")

// DefaultMemoryLimit is the default memory limit (300MB - leaves headroom in 512MB container).
// BadgerDB uses mmap which doesn't show in Go's MemStats but counts toward container memory.
const DefaultMemoryLimit = 300 * 1024 * 1024

// Broker manages queues and message routing.
type Broker struct {
	mu          sync.RWMutex
	queues      map[string]*Queue
	done        chan struct{}
	storage     storage.Storage
	dedup       *IdempotencyTracker
	memoryLimit uint64        // Maximum memory in bytes (0 = unlimited)
	memCheck    atomic.Uint64 // Counter to throttle memory checks
}

// BrokerOption configures a Broker.
type BrokerOption func(*Broker)

// WithStorage sets the storage backend.
func WithStorage(s storage.Storage) BrokerOption {
	return func(b *Broker) { b.storage = s }
}

// WithMemoryLimit sets the maximum memory usage in bytes.
// When exceeded, publishes will be rejected with ErrMemoryPressure.
// Set to 0 to disable memory checking (not recommended in containers).
func WithMemoryLimit(limit uint64) BrokerOption {
	return func(b *Broker) { b.memoryLimit = limit }
}

// NewBroker creates a new broker.
func NewBroker(opts ...BrokerOption) *Broker {
	b := &Broker{
		queues:      make(map[string]*Queue),
		done:        make(chan struct{}),
		dedup:       NewIdempotencyTracker(DefaultIdempotencyTTL),
		memoryLimit: DefaultMemoryLimit,
	}
	for _, opt := range opts {
		opt(b)
	}
	go b.reaper()
	return b
}

// CheckMemoryPressure returns ErrMemoryPressure if memory usage exceeds limit.
// The check is throttled to avoid expensive MemStats calls on every publish.
func (b *Broker) CheckMemoryPressure() error {
	if b.memoryLimit == 0 {
		return nil
	}

	// Only check every 10 calls to balance overhead vs responsiveness
	count := b.memCheck.Add(1)
	if count%10 != 0 {
		return nil
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Alloc > b.memoryLimit {
		return ErrMemoryPressure
	}
	return nil
}

// reaper periodically checks for expired in-flight messages.
func (b *Broker) reaper() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.mu.RLock()
			for _, q := range b.queues {
				q.RequeueExpired()
			}
			b.mu.RUnlock()
		case <-b.done:
			return
		}
	}
}

// Close stops the broker.
func (b *Broker) Close() {
	close(b.done)
	if b.dedup != nil {
		b.dedup.Close()
	}
	if b.storage != nil {
		b.storage.Close()
	}
}

// Storage returns the storage backend (may be nil).
func (b *Broker) Storage() storage.Storage {
	return b.storage
}

// LoadFromStorage restores messages from storage.
func (b *Broker) LoadFromStorage() error {
	if b.storage == nil {
		return nil
	}
	queues, err := b.storage.LoadQueues()
	if err != nil {
		return err
	}
	for name := range queues {
		storedMsgs, err := b.storage.LoadMessages(name)
		if err != nil {
			return err
		}
		q := b.GetOrCreateQueue(name)
		for _, sm := range storedMsgs {
			msg := &Message{
				ID:          sm.ID,
				Queue:       sm.Queue,
				Payload:     sm.Payload,
				Headers:     sm.Headers,
				Attempt:     sm.Attempt,
				PublishedAt: sm.PublishedAt,
				VisibleAt:   sm.VisibleAt,
			}
			q.EnqueueDirect(msg)
		}
	}
	return nil
}

// GetOrCreateQueue returns the queue with the given name, creating it if needed.
func (b *Broker) GetOrCreateQueue(name string) *Queue {
	b.mu.RLock()
	q, ok := b.queues[name]
	b.mu.RUnlock()
	if ok {
		return q
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// Double-check after acquiring write lock
	if q, ok = b.queues[name]; ok {
		return q
	}
	q = NewQueue(name)
	b.queues[name] = q

	// Persist queue config for recovery
	if b.storage != nil {
		b.storage.SaveQueue(name, storage.QueueConfig{})
	}

	return q
}

// GetQueue returns the queue with the given name, or nil if not found.
func (b *Broker) GetQueue(name string) *Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.queues[name]
}

// ListQueues returns all queue names sorted alphabetically.
func (b *Broker) ListQueues() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.queues))
	for name := range b.queues {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
