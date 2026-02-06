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

// DefaultMemoryLimit is the default memory limit (400MB - leaves ~100MB headroom in 512MB container).
// BadgerDB uses ~100-150MB for memtables and caches, plus Go runtime overhead.
// Previous 300MB limit was too aggressive and triggered false memory pressure.
const DefaultMemoryLimit = 400 * 1024 * 1024

// Broker manages queues and message routing.
type Broker struct {
	mu          sync.RWMutex
	queues      map[string]*Queue
	done        chan struct{}
	storage     storage.Storage
	dedup       *IdempotencyTracker
	memoryLimit uint64        // Maximum memory in bytes (0 = unlimited)
	cachedAlloc atomic.Uint64 // Cached memory allocation (updated by background goroutine)
	startedAt   time.Time
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
		startedAt:   time.Now(),
	}
	for _, opt := range opts {
		opt(b)
	}
	go b.reaper()
	if b.memoryLimit != 0 {
		go b.memoryMonitor()
	}
	return b
}

// LargeMessageThreshold is the size above which eager memory checks are performed.
// Messages larger than this bypass the throttle and always check memory.
const LargeMessageThreshold = 64 * 1024 // 64KB

// CheckMemoryPressure returns ErrMemoryPressure if memory usage exceeds limit.
// Uses cached memory stats to avoid expensive ReadMemStats calls in the hot path.
func (b *Broker) CheckMemoryPressure() error {
	if b.memoryLimit == 0 {
		return nil
	}
	if b.cachedAlloc.Load() > b.memoryLimit {
		return ErrMemoryPressure
	}
	return nil
}

// CheckMemoryPressureEager is now equivalent to CheckMemoryPressure since we use
// cached stats. Kept for API compatibility.
func (b *Broker) CheckMemoryPressureEager() error {
	return b.CheckMemoryPressure()
}

// memoryMonitor updates cached memory stats periodically.
// This runs ReadMemStats in a dedicated goroutine to avoid blocking the hot path.
// Update interval of 100ms provides responsive memory pressure detection.
func (b *Broker) memoryMonitor() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Initial read
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	b.cachedAlloc.Store(m.Alloc)

	for {
		select {
		case <-ticker.C:
			runtime.ReadMemStats(&m)
			b.cachedAlloc.Store(m.Alloc)
		case <-b.done:
			return
		}
	}
}

// reaper periodically checks for expired in-flight messages and dead group members.
func (b *Broker) reaper() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.mu.RLock()
			for _, q := range b.queues {
				q.RequeueExpired()
				dead := q.ReapDeadGroupMembers()
				for _, memberID := range dead {
					logger.Info("group member timed out",
						"queue", q.Name(),
						"member", memberID,
					)
				}
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
				OrderingKey: sm.OrderingKey,
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

// StartedAt returns when the broker was created.
func (b *Broker) StartedAt() time.Time {
	return b.startedAt
}

// MemoryAlloc returns the cached memory allocation in bytes.
func (b *Broker) MemoryAlloc() uint64 {
	return b.cachedAlloc.Load()
}

// MemoryLimit returns the configured memory limit in bytes.
func (b *Broker) MemoryLimit() uint64 {
	return b.memoryLimit
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
