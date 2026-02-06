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
	mu            sync.RWMutex
	queues        map[string]*Queue
	streamQueues  map[string]*StreamQueue
	done          chan struct{}
	storage       storage.Storage
	dedup         *IdempotencyTracker
	memoryLimit   uint64        // Maximum memory in bytes (0 = unlimited)
	cachedAlloc   atomic.Uint64 // Cached memory allocation (updated by background goroutine)
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
		queues:       make(map[string]*Queue),
		streamQueues: make(map[string]*StreamQueue),
		done:         make(chan struct{}),
		dedup:        NewIdempotencyTracker(DefaultIdempotencyTTL),
		memoryLimit:  DefaultMemoryLimit,
	}
	for _, opt := range opts {
		opt(b)
	}
	go b.reaper()
	go b.retentionLoop()
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

// retentionLoop periodically runs retention cleanup on stream queues.
func (b *Broker) retentionLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.mu.RLock()
			for _, sq := range b.streamQueues {
				sq.RunRetention()
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
	for name, cfg := range queues {
		if cfg.Mode == storage.QueueModeStream {
			// Stream queues restore their sequence from storage in NewStreamQueue
			sq := b.GetOrCreateStreamQueue(name)
			if cfg.RetentionMaxAge > 0 || cfg.RetentionMaxBytes > 0 {
				sq.SetRetention(
					time.Duration(cfg.RetentionMaxAge)*time.Second,
					cfg.RetentionMaxBytes,
				)
			}
			continue
		}

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

// GetOrCreateStreamQueue returns the stream queue with the given name, creating it if needed.
func (b *Broker) GetOrCreateStreamQueue(name string) *StreamQueue {
	b.mu.RLock()
	sq, ok := b.streamQueues[name]
	b.mu.RUnlock()
	if ok {
		return sq
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if sq, ok = b.streamQueues[name]; ok {
		return sq
	}

	var streamStore storage.StreamStorage
	if b.storage != nil {
		if ss, ok := b.storage.(storage.StreamStorage); ok {
			streamStore = ss
		}
	}

	sq = NewStreamQueue(name, streamStore)
	b.streamQueues[name] = sq

	// Persist queue config
	if b.storage != nil {
		b.storage.SaveQueue(name, storage.QueueConfig{Mode: storage.QueueModeStream})
	}

	return sq
}

// GetStreamQueue returns the stream queue with the given name, or nil if not found.
func (b *Broker) GetStreamQueue(name string) *StreamQueue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.streamQueues[name]
}

// IsStreamQueue returns true if the named queue is in stream mode.
func (b *Broker) IsStreamQueue(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.streamQueues[name]
	return ok
}

// GetQueue returns the queue with the given name, or nil if not found.
func (b *Broker) GetQueue(name string) *Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.queues[name]
}

// ListQueues returns all queue names sorted alphabetically (includes stream queues).
func (b *Broker) ListQueues() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.queues)+len(b.streamQueues))
	for name := range b.queues {
		names = append(names, name)
	}
	for name := range b.streamQueues {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
