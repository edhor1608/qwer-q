package broker

import (
	"sort"
	"sync"
	"time"
)

// Broker manages queues and message routing.
type Broker struct {
	mu     sync.RWMutex
	queues map[string]*Queue
	done   chan struct{}
}

// NewBroker creates a new broker.
func NewBroker() *Broker {
	b := &Broker{
		queues: make(map[string]*Queue),
		done:   make(chan struct{}),
	}
	go b.reaper()
	return b
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
