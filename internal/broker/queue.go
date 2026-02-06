package broker

import (
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

// ErrQueueFull is returned when a queue is at capacity.
var ErrQueueFull = errors.New("queue is full")

// DefaultMaxQueueSize is the default maximum number of messages in a queue.
// Set conservatively for 512MB container with ~10KB average message size.
// BadgerDB uses ~3x message size due to mmap overhead.
// Can be increased via queue config for larger containers.
const DefaultMaxQueueSize = 10_000

// Consumer represents a message consumer channel.
type Consumer struct {
	Ch                chan *Message
	VisibilityTimeout time.Duration
}

// Queue is a thread-safe message queue.
type Queue struct {
	mu             sync.Mutex
	name           string
	messages       []*Message
	inFlight       map[string]*Message
	consumers      []*Consumer
	nextIdx        int           // round-robin index
	maxSize        int           // maximum number of messages (0 = unlimited)
	failurePolicy  FailurePolicy // what to do on max retries
	maxRetries     uint32        // max delivery attempts before failure policy kicks in
	keyAssignments map[string]*Consumer // ordering key -> assigned consumer
}

// NewQueue creates a new queue with the given name.
func NewQueue(name string) *Queue {
	return &Queue{
		name:           name,
		inFlight:       make(map[string]*Message),
		maxSize:        DefaultMaxQueueSize,
		failurePolicy:  FailurePolicyDLQ,
		maxRetries:     DefaultMaxRetries,
		keyAssignments: make(map[string]*Consumer),
	}
}

// SetMaxSize sets the maximum queue size. 0 means unlimited.
func (q *Queue) SetMaxSize(max int) {
	q.mu.Lock()
	q.maxSize = max
	q.mu.Unlock()
}

// MaxSize returns the maximum queue size.
func (q *Queue) MaxSize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxSize
}

// SetFailurePolicy sets the failure policy.
func (q *Queue) SetFailurePolicy(policy FailurePolicy) {
	q.mu.Lock()
	q.failurePolicy = policy
	q.mu.Unlock()
}

// SetMaxRetries sets the max delivery attempts.
func (q *Queue) SetMaxRetries(max uint32) {
	q.mu.Lock()
	q.maxRetries = max
	q.mu.Unlock()
}

// FailurePolicy returns the current failure policy.
func (q *Queue) FailurePolicy() FailurePolicy {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.failurePolicy
}

// MaxRetries returns the max delivery attempts.
func (q *Queue) MaxRetries() uint32 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxRetries
}

// Name returns the queue name.
func (q *Queue) Name() string {
	return q.name
}

// Enqueue adds a message to the queue. Returns ErrQueueFull if at capacity.
func (q *Queue) Enqueue(msg *Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.maxSize > 0 && len(q.messages) >= q.maxSize {
		return ErrQueueFull
	}
	q.messages = append(q.messages, msg)
	q.tryDeliver()
	return nil
}

// EnqueueDirect adds a message without triggering storage save (for recovery).
// It bypasses the max size check for recovery scenarios.
func (q *Queue) EnqueueDirect(msg *Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = append(q.messages, msg)
	q.tryDeliver()
}

// tryDeliver attempts to deliver messages to consumers. Must be called with lock held.
func (q *Queue) tryDeliver() {
	if len(q.consumers) == 0 || len(q.messages) == 0 {
		return
	}

	now := time.Now()
	// STRICT FIFO: only deliver the head of the queue, then recurse for more
	for len(q.messages) > 0 {
		msg := q.messages[0]
		if msg.VisibleAt.After(now) {
			return // Head not visible yet, maintain FIFO order
		}

		delivered := false

		if msg.OrderingKey != "" {
			// Ordering key routing: deliver to assigned consumer
			delivered = q.deliverOrdered(msg, now)
		} else {
			// No ordering key: round-robin delivery
			delivered = q.deliverRoundRobin(msg, now)
		}

		if !delivered {
			return // Target consumer full, stop trying
		}

		// Move to in-flight - use proper slice removal to free memory
		copy(q.messages, q.messages[1:])
		q.messages = q.messages[:len(q.messages)-1]
		msg.Attempt++
		q.inFlight[msg.ID] = msg
	}
}

// deliverOrdered delivers a message with an ordering key to its assigned consumer.
// If the key has no assignment, picks a consumer using consistent hashing.
// Returns true if delivered. Must be called with lock held.
func (q *Queue) deliverOrdered(msg *Message, now time.Time) bool {
	c, ok := q.keyAssignments[msg.OrderingKey]
	if !ok {
		// Assign using hash for deterministic initial placement
		h := fnv.New32a()
		h.Write([]byte(msg.OrderingKey))
		idx := int(h.Sum32()) % len(q.consumers)
		c = q.consumers[idx]
		q.keyAssignments[msg.OrderingKey] = c
	}

	select {
	case c.Ch <- msg:
		msg.VisibleAt = now.Add(c.VisibilityTimeout)
		return true
	default:
		return false // Assigned consumer full, must wait to preserve ordering
	}
}

// deliverRoundRobin delivers a message without ordering key using round-robin.
// Returns true if delivered. Must be called with lock held.
func (q *Queue) deliverRoundRobin(msg *Message, now time.Time) bool {
	for j := 0; j < len(q.consumers); j++ {
		idx := (q.nextIdx + j) % len(q.consumers)
		c := q.consumers[idx]

		select {
		case c.Ch <- msg:
			msg.VisibleAt = now.Add(c.VisibilityTimeout)
			q.nextIdx = (idx + 1) % len(q.consumers)
			return true
		default:
			// Channel full, try next consumer
		}
	}
	return false
}

// Dequeue returns a consumer channel for receiving messages.
func (q *Queue) Dequeue(visibilityTimeout time.Duration) <-chan *Message {
	ch := make(chan *Message, 1)
	c := &Consumer{
		Ch:                ch,
		VisibilityTimeout: visibilityTimeout,
	}

	q.mu.Lock()
	q.consumers = append(q.consumers, c)
	q.tryDeliver()
	q.mu.Unlock()

	return ch
}

// RemoveConsumer removes a consumer from the queue.
// Ordering key assignments for this consumer are cleared so they
// get reassigned to remaining consumers on next delivery.
func (q *Queue) RemoveConsumer(ch <-chan *Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, c := range q.consumers {
		if c.Ch == ch {
			// Clear ordering key assignments for this consumer
			for key, assigned := range q.keyAssignments {
				if assigned == c {
					delete(q.keyAssignments, key)
				}
			}
			q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
			close(c.Ch)
			return
		}
	}
}

// Ack acknowledges a message, removing it from in-flight.
func (q *Queue) Ack(messageID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.inFlight[messageID]; ok {
		delete(q.inFlight, messageID)
		q.tryDeliver() // Deliver next message now that channel has space
		return true
	}
	return false
}

// NackResult indicates what happened to a nacked message.
type NackResult struct {
	Found   bool     // Was the message found?
	Message *Message // The message (for DLQ handling)
	ToDLQ   bool     // Should message go to DLQ?
}

// Nack negatively acknowledges a message.
// If requeue is true, the message is put back in the queue (subject to retry limits).
// Returns NackResult indicating what to do with the message.
func (q *Queue) Nack(messageID string, requeue bool) NackResult {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg, ok := q.inFlight[messageID]
	if !ok {
		return NackResult{Found: false}
	}
	delete(q.inFlight, messageID)

	if !requeue {
		// Explicit reject without requeue - send to DLQ if policy allows
		if q.failurePolicy == FailurePolicyDLQ {
			return NackResult{Found: true, Message: msg, ToDLQ: true}
		}
		return NackResult{Found: true, Message: msg, ToDLQ: false}
	}

	// Check if we've exceeded max retries
	if q.failurePolicy != FailurePolicyInfinite && msg.Attempt >= q.maxRetries {
		switch q.failurePolicy {
		case FailurePolicyDLQ:
			return NackResult{Found: true, Message: msg, ToDLQ: true}
		case FailurePolicyDrop:
			return NackResult{Found: true, Message: msg, ToDLQ: false}
		}
	}

	// Requeue the message
	msg.VisibleAt = time.Now()
	q.messages = append(q.messages, msg)
	q.tryDeliver()
	return NackResult{Found: true, Message: msg, ToDLQ: false}
}

// Len returns the number of messages in the queue (not in-flight).
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}

// InFlightLen returns the number of in-flight messages.
func (q *Queue) InFlightLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.inFlight)
}

// RequeueExpired moves expired in-flight messages back to the queue.
func (q *Queue) RequeueExpired() {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for id, msg := range q.inFlight {
		if msg.VisibleAt.Before(now) {
			delete(q.inFlight, id)
			msg.VisibleAt = now
			q.messages = append(q.messages, msg)
		}
	}
	q.tryDeliver()
}

// ExtendVisibility extends the visibility timeout for an in-flight message.
// Returns the new visibility timestamp, or zero time if message not found.
func (q *Queue) ExtendVisibility(messageID string, extension time.Duration) time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()
	msg, ok := q.inFlight[messageID]
	if !ok {
		return time.Time{}
	}
	msg.VisibleAt = time.Now().Add(extension)
	return msg.VisibleAt
}
