package broker

import (
	"sync"
	"time"
)

// Consumer represents a message consumer channel.
type Consumer struct {
	Ch                chan *Message
	VisibilityTimeout time.Duration
}

// Queue is a thread-safe message queue.
type Queue struct {
	mu        sync.Mutex
	name      string
	messages  []*Message
	inFlight  map[string]*Message
	consumers []*Consumer
	nextIdx   int // round-robin index
}

// NewQueue creates a new queue with the given name.
func NewQueue(name string) *Queue {
	return &Queue{
		name:     name,
		inFlight: make(map[string]*Message),
	}
}

// Name returns the queue name.
func (q *Queue) Name() string {
	return q.name
}

// Enqueue adds a message to the queue.
func (q *Queue) Enqueue(msg *Message) {
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
	for i := 0; i < len(q.messages); i++ {
		msg := q.messages[i]
		if msg.VisibleAt.After(now) {
			continue
		}

		// Find a consumer with available channel capacity (round-robin)
		for j := 0; j < len(q.consumers); j++ {
			idx := (q.nextIdx + j) % len(q.consumers)
			c := q.consumers[idx]

			select {
			case c.Ch <- msg:
				// Move to in-flight
				q.messages = append(q.messages[:i], q.messages[i+1:]...)
				i--
				msg.Attempt++
				msg.VisibleAt = now.Add(c.VisibilityTimeout)
				q.inFlight[msg.ID] = msg
				q.nextIdx = (idx + 1) % len(q.consumers)
				goto nextMsg
			default:
				// Channel full, try next consumer
			}
		}
	nextMsg:
	}
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
func (q *Queue) RemoveConsumer(ch <-chan *Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, c := range q.consumers {
		if c.Ch == ch {
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
		return true
	}
	return false
}

// Nack negatively acknowledges a message.
// If requeue is true, the message is put back in the queue.
func (q *Queue) Nack(messageID string, requeue bool) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	msg, ok := q.inFlight[messageID]
	if !ok {
		return false
	}
	delete(q.inFlight, messageID)
	if requeue {
		msg.VisibleAt = time.Now()
		q.messages = append(q.messages, msg)
		q.tryDeliver()
	}
	return true
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
