package broker

import (
	"sync"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
)

// ReplyQueuePrefix is the prefix for reply queues.
const ReplyQueuePrefix = "_reply."

// DefaultCallTimeout is the default timeout for CALL operations.
const DefaultCallTimeout = 30 * time.Second

// CallManager manages request/reply correlation.
type CallManager struct {
	mu       sync.Mutex
	pending  map[string]chan *Message // correlationID -> reply channel
	broker   *Broker
	replyQ   string // This connection's reply queue name
	consumer <-chan *Message
	done     chan struct{}
}

// NewCallManager creates a call manager for a connection.
func NewCallManager(broker *Broker, connID string) *CallManager {
	replyQ := ReplyQueuePrefix + connID
	cm := &CallManager{
		pending: make(map[string]chan *Message),
		broker:  broker,
		replyQ:  replyQ,
		done:    make(chan struct{}),
	}

	// Create and consume from reply queue
	q := broker.GetOrCreateQueue(replyQ)
	q.SetMaxSize(1000) // Limit reply queue size
	cm.consumer = q.Dequeue(60 * time.Second)

	// Start reply listener
	go cm.listenForReplies()

	return cm
}

// listenForReplies routes incoming replies to waiting callers.
func (cm *CallManager) listenForReplies() {
	for {
		select {
		case msg, ok := <-cm.consumer:
			if !ok {
				return
			}
			cm.handleReply(msg)
		case <-cm.done:
			return
		}
	}
}

// handleReply routes a reply to the waiting caller.
func (cm *CallManager) handleReply(msg *Message) {
	correlationID := msg.Headers["correlation_id"]
	if correlationID == "" {
		return // No correlation ID, ignore
	}

	cm.mu.Lock()
	ch, ok := cm.pending[correlationID]
	if ok {
		delete(cm.pending, correlationID)
	}
	cm.mu.Unlock()

	if ok {
		select {
		case ch <- msg:
		default:
			// Caller already gone
		}
		close(ch)
	}

	// Ack the reply message
	q := cm.broker.GetQueue(cm.replyQ)
	if q != nil {
		q.Ack(msg.ID)
	}
}

// Call performs a request/reply operation.
func (cm *CallManager) Call(req *protocol.CallRequest) (*protocol.CallResponse, error) {
	timeout := DefaultCallTimeout
	if req.GetTimeoutMs() > 0 {
		timeout = time.Duration(req.GetTimeoutMs()) * time.Millisecond
	}

	// Generate correlation ID
	correlationID := NewULID()

	// Set up reply channel
	replyCh := make(chan *Message, 1)
	cm.mu.Lock()
	cm.pending[correlationID] = replyCh
	cm.mu.Unlock()

	// Clean up on return
	defer func() {
		cm.mu.Lock()
		delete(cm.pending, correlationID)
		cm.mu.Unlock()
	}()

	// Build headers with reply info
	headers := make(map[string]string)
	for k, v := range req.GetHeaders() {
		headers[k] = v
	}
	headers["reply_to"] = cm.replyQ
	headers["correlation_id"] = correlationID

	// Publish request to target queue
	pubReq := &protocol.PublishRequest{
		Queue:   req.GetQueue(),
		Payload: req.GetPayload(),
		Headers: headers,
	}

	_, err := cm.broker.HandlePublish(pubReq)
	if err != nil {
		return nil, err
	}

	// Wait for reply
	select {
	case reply := <-replyCh:
		return &protocol.CallResponse{
			Payload: reply.Payload,
			Headers: reply.Headers,
		}, nil
	case <-time.After(timeout):
		return nil, ErrCallTimeoutInstance
	}
}

// Close shuts down the call manager.
func (cm *CallManager) Close() {
	close(cm.done)
	// Clean up pending calls
	cm.mu.Lock()
	for _, ch := range cm.pending {
		close(ch)
	}
	cm.pending = nil
	cm.mu.Unlock()
}

// ReplyQueue returns the reply queue name.
func (cm *CallManager) ReplyQueue() string {
	return cm.replyQ
}

// ErrCallTimeout indicates a CALL operation timed out.
type ErrCallTimeout struct{}

func (e ErrCallTimeout) Error() string {
	return "call timeout"
}

var ErrCallTimeoutInstance = ErrCallTimeout{}
