package broker

import (
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
)

const defaultVisibilityTimeout = 30 * time.Second

// HandlePublish processes a publish request.
func (b *Broker) HandlePublish(req *protocol.PublishRequest) (*protocol.PublishResponse, error) {
	// Check memory pressure before accepting new messages
	if err := b.CheckMemoryPressure(); err != nil {
		return nil, err
	}

	// Check idempotency key
	if b.dedup != nil {
		if err := b.dedup.Check(req.GetIdempotencyKey()); err != nil {
			return nil, err
		}
	}

	msgID := req.GetMessageId()
	if msgID == "" {
		msgID = NewULID()
	}

	msg := &Message{
		ID:          msgID,
		Queue:       req.GetQueue(),
		Payload:     req.GetPayload(),
		Headers:     req.GetHeaders(),
		Attempt:     0,
		PublishedAt: time.Now(),
		VisibleAt:   time.Now(),
	}

	q := b.GetOrCreateQueue(req.GetQueue())
	if err := q.Enqueue(msg); err != nil {
		return nil, err
	}

	if b.storage != nil {
		if err := b.storage.SaveMessage(msg); err != nil {
			return nil, err
		}
	}

	return &protocol.PublishResponse{MessageId: msgID}, nil
}

// HandleConsume processes a consume request and returns a channel for messages.
func (b *Broker) HandleConsume(req *protocol.ConsumeRequest) <-chan *Message {
	timeout := defaultVisibilityTimeout
	if req.GetVisibilityTimeout() > 0 {
		timeout = time.Duration(req.GetVisibilityTimeout()) * time.Second
	}

	q := b.GetOrCreateQueue(req.GetQueue())
	return q.Dequeue(timeout)
}

// HandleAck processes an ack request.
func (b *Broker) HandleAck(req *protocol.AckRequest, queueName string) bool {
	q := b.GetQueue(queueName)
	if q == nil {
		return false
	}
	if !q.Ack(req.GetMessageId()) {
		return false
	}
	if b.storage != nil {
		b.storage.DeleteMessage(req.GetMessageId())
	}
	return true
}

// HandleNack processes a nack request.
func (b *Broker) HandleNack(req *protocol.NackRequest, queueName string) bool {
	q := b.GetQueue(queueName)
	if q == nil {
		return false
	}

	result := q.Nack(req.GetMessageId(), req.GetRequeue())
	if !result.Found {
		return false
	}

	// Move to DLQ if needed
	if result.ToDLQ && result.Message != nil {
		dlqName := DLQName(queueName)
		dlq := b.GetOrCreateQueue(dlqName)
		result.Message.Queue = dlqName
		result.Message.VisibleAt = time.Now()
		dlq.Enqueue(result.Message) // Ignore error - DLQ should always accept

		if b.storage != nil {
			// Delete from original queue storage
			b.storage.DeleteMessage(result.Message.ID)
			// Save to DLQ storage
			b.storage.SaveMessage(result.Message)
		}
	}

	return true
}

// HandleExtendVisibility processes a visibility timeout extension request.
func (b *Broker) HandleExtendVisibility(req *protocol.ExtendVisibilityRequest, queueName string) (time.Time, bool) {
	q := b.GetQueue(queueName)
	if q == nil {
		return time.Time{}, false
	}
	extension := time.Duration(req.GetExtensionSeconds()) * time.Second
	newVisibleAt := q.ExtendVisibility(req.GetMessageId(), extension)
	if newVisibleAt.IsZero() {
		return time.Time{}, false
	}
	return newVisibleAt, true
}

// MessageToProto converts an internal Message to protocol Message.
func MessageToProto(msg *Message) *protocol.Message {
	return &protocol.Message{
		MessageId:   msg.ID,
		Queue:       msg.Queue,
		Payload:     msg.Payload,
		Headers:     msg.Headers,
		Attempt:     msg.Attempt,
		PublishedAt: msg.PublishedAt.UnixMilli(),
	}
}

// EncodeError creates an error response frame.
func EncodeError(code uint32, message string) []byte {
	resp := &protocol.ErrorResponse{Code: code, Message: message}
	payload, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpError, payload)
}
