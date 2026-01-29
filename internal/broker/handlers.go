package broker

import (
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
)

const defaultVisibilityTimeout = 30 * time.Second

// HandlePublish processes a publish request.
func (b *Broker) HandlePublish(req *protocol.PublishRequest) (*protocol.PublishResponse, error) {
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
	q.Enqueue(msg)

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
	return q.Ack(req.GetMessageId())
}

// HandleNack processes a nack request.
func (b *Broker) HandleNack(req *protocol.NackRequest, queueName string) bool {
	q := b.GetQueue(queueName)
	if q == nil {
		return false
	}
	return q.Nack(req.GetMessageId(), req.GetRequeue())
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
