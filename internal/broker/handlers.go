package broker

import (
	"errors"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
)

const defaultVisibilityTimeout = 30 * time.Second

// HandlePublish processes a publish request.
func (b *Broker) HandlePublish(req *protocol.PublishRequest) (*protocol.PublishResponse, error) {
	// Check memory pressure before accepting new messages.
	// Use eager (non-throttled) check for large messages where allocation cost is significant.
	payloadSize := len(req.GetPayload())
	if payloadSize > LargeMessageThreshold {
		if err := b.CheckMemoryPressureEager(); err != nil {
			return nil, err
		}
	} else {
		if err := b.CheckMemoryPressure(); err != nil {
			return nil, err
		}
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

	now := time.Now()
	msg := &Message{
		ID:          msgID,
		Queue:       req.GetQueue(),
		Payload:     req.GetPayload(),
		Headers:     req.GetHeaders(),
		Attempt:     0,
		PublishedAt: now,
		VisibleAt:   now,
		OrderingKey: req.GetOrderingKey(),
	}

	q, err := b.GetOrCreateQueueWithError(req.GetQueue())
	if err != nil {
		return nil, err
	}
	if b.storage != nil {
		if err := b.storage.SaveMessage(msg); err != nil {
			return nil, err
		}
	}
	if err := q.Enqueue(msg); err != nil {
		if b.storage != nil {
			if rbErr := b.storage.DeleteMessage(msg.Queue, msg.ID); rbErr != nil {
				return nil, errors.Join(err, rbErr)
			}
		}
		return nil, err
	}

	return &protocol.PublishResponse{MessageId: msgID}, nil
}

// HandleConsume processes a consume request and returns a channel for messages.
// If a group name is provided, the consumer joins that group; otherwise legacy behavior.
func (b *Broker) HandleConsume(req *protocol.ConsumeRequest, memberID string) <-chan *Message {
	timeout := defaultVisibilityTimeout
	if req.GetVisibilityTimeout() > 0 {
		timeout = time.Duration(req.GetVisibilityTimeout()) * time.Second
	}

	q := b.GetOrCreateQueue(req.GetQueue())

	groupName := req.GetGroup()
	if groupName == "" {
		return q.Dequeue(timeout)
	}

	g := q.GetOrCreateGroup(groupName)
	return g.AddMember(memberID, timeout)
}

// HandleAck processes an ack request.
// If groupName is non-empty, ack within that group instead of the queue's ungrouped consumers.
func (b *Broker) HandleAck(req *protocol.AckRequest, queueName, groupName string) (bool, error) {
	q := b.GetQueue(queueName)
	if q == nil {
		return false, nil
	}

	deleteStored := func(*Message) error {
		if b.storage == nil {
			return nil
		}
		return b.storage.DeleteMessage(queueName, req.GetMessageId())
	}

	if groupName != "" {
		g := q.GetGroup(groupName)
		if g == nil {
			return false, nil
		}
		return g.ackWithHook(req.GetMessageId(), deleteStored)
	}

	return q.ackWithHook(req.GetMessageId(), deleteStored)
}

// HandleNack processes a nack request.
// If groupName is non-empty, nack within that group.
func (b *Broker) HandleNack(req *protocol.NackRequest, queueName, groupName string) (bool, error) {
	q := b.GetQueue(queueName)
	if q == nil {
		return false, nil
	}

	deleteDropped := func(*Message) error {
		if b.storage == nil {
			return nil
		}
		return b.storage.DeleteMessage(queueName, req.GetMessageId())
	}
	persistRequeue := func(msg *Message) error {
		if b.storage == nil {
			return nil
		}
		return b.storage.SaveMessage(msg)
	}

	if groupName != "" {
		g := q.GetGroup(groupName)
		if g == nil {
			return false, nil
		}
		result, err := g.nackWithHooks(req.GetMessageId(), req.GetRequeue(), q.MaxRetries(), q.FailurePolicy(), deleteDropped)
		if err != nil {
			return false, err
		}
		if !result.Found {
			return false, nil
		}
		if result.ToDLQ && result.Message != nil {
			dlqName := DLQName(queueName)
			dlq := b.GetOrCreateQueue(dlqName)
			result.Message.Queue = dlqName
			result.Message.VisibleAt = time.Now()
			dlq.Enqueue(result.Message)
			if b.storage != nil {
				b.storage.DeleteMessage(queueName, result.Message.ID)
				b.storage.SaveMessage(result.Message)
			}
		}
		return true, nil
	}

	result, err := q.nackWithHooks(req.GetMessageId(), req.GetRequeue(), deleteDropped, persistRequeue)
	if err != nil {
		return false, err
	}
	if !result.Found {
		return false, nil
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
			b.storage.DeleteMessage(queueName, result.Message.ID)
			// Save to DLQ storage
			b.storage.SaveMessage(result.Message)
		}
	}

	return true, nil
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

// HandleHeartbeat processes a heartbeat from a group member.
func (b *Broker) HandleHeartbeat(req *protocol.HeartbeatRequest, memberID string) bool {
	q := b.GetQueue(req.GetQueue())
	if q == nil {
		return false
	}
	g := q.GetGroup(req.GetGroup())
	if g == nil {
		return false
	}
	return g.Heartbeat(memberID)
}

// HandleUnsubscribe removes a consumer from a queue/group.
// Returns true if the consumer was found and removed.
func (b *Broker) HandleUnsubscribe(req *protocol.UnsubscribeRequest, memberID string, msgCh <-chan *Message) bool {
	q := b.GetQueue(req.GetQueue())
	if q == nil {
		return false
	}

	groupName := req.GetGroup()
	if groupName != "" {
		g := q.GetGroup(groupName)
		if g == nil {
			return false
		}
		empty := g.RemoveMember(memberID)
		if empty {
			q.RemoveGroup(groupName)
		}
		return true
	}

	// Legacy: remove ungrouped consumer
	if msgCh != nil {
		q.RemoveConsumer(msgCh)
		return true
	}
	return false
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
		OrderingKey: msg.OrderingKey,
	}
}

// HandleStreamPublish processes a publish to a stream queue.
func (b *Broker) HandleStreamPublish(req *protocol.PublishRequest) (*protocol.PublishResponse, uint64, error) {
	// Memory pressure checks (same as queue mode)
	payloadSize := len(req.GetPayload())
	if payloadSize > LargeMessageThreshold {
		if err := b.CheckMemoryPressureEager(); err != nil {
			return nil, 0, err
		}
	} else {
		if err := b.CheckMemoryPressure(); err != nil {
			return nil, 0, err
		}
	}

	if b.dedup != nil {
		if err := b.dedup.Check(req.GetIdempotencyKey()); err != nil {
			return nil, 0, err
		}
	}

	msgID := req.GetMessageId()
	if msgID == "" {
		msgID = NewULID()
	}

	now := time.Now()
	msg := &Message{
		ID:          msgID,
		Queue:       req.GetQueue(),
		Payload:     req.GetPayload(),
		Headers:     req.GetHeaders(),
		Attempt:     0,
		PublishedAt: now,
	}

	sq, err := b.GetOrCreateStreamQueueWithError(req.GetQueue())
	if err != nil {
		return nil, 0, err
	}
	seq, err := sq.Publish(msg)
	if err != nil {
		return nil, 0, err
	}

	return &protocol.PublishResponse{MessageId: msgID}, seq, nil
}

// HandleSeek processes a seek request for a stream consumer.
func (b *Broker) HandleSeek(req *protocol.SeekRequest) (uint64, error) {
	sq := b.GetStreamQueue(req.GetQueue())
	if sq == nil {
		return 0, ErrQueueFull // reuse existing error for "not found" (TODO: proper error)
	}

	var offset uint64
	switch req.GetPosition() {
	case protocol.SeekPosition_SEEK_BEGINNING:
		offset = 1
	case protocol.SeekPosition_SEEK_END:
		offset = sq.NextSequence()
	case protocol.SeekPosition_SEEK_OFFSET:
		offset = req.GetOffset()
	case protocol.SeekPosition_SEEK_TIMESTAMP:
		if sq.storage != nil {
			ts, err := sq.storage.GetStreamMessageByTimestamp(sq.name, req.GetTimestamp())
			if err != nil {
				return 0, err
			}
			if ts == 0 {
				offset = sq.NextSequence() // no message found, start at end
			} else {
				offset = ts
			}
		}
	}

	sq.Seek(req.GetConsumerGroup(), offset)
	return offset, nil
}

// HandleCommitOffset processes an offset commit request.
func (b *Broker) HandleCommitOffset(req *protocol.CommitOffsetRequest) error {
	sq := b.GetStreamQueue(req.GetQueue())
	if sq == nil {
		return ErrQueueFull
	}
	return sq.CommitOffset(req.GetConsumerGroup(), req.GetOffset())
}

// StreamMessageToProto converts a Message to a protocol StreamMessage.
func StreamMessageToProto(msg *Message) *protocol.StreamMessage {
	return &protocol.StreamMessage{
		MessageId:   msg.ID,
		Queue:       msg.Queue,
		Payload:     msg.Payload,
		Headers:     msg.Headers,
		Sequence:    msg.Sequence,
		PublishedAt: msg.PublishedAt.UnixMilli(),
	}
}

// EncodeError creates an error response frame.
func EncodeError(code uint32, message string) []byte {
	resp := &protocol.ErrorResponse{Code: code, Message: message}
	payload, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpError, payload)
}
