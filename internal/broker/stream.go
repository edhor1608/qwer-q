package broker

import (
	"sync"
	"time"

	"github.com/jonas/qwer-q/internal/storage"
)

// StreamConsumer tracks a consumer's position in a stream.
type StreamConsumer struct {
	Group   string
	Ch      chan *Message
	offset  uint64 // next sequence to deliver
	stopCh  chan struct{}
}

// StreamQueue implements log semantics: messages are retained, consumers track offsets.
type StreamQueue struct {
	mu        sync.Mutex
	name      string
	nextSeq   uint64 // next sequence number to assign
	consumers []*StreamConsumer
	storage   storage.StreamStorage

	// Retention config
	retentionMaxAge   time.Duration // 0 = unlimited
	retentionMaxBytes int64         // 0 = unlimited
}

// NewStreamQueue creates a stream queue backed by storage.
func NewStreamQueue(name string, store storage.StreamStorage) *StreamQueue {
	sq := &StreamQueue{
		name:    name,
		nextSeq: 1, // sequences start at 1
		storage: store,
	}

	// Restore sequence number from storage
	if store != nil {
		seq, err := store.GetStreamSequence(name)
		if err == nil && seq > 0 {
			sq.nextSeq = seq + 1
		}
	}

	return sq
}

// Name returns the stream queue name.
func (sq *StreamQueue) Name() string {
	return sq.name
}

// SetRetention configures retention policy.
func (sq *StreamQueue) SetRetention(maxAge time.Duration, maxBytes int64) {
	sq.mu.Lock()
	sq.retentionMaxAge = maxAge
	sq.retentionMaxBytes = maxBytes
	sq.mu.Unlock()
}

// Publish appends a message to the stream log and notifies consumers.
func (sq *StreamQueue) Publish(msg *Message) (uint64, error) {
	sq.mu.Lock()
	seq := sq.nextSeq
	sq.nextSeq++

	msg.Sequence = seq

	// Persist to stream storage
	if sq.storage != nil {
		smsg := &storage.StreamMessage{
			Message:  *msg,
			Sequence: seq,
		}
		if err := sq.storage.SaveStreamMessage(sq.name, seq, smsg); err != nil {
			sq.nextSeq-- // rollback
			sq.mu.Unlock()
			return 0, err
		}
	}

	// Notify consumers that have caught up to the tail
	for _, c := range sq.consumers {
		if c.offset == seq {
			select {
			case c.Ch <- msg:
				c.offset = seq + 1
			default:
				// Consumer channel full, they'll catch up on next poll
			}
		}
	}

	sq.mu.Unlock()
	return seq, nil
}

// Subscribe creates a stream consumer starting at the given offset.
// If offset is 0, starts from the next new message (tail).
func (sq *StreamQueue) Subscribe(group string, startOffset uint64) *StreamConsumer {
	ch := make(chan *Message, 100) // buffered for stream consumers
	sc := &StreamConsumer{
		Group:  group,
		Ch:     ch,
		offset: startOffset,
		stopCh: make(chan struct{}),
	}

	sq.mu.Lock()
	// If offset is 0 (end), set to current tail
	if startOffset == 0 {
		sc.offset = sq.nextSeq
	}
	sq.consumers = append(sq.consumers, sc)
	sq.mu.Unlock()

	// Start background delivery from storage for historical messages
	if startOffset > 0 && startOffset < sq.nextSeq {
		go sq.deliverFromStorage(sc)
	}

	return sc
}

// deliverFromStorage reads historical messages from storage and delivers them.
func (sq *StreamQueue) deliverFromStorage(sc *StreamConsumer) {
	const batchSize = 100

	for {
		select {
		case <-sc.stopCh:
			return
		default:
		}

		sq.mu.Lock()
		currentOffset := sc.offset
		tailSeq := sq.nextSeq
		sq.mu.Unlock()

		if currentOffset >= tailSeq {
			// Caught up to tail, new messages will be pushed in Publish()
			return
		}

		if sq.storage == nil {
			return
		}

		msgs, err := sq.storage.LoadStreamMessages(sq.name, currentOffset, batchSize)
		if err != nil || len(msgs) == 0 {
			return
		}

		for _, smsg := range msgs {
			msg := &smsg.Message
			msg.Sequence = smsg.Sequence
			select {
			case sc.Ch <- msg:
				sq.mu.Lock()
				sc.offset = smsg.Sequence + 1
				sq.mu.Unlock()
			case <-sc.stopCh:
				return
			}
		}
	}
}

// Seek repositions a consumer to a specific offset.
func (sq *StreamQueue) Seek(group string, offset uint64) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	for _, c := range sq.consumers {
		if c.Group == group {
			c.offset = offset
			// Restart delivery from storage if seeking backwards
			go sq.deliverFromStorage(c)
			return
		}
	}
}

// CommitOffset persists a consumer group's offset.
func (sq *StreamQueue) CommitOffset(group string, offset uint64) error {
	if sq.storage == nil {
		return nil
	}
	return sq.storage.SaveConsumerOffset(sq.name, group, offset)
}

// GetCommittedOffset returns the last committed offset for a consumer group.
func (sq *StreamQueue) GetCommittedOffset(group string) (uint64, error) {
	if sq.storage == nil {
		return 0, nil
	}
	return sq.storage.LoadConsumerOffset(sq.name, group)
}

// RemoveConsumer removes a stream consumer.
func (sq *StreamQueue) RemoveConsumer(ch <-chan *Message) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	for i, c := range sq.consumers {
		if c.Ch == ch {
			close(c.stopCh)
			close(c.Ch)
			sq.consumers = append(sq.consumers[:i], sq.consumers[i+1:]...)
			return
		}
	}
}

// Len returns the total number of messages in the stream.
func (sq *StreamQueue) Len() int {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return int(sq.nextSeq - 1) // nextSeq starts at 1
}

// NextSequence returns the next sequence number that will be assigned.
func (sq *StreamQueue) NextSequence() uint64 {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return sq.nextSeq
}

// RunRetention cleans up messages based on retention policy.
// Called periodically by the broker's retention goroutine.
func (sq *StreamQueue) RunRetention() error {
	sq.mu.Lock()
	maxAge := sq.retentionMaxAge
	maxBytes := sq.retentionMaxBytes
	sq.mu.Unlock()

	if sq.storage == nil || (maxAge == 0 && maxBytes == 0) {
		return nil
	}

	// Time-based retention
	if maxAge > 0 {
		cutoffMs := time.Now().Add(-maxAge).UnixMilli()
		// Find the first message that is NOT expired
		seq, err := sq.storage.GetStreamMessageByTimestamp(sq.name, cutoffMs)
		if err != nil {
			return err
		}
		if seq > 0 {
			if err := sq.storage.DeleteStreamMessagesBefore(sq.name, seq); err != nil {
				return err
			}
		}
	}

	// Size-based retention: load stats and delete old messages if over limit
	if maxBytes > 0 {
		oldest, newest, err := sq.storage.GetStreamStats(sq.name)
		if err != nil || oldest == 0 {
			return err
		}

		// Estimate: load messages in batches, accumulate size from newest,
		// delete everything older than the cutoff.
		// For v1, a simple approach: iterate from oldest, delete until under limit.
		var totalSize int64
		const batchSize = 100
		var deleteUpTo uint64

		// Scan from newest backwards to find how much we can keep
		for seq := newest; seq >= oldest; {
			start := seq - uint64(batchSize) + 1
			if start < oldest {
				start = oldest
			}
			msgs, err := sq.storage.LoadStreamMessages(sq.name, start, batchSize)
			if err != nil {
				return err
			}
			// Process in reverse (newest first)
			for i := len(msgs) - 1; i >= 0; i-- {
				totalSize += int64(len(msgs[i].Payload))
				if totalSize > maxBytes {
					deleteUpTo = msgs[i].Sequence + 1
					goto doDelete
				}
			}
			if start <= oldest {
				break
			}
			seq = start - 1
		}
		return nil

	doDelete:
		if deleteUpTo > 0 {
			return sq.storage.DeleteStreamMessagesBefore(sq.name, deleteUpTo)
		}
	}

	return nil
}
