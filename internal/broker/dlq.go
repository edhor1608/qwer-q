package broker

import (
	"fmt"
	"time"
)

// FailurePolicy defines how to handle messages that exceed max retries.
type FailurePolicy string

const (
	// FailurePolicyDLQ moves failed messages to a dead letter queue.
	FailurePolicyDLQ FailurePolicy = "dlq"
	// FailurePolicyDrop discards failed messages.
	FailurePolicyDrop FailurePolicy = "drop"
	// FailurePolicyInfinite retries forever.
	FailurePolicyInfinite FailurePolicy = "infinite"
)

// DefaultMaxRetries is the default number of delivery attempts before DLQ.
const DefaultMaxRetries uint32 = 5

// DLQSuffix is appended to queue name for dead letter queue.
const DLQSuffix = ".dlq"

// DLQName returns the dead letter queue name for the given queue.
func DLQName(queue string) string {
	return queue + DLQSuffix
}

// IsDLQ returns true if the queue name is a dead letter queue.
func IsDLQ(queue string) bool {
	return len(queue) > len(DLQSuffix) && queue[len(queue)-len(DLQSuffix):] == DLQSuffix
}

// PurgeQueue removes all runtime and durable messages for a queue.
func (b *Broker) PurgeQueue(name string) (int, error) {
	q := b.GetQueue(name)
	if q == nil {
		return 0, nil
	}
	if b.storage != nil {
		if err := b.storage.DeleteQueueMessages(name); err != nil {
			return 0, err
		}
	}
	return q.Purge(), nil
}

// RetryDLQ moves all pending DLQ messages back to their original queue.
func (b *Broker) RetryDLQ(name string) (int, error) {
	dlqName := DLQName(name)
	dlq := b.GetQueue(dlqName)
	if dlq == nil {
		return 0, nil
	}

	q := b.GetOrCreateQueue(name)
	retried := 0
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	for len(dlq.messages) > 0 {
		msg := dlq.messages[0]
		msgCopy := *msg
		msgCopy.Queue = name
		msgCopy.Attempt = 0
		msgCopy.VisibleAt = time.Now()
		if b.storage != nil {
			if err := b.storage.SaveMessage(&msgCopy); err != nil {
				return retried, err
			}
			if err := b.storage.DeleteMessage(dlqName, msg.ID); err != nil {
				if rbErr := b.storage.DeleteMessage(name, msgCopy.ID); rbErr != nil {
					return retried, fmt.Errorf("delete DLQ message %s from %s failed: %w; rollback from %s also failed: %w", msg.ID, dlqName, err, name, rbErr)
				}
				return retried, err
			}
		}
		if err := q.Enqueue(&msgCopy); err != nil {
			if b.storage != nil {
				if rbErr := b.storage.SaveMessage(msg); rbErr != nil {
					return retried, fmt.Errorf("enqueue retry message %s to %s failed: %w; DLQ restore to %s also failed: %w", msgCopy.ID, name, err, dlqName, rbErr)
				}
				if rbErr := b.storage.DeleteMessage(name, msgCopy.ID); rbErr != nil {
					return retried, fmt.Errorf("enqueue retry message %s to %s failed: %w; original queue rollback also failed: %w", msgCopy.ID, name, err, rbErr)
				}
			}
			return retried, err
		}
		copy(dlq.messages, dlq.messages[1:])
		dlq.messages[len(dlq.messages)-1] = nil
		dlq.messages = dlq.messages[:len(dlq.messages)-1]
		retried++
	}
	return retried, nil
}
