package storage

import "github.com/jonas/qwer-q/internal/types"

// Message is an alias for types.Message.
type Message = types.Message

// QueueMode defines whether a queue operates in queue or stream mode.
type QueueMode string

const (
	QueueModeQueue  QueueMode = "queue"  // Default: messages deleted after ack
	QueueModeStream QueueMode = "stream" // Log semantics: messages retained, consumers track offsets
)

// QueueConfig holds queue configuration.
type QueueConfig struct {
	MaxSize       int    `json:"max_size"`
	MaxRetries    int    `json:"max_retries"`
	FailurePolicy string `json:"failure_policy"` // "dlq", "drop", "infinite"

	// Stream mode fields
	Mode              QueueMode `json:"mode,omitempty"`               // "queue" (default) or "stream"
	RetentionMaxAge   int64     `json:"retention_max_age,omitempty"`  // Max age in seconds (0 = unlimited)
	RetentionMaxBytes int64     `json:"retention_max_bytes,omitempty"` // Max total bytes (0 = unlimited)
}

// StreamMessage extends Message with a sequence number for stream mode.
type StreamMessage struct {
	types.Message
	Sequence uint64 `json:"sequence"`
}

// Storage interface for message persistence.
type Storage interface {
	SaveMessage(msg *Message) error
	DeleteMessage(id string) error
	LoadMessages(queue string) ([]*Message, error)
	SaveQueue(name string, config QueueConfig) error
	LoadQueues() (map[string]QueueConfig, error)
	Close() error
}

// StreamStorage extends Storage with stream-mode operations.
type StreamStorage interface {
	Storage

	// SaveStreamMessage stores a message with its sequence number.
	SaveStreamMessage(queue string, seq uint64, msg *StreamMessage) error

	// LoadStreamMessages loads messages from a queue starting at the given offset.
	// Returns up to limit messages.
	LoadStreamMessages(queue string, fromSeq uint64, limit int) ([]*StreamMessage, error)

	// SaveConsumerOffset persists a consumer group's committed offset.
	SaveConsumerOffset(queue, group string, offset uint64) error

	// LoadConsumerOffset loads a consumer group's last committed offset.
	// Returns 0 if no offset has been committed.
	LoadConsumerOffset(queue, group string) (uint64, error)

	// LoadAllConsumerOffsets loads all consumer group offsets for a queue.
	LoadAllConsumerOffsets(queue string) (map[string]uint64, error)

	// GetStreamSequence returns the current highest sequence number for a queue.
	// Returns 0 if the stream is empty.
	GetStreamSequence(queue string) (uint64, error)

	// DeleteStreamMessagesBefore deletes all stream messages with sequence < seq.
	DeleteStreamMessagesBefore(queue string, seq uint64) error

	// GetStreamMessageByTimestamp finds the first message at or after the given timestamp.
	// Returns the sequence number, or 0 if no message found.
	GetStreamMessageByTimestamp(queue string, timestampMs int64) (uint64, error)

	// GetStreamStats returns the oldest and newest sequence numbers for a queue.
	GetStreamStats(queue string) (oldest, newest uint64, err error)
}
