package storage

import "github.com/jonas/qwer-q/internal/types"

// Message is an alias for types.Message.
type Message = types.Message

// QueueConfig holds queue configuration.
type QueueConfig struct {
	MaxSize       int    `json:"max_size"`
	MaxRetries    int    `json:"max_retries"`
	FailurePolicy string `json:"failure_policy"` // "dlq", "drop", "infinite"
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
