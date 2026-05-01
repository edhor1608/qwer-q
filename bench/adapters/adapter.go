package adapters

import (
	"context"
	"errors"
)

// ErrSkipAck tells an adapter to deliver a message without acknowledging it.
var ErrSkipAck = errors.New("skip ack")

// Adapter defines the interface for message queue adapters.
type Adapter interface {
	Name() string
	Setup(ctx context.Context) error
	Teardown() error
	Publish(ctx context.Context, queue string, payload []byte) error
	Consume(ctx context.Context, queue string, handler func([]byte) error) error
}

// TypedQueueAdapter is implemented by brokers that support broker-enforced schemas.
type TypedQueueAdapter interface {
	Adapter
	RegisterSchema(ctx context.Context, queue string, descriptor []byte, messageType string) error
}
