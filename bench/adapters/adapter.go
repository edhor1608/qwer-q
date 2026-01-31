package adapters

import "context"

// Adapter defines the interface for message queue adapters.
type Adapter interface {
	Name() string
	Setup(ctx context.Context) error
	Teardown() error
	Publish(ctx context.Context, queue string, payload []byte) error
	Consume(ctx context.Context, queue string, handler func([]byte) error) error
}
