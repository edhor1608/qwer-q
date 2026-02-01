package adapters

import (
	"context"

	"github.com/nats-io/nats.go"
)

// NATSAdapter implements Adapter for NATS.
type NATSAdapter struct {
	url  string
	conn *nats.Conn
	sub  *nats.Subscription
}

// NewNATSAdapter creates a new NATS adapter.
func NewNATSAdapter(url string) *NATSAdapter {
	return &NATSAdapter{url: url}
}

func (a *NATSAdapter) Name() string { return "NATS" }

func (a *NATSAdapter) Setup(ctx context.Context) error {
	var err error
	a.conn, err = nats.Connect(a.url)
	return err
}

func (a *NATSAdapter) Teardown() error {
	if a.sub != nil {
		a.sub.Unsubscribe()
	}
	if a.conn != nil {
		a.conn.Close()
	}
	return nil
}

func (a *NATSAdapter) Publish(ctx context.Context, queue string, payload []byte) error {
	return a.conn.Publish(queue, payload)
}

func (a *NATSAdapter) Consume(ctx context.Context, queue string, handler func([]byte) error) error {
	var err error
	a.sub, err = a.conn.Subscribe(queue, func(msg *nats.Msg) {
		handler(msg.Data)
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
