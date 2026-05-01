package adapters

import (
	"context"

	"github.com/jonas/qwer-q/internal/protocol"
	"github.com/jonas/qwer-q/pkg/client"
)

// QWERQAdapter implements Adapter for QWER-Q.
type QWERQAdapter struct {
	addr       string
	pubClient  *client.Client
	consClient *client.Client
}

// NewQWERQAdapter creates a new QWER-Q adapter.
func NewQWERQAdapter(addr string) *QWERQAdapter {
	return &QWERQAdapter{addr: addr}
}

func (a *QWERQAdapter) Name() string { return "QWER-Q" }

func (a *QWERQAdapter) Setup(ctx context.Context) error {
	var err error
	a.pubClient, err = client.Dial(a.addr)
	if err != nil {
		return err
	}
	a.consClient, err = client.Dial(a.addr)
	if err != nil {
		a.pubClient.Close()
		return err
	}
	return nil
}

func (a *QWERQAdapter) Teardown() error {
	if a.pubClient != nil {
		a.pubClient.Close()
	}
	if a.consClient != nil {
		a.consClient.Close()
	}
	return nil
}

func (a *QWERQAdapter) Publish(ctx context.Context, queue string, payload []byte) error {
	_, err := a.pubClient.Publish(queue, payload)
	return err
}

func (a *QWERQAdapter) RegisterSchema(ctx context.Context, queue string, descriptor []byte, messageType string) error {
	_, err := a.pubClient.SchemaRegister(queue, descriptor, messageType)
	return err
}

func (a *QWERQAdapter) Consume(ctx context.Context, queue string, handler func([]byte) error) error {
	// Close connection when context is cancelled to break out of blocking read
	go func() {
		<-ctx.Done()
		a.consClient.Close()
	}()

	return a.consClient.ConsumeWithVisibility(queue, 100, 1, func(msg *protocol.Message) error {
		err := handler(msg.Payload)
		if err == ErrSkipAck {
			return nil
		}
		if err != nil {
			return err
		}
		return a.consClient.Ack(msg.MessageId)
	})
}
