package adapters

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQAdapter implements Adapter for RabbitMQ.
type RabbitMQAdapter struct {
	url  string
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewRabbitMQAdapter creates a new RabbitMQ adapter.
func NewRabbitMQAdapter(url string) *RabbitMQAdapter {
	return &RabbitMQAdapter{url: url}
}

func (a *RabbitMQAdapter) Name() string { return "RabbitMQ" }

func (a *RabbitMQAdapter) Setup(ctx context.Context) error {
	var err error
	a.conn, err = amqp.Dial(a.url)
	if err != nil {
		return err
	}
	a.ch, err = a.conn.Channel()
	if err != nil {
		a.conn.Close()
		return err
	}
	return nil
}

func (a *RabbitMQAdapter) Teardown() error {
	if a.ch != nil {
		a.ch.Close()
	}
	if a.conn != nil {
		a.conn.Close()
	}
	return nil
}

func (a *RabbitMQAdapter) Publish(ctx context.Context, queue string, payload []byte) error {
	_, err := a.ch.QueueDeclare(queue, false, true, false, false, nil)
	if err != nil {
		return err
	}
	return a.ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		Body: payload,
	})
}

func (a *RabbitMQAdapter) Consume(ctx context.Context, queue string, handler func([]byte) error) error {
	_, err := a.ch.QueueDeclare(queue, false, true, false, false, nil)
	if err != nil {
		return err
	}
	msgs, err := a.ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-msgs:
			if err := handler(msg.Body); err != nil {
				return err
			}
		}
	}
}
