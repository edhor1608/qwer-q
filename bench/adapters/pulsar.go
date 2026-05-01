package adapters

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// PulsarAdapter implements Adapter for Apache Pulsar.
type PulsarAdapter struct {
	url      string
	client   pulsar.Client
	producer pulsar.Producer
	consumer pulsar.Consumer
}

// NewPulsarAdapter creates a new Pulsar adapter.
func NewPulsarAdapter(url string) *PulsarAdapter {
	if url == "" {
		url = "pulsar://localhost:6650"
	}
	return &PulsarAdapter{url: url}
}

func (a *PulsarAdapter) Name() string { return "Pulsar" }

func (a *PulsarAdapter) Setup(ctx context.Context) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               a.url,
		OperationTimeout:  30000,
		ConnectionTimeout: 30000,
	})
	if err != nil {
		return fmt.Errorf("failed to create pulsar client: %w", err)
	}
	a.client = client
	return nil
}

func (a *PulsarAdapter) Teardown() error {
	if a.producer != nil {
		a.producer.Close()
	}
	if a.consumer != nil {
		a.consumer.Close()
	}
	if a.client != nil {
		a.client.Close()
	}
	return nil
}

func (a *PulsarAdapter) Publish(ctx context.Context, queue string, payload []byte) error {
	if a.producer == nil {
		producer, err := a.client.CreateProducer(pulsar.ProducerOptions{
			Topic: queue,
		})
		if err != nil {
			return fmt.Errorf("failed to create producer: %w", err)
		}
		a.producer = producer
	}

	_, err := a.producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: payload,
	})
	return err
}

func (a *PulsarAdapter) Consume(ctx context.Context, queue string, handler func([]byte) error) error {
	consumer, err := a.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            queue,
		SubscriptionName: fmt.Sprintf("bench-%d", time.Now().UnixNano()),
		Type:             pulsar.Shared,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	a.consumer = consumer

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := consumer.Receive(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			err = handler(msg.Payload())
			if err == ErrSkipAck {
				consumer.Nack(msg)
				continue
			}
			if err != nil {
				consumer.Nack(msg)
				return err
			}
			consumer.Ack(msg)
		}
	}
}
