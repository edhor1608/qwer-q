package adapters

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaAdapter implements Adapter for Apache Kafka.
type KafkaAdapter struct {
	brokers []string
	writer  *kafka.Writer
	reader  *kafka.Reader
}

// NewKafkaAdapter creates a new Kafka adapter.
func NewKafkaAdapter(brokers ...string) *KafkaAdapter {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	return &KafkaAdapter{brokers: brokers}
}

func (a *KafkaAdapter) Name() string { return "Kafka" }

func (a *KafkaAdapter) Setup(ctx context.Context) error {
	a.writer = &kafka.Writer{
		Addr:         kafka.TCP(a.brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1 * time.Millisecond, // Low latency
		RequiredAcks: kafka.RequireOne,     // At-least-once
	}
	return nil
}

func (a *KafkaAdapter) Teardown() error {
	if a.writer != nil {
		a.writer.Close()
	}
	if a.reader != nil {
		a.reader.Close()
	}
	return nil
}

func (a *KafkaAdapter) Publish(ctx context.Context, queue string, payload []byte) error {
	return a.writer.WriteMessages(ctx, kafka.Message{
		Topic: queue,
		Value: payload,
	})
}

func (a *KafkaAdapter) Consume(ctx context.Context, queue string, handler func([]byte) error) error {
	a.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  a.brokers,
		Topic:    queue,
		GroupID:  "bench-consumer",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := a.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if err := handler(msg.Value); err != nil {
				return err
			}
			// Commit offset (at-least-once)
			if err := a.reader.CommitMessages(ctx, msg); err != nil {
				return err
			}
		}
	}
}
