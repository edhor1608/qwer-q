package adapters

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// RedPandaAdapter implements Adapter for RedPanda (Kafka-compatible).
type RedPandaAdapter struct {
	brokers []string
	writer  *kafka.Writer
	reader  *kafka.Reader
}

// NewRedPandaAdapter creates a new RedPanda adapter.
func NewRedPandaAdapter(brokers ...string) *RedPandaAdapter {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9093"}
	}
	return &RedPandaAdapter{brokers: brokers}
}

func (a *RedPandaAdapter) Name() string { return "RedPanda" }

func (a *RedPandaAdapter) Setup(ctx context.Context) error {
	conn, err := kafka.Dial("tcp", a.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to redpanda: %w", err)
	}
	conn.Close()

	a.writer = &kafka.Writer{
		Addr:                   kafka.TCP(a.brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           1 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	return nil
}

func (a *RedPandaAdapter) Teardown() error {
	if a.writer != nil {
		a.writer.Close()
	}
	if a.reader != nil {
		a.reader.Close()
	}
	return nil
}

func (a *RedPandaAdapter) Publish(ctx context.Context, queue string, payload []byte) error {
	return a.writer.WriteMessages(ctx, kafka.Message{
		Topic: queue,
		Value: payload,
	})
}

func (a *RedPandaAdapter) Consume(ctx context.Context, queue string, handler func([]byte) error) error {
	if err := a.ensureTopic(queue); err != nil {
		return err
	}

	a.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        a.brokers,
		Topic:          queue,
		GroupID:        fmt.Sprintf("bench-%d", time.Now().UnixNano()),
		MinBytes:       1,
		MaxBytes:       10e6,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 100 * time.Millisecond,
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
			if err := a.reader.CommitMessages(ctx, msg); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func (a *RedPandaAdapter) ensureTopic(topic string) error {
	conn, err := kafka.Dial("tcp", a.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}
