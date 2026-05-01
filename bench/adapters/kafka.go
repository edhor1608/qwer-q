package adapters

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaAdapter implements Adapter for Apache Kafka.
type KafkaAdapter struct {
	brokers []string
	writer  *kafka.Writer
	reader  *kafka.Reader
	topic   string
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
	// Verify connection to Kafka
	conn, err := kafka.Dial("tcp", a.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	conn.Close()

	a.writer = &kafka.Writer{
		Addr:                   kafka.TCP(a.brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           1 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true, // Auto-create topics
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
	a.topic = queue
	return a.writer.WriteMessages(ctx, kafka.Message{
		Topic: queue,
		Value: payload,
	})
}

func (a *KafkaAdapter) Consume(ctx context.Context, queue string, handler func([]byte) error) error {
	// Ensure topic exists by creating it if needed
	if err := a.ensureTopic(queue); err != nil {
		return err
	}

	a.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        a.brokers,
		Topic:          queue,
		GroupID:        fmt.Sprintf("bench-%d", time.Now().UnixNano()), // Unique group per run
		MinBytes:       1,
		MaxBytes:       10e6,
		StartOffset:    kafka.FirstOffset, // Start from beginning
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
			err = handler(msg.Value)
			if err == ErrSkipAck {
				continue
			}
			if err != nil {
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

func (a *KafkaAdapter) ensureTopic(topic string) error {
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
