package adapters

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RedisAdapter implements Adapter for Redis Streams.
type RedisAdapter struct {
	addr   string
	client *redis.Client
	group  string
}

// NewRedisAdapter creates a new Redis Streams adapter.
func NewRedisAdapter(addr string) *RedisAdapter {
	return &RedisAdapter{addr: addr, group: "bench-group"}
}

func (a *RedisAdapter) Name() string { return "Redis" }

func (a *RedisAdapter) Setup(ctx context.Context) error {
	a.client = redis.NewClient(&redis.Options{Addr: a.addr})
	return a.client.Ping(ctx).Err()
}

func (a *RedisAdapter) Teardown() error {
	if a.client != nil {
		return a.client.Close()
	}
	return nil
}

func (a *RedisAdapter) Publish(ctx context.Context, queue string, payload []byte) error {
	return a.client.XAdd(ctx, &redis.XAddArgs{
		Stream: queue,
		Values: map[string]interface{}{"data": payload},
	}).Err()
}

func (a *RedisAdapter) Consume(ctx context.Context, queue string, handler func([]byte) error) error {
	// Create consumer group if not exists
	a.client.XGroupCreateMkStream(ctx, queue, a.group, "0").Err()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		streams, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    a.group,
			Consumer: "bench-consumer",
			Streams:  []string{queue, ">"},
			Count:    100,
			Block:    100,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return err
		}
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				data := msg.Values["data"].(string)
				if err := handler([]byte(data)); err != nil {
					return err
				}
				a.client.XAck(ctx, queue, a.group, msg.ID)
			}
		}
	}
}
