package scenarios

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jonas/qwer-q/bench/adapters"
	"github.com/jonas/qwer-q/bench/harness"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const typedQueueMessageType = "bench.TypedEvent"

// TypedQueueConfig configures the typed queue benchmark layer.
type TypedQueueConfig struct {
	Config          harness.Config
	InvalidAttempts int
}

// RunTypedQueue runs the typed-queue benchmark layer.
func RunTypedQueue(ctx context.Context, adapter adapters.Adapter, cfg TypedQueueConfig) harness.TypedQueueResult {
	result := harness.TypedQueueResult{Name: adapter.Name()}

	typedAdapter, ok := adapter.(adapters.TypedQueueAdapter)
	if !ok {
		result.Notes = "no broker-enforced schema"
		return result
	}
	result.Supported = true

	if err := adapter.Setup(ctx); err != nil {
		result.Error = fmt.Errorf("setup: %w", err)
		return result
	}
	defer adapter.Teardown()

	descriptor := typedQueueDescriptor()
	base := fmt.Sprintf("bench-typed-%d", time.Now().UnixNano())

	throughputQueue := base + "-throughput"
	if err := typedAdapter.RegisterSchema(ctx, throughputQueue, descriptor, typedQueueMessageType); err != nil {
		result.Error = fmt.Errorf("register throughput schema: %w", err)
		return result
	}
	validMsgsPerSec, err := runTypedThroughput(ctx, adapter, throughputQueue, cfg.Config)
	if err != nil {
		result.Error = fmt.Errorf("throughput: %w", err)
		return result
	}
	result.ValidMsgsPerSec = validMsgsPerSec

	latencyQueue := base + "-latency"
	if err := typedAdapter.RegisterSchema(ctx, latencyQueue, descriptor, typedQueueMessageType); err != nil {
		result.Error = fmt.Errorf("register latency schema: %w", err)
		return result
	}
	latencyP95, err := runTypedLatency(ctx, adapter, latencyQueue, cfg.Config)
	if err != nil {
		result.Error = fmt.Errorf("latency: %w", err)
		return result
	}
	result.LatencyP95 = latencyP95

	invalidQueue := base + "-invalid"
	if err := typedAdapter.RegisterSchema(ctx, invalidQueue, descriptor, typedQueueMessageType); err != nil {
		result.Error = fmt.Errorf("register invalid schema: %w", err)
		return result
	}
	rejected, attempts, err := runInvalidTypedPublishes(ctx, adapter, invalidQueue, cfg.InvalidAttempts)
	if err != nil {
		result.Error = fmt.Errorf("invalid publishes: %w", err)
		return result
	}
	result.InvalidRejected = rejected
	result.InvalidAttempts = attempts
	if attempts > 0 {
		result.InvalidRejectRate = float64(rejected) / float64(attempts) * 100
	}
	result.Notes = "schema validation enabled"
	return result
}

func runTypedThroughput(ctx context.Context, adapter adapters.Adapter, queue string, cfg harness.Config) (float64, error) {
	payload := makeTypedPayload(cfg.MessageSize, 1, nil)
	var consumed int64
	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()
	go func() {
		_ = adapter.Consume(consCtx, queue, func(data []byte) error {
			atomic.AddInt64(&consumed, 1)
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	deadline := time.Now().Add(cfg.Duration)
	var published int64
	for time.Now().Before(deadline) {
		if err := adapter.Publish(ctx, queue, payload); err != nil {
			return 0, err
		}
		published++
	}

	waitDeadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt64(&consumed) < published && time.Now().Before(waitDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return float64(published) / cfg.Duration.Seconds(), nil
}

func runTypedLatency(ctx context.Context, adapter adapters.Adapter, queue string, cfg harness.Config) (time.Duration, error) {
	hist := harness.NewHistogram()
	var consumed int64
	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()
	go func() {
		_ = adapter.Consume(consCtx, queue, func(data []byte) error {
			payloadData, ok := extractTypedData(data)
			if ok && len(payloadData) >= 8 {
				sentNano := int64(binary.BigEndian.Uint64(payloadData[:8]))
				hist.Record(time.Duration(time.Now().UnixNano() - sentNano))
			}
			atomic.AddInt64(&consumed, 1)
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	rate := cfg.TargetRate
	if rate <= 0 || rate > 2000 {
		rate = 2000
	}
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.Now().Add(cfg.Duration)
	var published int64
	for time.Now().Before(deadline) {
		<-ticker.C
		stamp := make([]byte, 8)
		binary.BigEndian.PutUint64(stamp, uint64(time.Now().UnixNano()))
		if err := adapter.Publish(ctx, queue, makeTypedPayload(cfg.MessageSize, int32(published+1), stamp)); err != nil {
			return 0, err
		}
		published++
	}

	waitDeadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt64(&consumed) < published && time.Now().Before(waitDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return hist.Percentile(95), nil
}

func runInvalidTypedPublishes(ctx context.Context, adapter adapters.Adapter, queue string, attempts int) (rejected, total int, err error) {
	if attempts <= 0 {
		attempts = 100
	}
	invalidPayload := []byte{0x12, 0x80}
	for i := 0; i < attempts; i++ {
		total++
		if err := adapter.Publish(ctx, queue, invalidPayload); err != nil {
			rejected++
			continue
		}
	}
	return rejected, total, nil
}

func typedQueueDescriptor() []byte {
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{{
			Name:    proto.String("bench_typed.proto"),
			Package: proto.String("bench"),
			MessageType: []*descriptorpb.DescriptorProto{{
				Name: proto.String("TypedEvent"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:   proto.String("id"),
					Number: proto.Int32(1),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				}, {
					Name:   proto.String("data"),
					Number: proto.Int32(2),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				}},
			}},
			Syntax: proto.String("proto3"),
		}},
	}
	data, _ := proto.Marshal(fds)
	return data
}

func makeTypedPayload(targetSize int, id int32, prefix []byte) []byte {
	if targetSize < len(prefix)+4 {
		targetSize = len(prefix) + 4
	}
	dataLen := targetSize - 8
	if dataLen < len(prefix) {
		dataLen = len(prefix)
	}
	data := make([]byte, dataLen)
	copy(data, prefix)

	buf := make([]byte, 0, 16+len(data))
	buf = append(buf, 0x08)
	buf = binary.AppendUvarint(buf, uint64(id))
	buf = append(buf, 0x12)
	buf = binary.AppendUvarint(buf, uint64(len(data)))
	buf = append(buf, data...)
	return buf
}

func extractTypedData(payload []byte) ([]byte, bool) {
	if len(payload) == 0 || payload[0] != 0x08 {
		return nil, false
	}
	_, n := binary.Uvarint(payload[1:])
	if n <= 0 {
		return nil, false
	}
	offset := 1 + n
	if offset >= len(payload) || payload[offset] != 0x12 {
		return nil, false
	}
	length, n := binary.Uvarint(payload[offset+1:])
	if n <= 0 {
		return nil, false
	}
	start := offset + 1 + n
	end := start + int(length)
	if end > len(payload) {
		return nil, false
	}
	return payload[start:end], true
}
