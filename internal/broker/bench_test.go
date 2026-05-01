package broker

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"github.com/jonas/qwer-q/internal/storage"
)

func BenchmarkPersistentPublishDequeueAck(b *testing.B) {
	dir, err := os.MkdirTemp("", "broker-persist-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.NewBadgerStorage(dir, storage.WithSyncInterval(100*time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	broker := NewBroker(WithStorage(store), WithMemoryLimit(0))
	defer broker.Close()

	queueName := "bench-persistent-roundtrip"
	payload := make([]byte, 1024)
	consumeCh := broker.GetOrCreateQueue(queueName).Dequeue(30 * time.Second)

	publishReq := &protocol.PublishRequest{
		Queue:   queueName,
		Payload: payload,
	}
	ackReq := &protocol.AckRequest{}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := broker.HandlePublish(publishReq)
		if err != nil {
			b.Fatal(err)
		}

		msg := <-consumeCh
		if msg.ID != resp.MessageId {
			b.Fatalf("message id mismatch: got %s want %s", msg.ID, resp.MessageId)
		}

		ackReq.MessageId = msg.ID
		if !broker.HandleAck(ackReq, queueName, "") {
			b.Fatalf("ack failed for %s", msg.ID)
		}
	}
}

func BenchmarkQueueBacklogDispatchAck(b *testing.B) {
	for _, backlog := range []int{128, 1024, 8192, 65536} {
		b.Run(strconv.Itoa(backlog), func(b *testing.B) {
			q := NewQueue("bench-queue-backlog")
			msgs := make([]*Message, backlog+b.N+1)
			for i := range msgs {
				msgs[i] = &Message{ID: strconv.Itoa(i), Queue: q.name}
			}
			for i := 0; i < backlog; i++ {
				q.messages = append(q.messages, msgs[i])
			}
			consumeCh := q.Dequeue(30 * time.Second)
			if len(q.inFlight) != 1 {
				b.Fatalf("expected 1 in-flight message after initial dequeue, got %d", len(q.inFlight))
			}

			b.ReportAllocs()
			b.ResetTimer()

			next := backlog
			for i := 0; i < b.N; i++ {
				msg := <-consumeCh
				if !q.Ack(msg.ID) {
					b.Fatalf("ack failed for %s", msg.ID)
				}

				q.mu.Lock()
				q.messages = append(q.messages, msgs[next])
				next++
				q.mu.Unlock()
			}
		})
	}
}
