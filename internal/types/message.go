package types

import (
	"crypto/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
)

// Message represents a message in the queue.
type Message struct {
	ID          string            `json:"id"`
	Queue       string            `json:"queue"`
	Payload     []byte            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attempt     uint32            `json:"attempt"`
	PublishedAt time.Time         `json:"published_at"`
	VisibleAt   time.Time         `json:"visible_at"`
	OrderingKey string            `json:"ordering_key,omitempty"`
	Sequence    uint64            `json:"sequence,omitempty"` // Stream mode: monotonic sequence number
}

const numEntropyShards = 8

type ulidShard struct {
	mu      sync.Mutex
	entropy *ulid.MonotonicEntropy
}

var (
	entropyNext   atomic.Uint64
	entropyShards [numEntropyShards]ulidShard
)

func init() {
	for i := range entropyShards {
		entropyShards[i].entropy = ulid.Monotonic(rand.Reader, 0)
	}
}

// NewULID generates a new ULID.
func NewULID() string {
	idx := int(entropyNext.Add(1) % numEntropyShards)
	shard := &entropyShards[idx]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), shard.entropy).String()
}
