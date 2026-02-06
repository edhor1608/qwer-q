package types

import (
	"crypto/rand"
	"sync"
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
}

var (
	entropyMu sync.Mutex
	entropy   = ulid.Monotonic(rand.Reader, 0)
)

// NewULID generates a new ULID.
func NewULID() string {
	entropyMu.Lock()
	defer entropyMu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
}
