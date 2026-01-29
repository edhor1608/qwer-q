package broker

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// Message represents a message in the queue.
type Message struct {
	ID          string
	Queue       string
	Payload     []byte
	Headers     map[string]string
	Attempt     uint32
	PublishedAt time.Time
	VisibleAt   time.Time
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
