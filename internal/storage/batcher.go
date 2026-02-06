package storage

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// writeRequest is a single message write queued for batching.
type writeRequest struct {
	key  []byte
	data []byte
	err  chan error // caller blocks on this
}

// writeBatcher collects individual writes and flushes them as a single
// BadgerDB WriteBatch. This amortizes transaction overhead across many
// messages, dramatically improving throughput under concurrent load.
type writeBatcher struct {
	db       *badger.DB
	interval time.Duration
	maxSize  int
	inbox    chan *writeRequest
	done     chan struct{}
	wg       sync.WaitGroup
}

// newWriteBatcher starts a background goroutine that collects writes
// and flushes them in batches. interval is the max time to wait before
// flushing. maxSize is the max number of writes per batch.
func newWriteBatcher(db *badger.DB, interval time.Duration, maxSize int) *writeBatcher {
	b := &writeBatcher{
		db:       db,
		interval: interval,
		maxSize:  maxSize,
		inbox:    make(chan *writeRequest, maxSize*2),
		done:     make(chan struct{}),
	}
	b.wg.Add(1)
	go b.loop()
	return b
}

// submit queues a message write and blocks until it's flushed to BadgerDB.
func (b *writeBatcher) submit(msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	req := &writeRequest{
		key:  msgKey(msg.Queue, msg.ID),
		data: data,
		err:  make(chan error, 1),
	}
	select {
	case b.inbox <- req:
		return <-req.err
	case <-b.done:
		return ErrBatcherClosed
	}
}

// loop is the background goroutine that collects and flushes writes.
func (b *writeBatcher) loop() {
	defer b.wg.Done()

	batch := make([]*writeRequest, 0, b.maxSize)
	timer := time.NewTimer(b.interval)
	defer timer.Stop()

	// Stop the timer initially -- we only start it when the first write arrives
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case req, ok := <-b.inbox:
			if !ok {
				// Channel closed during shutdown -- flush what we have
				if len(batch) > 0 {
					b.flush(batch)
				}
				return
			}
			if len(batch) == 0 {
				// First write in a new batch: start the timer
				timer.Reset(b.interval)
			}
			batch = append(batch, req)
			if len(batch) >= b.maxSize {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				b.flush(batch)
				batch = batch[:0]
			}

		case <-timer.C:
			if len(batch) > 0 {
				b.flush(batch)
				batch = batch[:0]
			}

		case <-b.done:
			// Drain remaining from inbox
			close(b.inbox)
			for req := range b.inbox {
				batch = append(batch, req)
			}
			if len(batch) > 0 {
				b.flush(batch)
			}
			return
		}
	}
}

// flush writes all pending requests as a single BadgerDB WriteBatch.
func (b *writeBatcher) flush(batch []*writeRequest) {
	wb := b.db.NewWriteBatch()
	var batchErr error

	for _, req := range batch {
		if err := wb.Set(req.key, req.data); err != nil {
			batchErr = err
			break
		}
	}

	if batchErr == nil {
		batchErr = wb.Flush()
	} else {
		wb.Cancel()
	}

	if batchErr != nil {
		log.Printf("write batch flush error: %v", batchErr)
	}

	// Notify all callers
	for _, req := range batch {
		req.err <- batchErr
	}
}

// close drains the batcher and waits for the background goroutine to exit.
func (b *writeBatcher) close() {
	close(b.done)
	b.wg.Wait()
}

// ErrBatcherClosed is returned when submitting to a closed batcher.
var ErrBatcherClosed = errBatcherClosed{}

type errBatcherClosed struct{}

func (errBatcherClosed) Error() string { return "write batcher closed" }
