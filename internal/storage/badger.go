package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// DefaultSyncInterval is how often to fsync the write-ahead log.
// 100ms provides good balance: ~10x throughput vs sync-every-write,
// max 100ms data loss on crash. Similar to Redis appendfsync everysec.
const DefaultSyncInterval = 100 * time.Millisecond

// StorageOption configures BadgerStorage.
type StorageOption func(*badgerConfig)

type badgerConfig struct {
	syncInterval  time.Duration
	batchInterval time.Duration // 0 = no batching
	batchMaxSize  int
}

// WithSyncInterval sets how often to fsync (0 = sync every write).
func WithSyncInterval(d time.Duration) StorageOption {
	return func(c *badgerConfig) { c.syncInterval = d }
}

// WithBatchInterval sets the write batch flush interval (0 = no batching).
// When enabled, multiple SaveMessage calls are collected and flushed as a
// single BadgerDB WriteBatch, amortizing transaction overhead.
func WithBatchInterval(d time.Duration) StorageOption {
	return func(c *badgerConfig) { c.batchInterval = d }
}

// WithBatchMaxSize sets the maximum number of writes per batch.
// When the batch reaches this size, it flushes immediately without
// waiting for the batch interval timer. Default: 100.
func WithBatchMaxSize(n int) StorageOption {
	return func(c *badgerConfig) { c.batchMaxSize = n }
}

// DefaultBatchMaxSize is the default maximum writes per batch.
const DefaultBatchMaxSize = 100

const (
	msgPrefix    = "msg:"
	queuePrefix  = "queue:"
	streamPrefix = "stream:" // stream:{queue}:{sequence} → StreamMessage
	offsetPrefix = "offset:" // offset:{queue}:{group} → uint64
)

// BadgerStorage implements Storage using BadgerDB.
type BadgerStorage struct {
	db           *badger.DB
	done         chan struct{}
	syncInterval time.Duration
	batcher      *writeBatcher // nil when batching disabled
	closeOnce    sync.Once
	syncWg       sync.WaitGroup
}

// NewBadgerStorage opens or creates a BadgerDB at the given path.
func NewBadgerStorage(path string, options ...StorageOption) (*BadgerStorage, error) {
	cfg := &badgerConfig{syncInterval: DefaultSyncInterval}
	for _, opt := range options {
		opt(cfg)
	}

	// Validate sync interval (negative would panic in time.NewTicker)
	if cfg.syncInterval < 0 {
		return nil, fmt.Errorf("sync interval must be >= 0, got %v", cfg.syncInterval)
	}

	// SyncWrites=true only if syncInterval is 0 (sync every write)
	syncEveryWrite := cfg.syncInterval == 0

	opts := badger.DefaultOptions(path).
		WithLogger(nil).
		// Memory optimization: reduce memtable count and size
		WithNumMemtables(2).            // Default: 5 → 2 (saves ~192MB)
		WithMemTableSize(32 << 20).     // Default: 64MB → 32MB
		WithValueLogFileSize(64 << 20). // Default: 1GB → 64MB
		WithNumLevelZeroTables(2).      // Faster L0 compaction
		WithNumLevelZeroTablesStall(4). // Stall writes earlier
		WithBlockCacheSize(32 << 20).   // 32MB block cache (default: 256MB)
		WithIndexCacheSize(16 << 20).   // 16MB index cache
		WithCompression(0).             // No compression (CPU tradeoff)
		WithSyncWrites(syncEveryWrite). // Sync every write only if interval is 0
		// Value log optimization: store small values in LSM tree
		WithValueThreshold(1 << 10). // Values < 1KB in LSM (faster reads)
		WithNumCompactors(2).        // Fewer compactors to save resources (default: 4)
		WithNumVersionsToKeep(1)     // Only keep latest version

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	s := &BadgerStorage{
		db:           db,
		done:         make(chan struct{}),
		syncInterval: cfg.syncInterval,
	}

	// Start background sync if not syncing every write
	if !syncEveryWrite {
		s.syncWg.Add(1)
		go s.syncLoop()
	}

	// Start write batcher if configured
	if cfg.batchInterval > 0 {
		maxSize := cfg.batchMaxSize
		if maxSize <= 0 {
			maxSize = DefaultBatchMaxSize
		}
		s.batcher = newWriteBatcher(db, cfg.batchInterval, maxSize)
	}

	return s, nil
}

// msgKey returns the storage key for a message.
func msgKey(queue, id string) []byte {
	key := make([]byte, 0, len(msgPrefix)+len(queue)+1+len(id))
	key = append(key, msgPrefix...)
	key = append(key, queue...)
	key = append(key, ':')
	key = append(key, id...)
	return key
}

// SaveMessage persists a message. When write batching is enabled,
// the write is queued and flushed with other writes for better throughput.
// The call blocks until the write is durable in BadgerDB.
func (s *BadgerStorage) SaveMessage(msg *Message) error {
	if s.batcher != nil {
		return s.batcher.submit(msg)
	}
	data := encodeMessage(msg)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(msgKey(msg.Queue, msg.ID), data)
	})
}

// DeleteMessage removes a message by queue and ID.
func (s *BadgerStorage) DeleteMessage(queue, id string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(msgKey(queue, id))
	})
}

// LoadMessages loads all messages for a queue.
func (s *BadgerStorage) LoadMessages(queue string) ([]*Message, error) {
	var messages []*Message

	prefix := []byte(msgPrefix + queue + ":")
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var msg Message
				if err := decodeMessage(val, &msg); err != nil {
					return err
				}
				messages = append(messages, &msg)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return messages, err
}

// SaveQueue persists queue configuration.
func (s *BadgerStorage) SaveQueue(name string, config QueueConfig) error {
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(queuePrefix+name), data)
	})
}

// LoadQueues loads all queue configurations.
func (s *BadgerStorage) LoadQueues() (map[string]QueueConfig, error) {
	queues := make(map[string]QueueConfig)

	prefix := []byte(queuePrefix)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			name := strings.TrimPrefix(key, queuePrefix)

			err := item.Value(func(val []byte) error {
				var cfg QueueConfig
				if err := json.Unmarshal(val, &cfg); err != nil {
					return err
				}
				queues[name] = cfg
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return queues, err
}

// syncLoop periodically fsyncs data to disk.
func (s *BadgerStorage) syncLoop() {
	defer s.syncWg.Done()
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.db.Sync(); err != nil {
				log.Printf("badger sync error: %v", err)
			}
		case <-s.done:
			return
		}
	}
}

// Close drains the write batcher, syncs data, and closes the database.
// Safe to call multiple times.
func (s *BadgerStorage) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		// Stop batcher first so all pending writes flush before we close DB
		if s.batcher != nil {
			s.batcher.close()
		}
		close(s.done)
		s.syncWg.Wait()
		if err := s.db.Sync(); err != nil {
			log.Printf("badger final sync error: %v", err)
		}
		closeErr = s.db.Close()
	})
	return closeErr
}

// --- Stream storage methods ---

// streamKey returns the storage key for a stream message.
// Format: stream:{queue}:{sequence_padded_to_20_digits}
func streamKey(queue string, seq uint64) []byte {
	return []byte(fmt.Sprintf("%s%s:%020d", streamPrefix, queue, seq))
}

// offsetKey returns the storage key for a consumer group offset.
func offsetKey(queue, group string) []byte {
	return []byte(offsetPrefix + queue + ":" + group)
}

// SaveStreamMessage stores a message with its sequence number.
func (s *BadgerStorage) SaveStreamMessage(queue string, seq uint64, msg *StreamMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(streamKey(queue, seq), data)
	})
}

// LoadStreamMessages loads messages starting at fromSeq, up to limit.
func (s *BadgerStorage) LoadStreamMessages(queue string, fromSeq uint64, limit int) ([]*StreamMessage, error) {
	var messages []*StreamMessage

	prefix := []byte(streamPrefix + queue + ":")
	startKey := streamKey(queue, fromSeq)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Seek(startKey); it.ValidForPrefix(prefix) && count < limit; it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var msg StreamMessage
				if err := json.Unmarshal(val, &msg); err != nil {
					return err
				}
				messages = append(messages, &msg)
				return nil
			})
			if err != nil {
				return err
			}
			count++
		}
		return nil
	})

	return messages, err
}

// SaveConsumerOffset persists a consumer group's committed offset.
func (s *BadgerStorage) SaveConsumerOffset(queue, group string, offset uint64) error {
	data := []byte(fmt.Sprintf("%d", offset))
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(offsetKey(queue, group), data)
	})
}

// LoadConsumerOffset loads a consumer group's last committed offset.
func (s *BadgerStorage) LoadConsumerOffset(queue, group string) (uint64, error) {
	var offset uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(offsetKey(queue, group))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			_, err := fmt.Sscanf(string(val), "%d", &offset)
			return err
		})
	})
	return offset, err
}

// LoadAllConsumerOffsets loads all consumer group offsets for a queue.
func (s *BadgerStorage) LoadAllConsumerOffsets(queue string) (map[string]uint64, error) {
	offsets := make(map[string]uint64)
	prefix := []byte(offsetPrefix + queue + ":")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			// Key format: offset:{queue}:{group}
			group := key[len(offsetPrefix)+len(queue)+1:]

			err := item.Value(func(val []byte) error {
				var offset uint64
				_, err := fmt.Sscanf(string(val), "%d", &offset)
				if err != nil {
					return err
				}
				offsets[group] = offset
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return offsets, err
}

// GetStreamSequence returns the highest sequence number for a queue.
func (s *BadgerStorage) GetStreamSequence(queue string) (uint64, error) {
	var seq uint64
	prefix := []byte(streamPrefix + queue + ":")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to the end of the prefix range
		// For reverse iteration, seek to prefix + max possible suffix
		seekKey := []byte(streamPrefix + queue + ":\xff")
		it.Seek(seekKey)

		if it.ValidForPrefix(prefix) {
			key := string(it.Item().Key())
			// Key format: stream:{queue}:{sequence_padded_20}
			seqStr := key[len(streamPrefix)+len(queue)+1:]
			_, err := fmt.Sscanf(seqStr, "%d", &seq)
			return err
		}
		return nil
	})

	return seq, err
}

// DeleteStreamMessagesBefore deletes all stream messages with sequence < seq.
func (s *BadgerStorage) DeleteStreamMessagesBefore(queue string, seq uint64) error {
	prefix := []byte(streamPrefix + queue + ":")
	endKey := streamKey(queue, seq)

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var toDelete [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			if string(key) >= string(endKey) {
				break
			}
			toDelete = append(toDelete, key)
		}
		it.Close()

		for _, key := range toDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetStreamMessageByTimestamp finds the first message at or after the given timestamp.
func (s *BadgerStorage) GetStreamMessageByTimestamp(queue string, timestampMs int64) (uint64, error) {
	var foundSeq uint64
	prefix := []byte(streamPrefix + queue + ":")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var msg StreamMessage
				if err := json.Unmarshal(val, &msg); err != nil {
					return err
				}
				if msg.PublishedAt.UnixMilli() >= timestampMs {
					foundSeq = msg.Sequence
					return errFound // sentinel to break iteration
				}
				return nil
			})
			if err == errFound {
				return nil
			}
			if err != nil {
				return err
			}
		}
		return nil
	})

	return foundSeq, err
}

// sentinel error to break iteration
var errFound = fmt.Errorf("found")

// GetStreamStats returns the oldest and newest sequence numbers for a queue.
func (s *BadgerStorage) GetStreamStats(queue string) (oldest, newest uint64, err error) {
	prefix := []byte(streamPrefix + queue + ":")

	err = s.db.View(func(txn *badger.Txn) error {
		// Get oldest (forward iteration)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			key := string(it.Item().Key())
			seqStr := key[len(streamPrefix)+len(queue)+1:]
			_, err := fmt.Sscanf(seqStr, "%d", &oldest)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return
	}

	// Get newest
	newest, err = s.GetStreamSequence(queue)
	return
}
