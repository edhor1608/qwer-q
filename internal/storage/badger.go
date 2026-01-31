package storage

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	msgPrefix   = "msg:"
	queuePrefix = "queue:"
)

// DefaultSyncInterval is the default interval between fsync calls.
// 100ms balances speed (~5K/s) with durability (lose at most 100ms on power failure).
const DefaultSyncInterval = 100 * time.Millisecond

// BadgerStorage implements Storage using BadgerDB.
type BadgerStorage struct {
	db           *badger.DB
	done         chan struct{}
	syncInterval time.Duration
}

// StorageOption configures BadgerStorage.
type StorageOption func(*badgerConfig)

type badgerConfig struct {
	syncInterval time.Duration
}

// WithSyncInterval sets how often to fsync. 0 = sync every write (slow but safest).
func WithSyncInterval(d time.Duration) StorageOption {
	return func(c *badgerConfig) { c.syncInterval = d }
}

// NewBadgerStorage opens or creates a BadgerDB at the given path.
func NewBadgerStorage(path string, options ...StorageOption) (*BadgerStorage, error) {
	cfg := &badgerConfig{syncInterval: DefaultSyncInterval}
	for _, opt := range options {
		opt(cfg)
	}

	// SyncWrites=true only if syncInterval is 0 (sync every write)
	syncEveryWrite := cfg.syncInterval == 0

	opts := badger.DefaultOptions(path).
		WithLogger(nil).
		WithNumMemtables(2).
		WithMemTableSize(32 << 20).
		WithValueLogFileSize(64 << 20).
		WithNumLevelZeroTables(2).
		WithNumLevelZeroTablesStall(4).
		WithBlockCacheSize(32 << 20).
		WithIndexCacheSize(16 << 20).
		WithCompression(0).
		WithSyncWrites(syncEveryWrite).
		WithValueThreshold(1 << 10).
		WithNumCompactors(2).
		WithNumVersionsToKeep(1)

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
		go s.syncLoop()
	}

	return s, nil
}

// msgKey returns the storage key for a message.
func msgKey(queue, id string) []byte {
	return []byte(msgPrefix + queue + ":" + id)
}

// SaveMessage persists a message.
func (s *BadgerStorage) SaveMessage(msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(msgKey(msg.Queue, msg.ID), data)
	})
}

// DeleteMessage removes a message by ID.
func (s *BadgerStorage) DeleteMessage(id string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(msgPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			// Key format: msg:{queue}:{id}
			parts := strings.Split(string(key), ":")
			if len(parts) >= 3 && parts[len(parts)-1] == id {
				return txn.Delete(key)
			}
		}
		return nil
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
				if err := json.Unmarshal(val, &msg); err != nil {
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
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.db.Sync()
		case <-s.done:
			return
		}
	}
}

// Close syncs data and closes the database.
func (s *BadgerStorage) Close() error {
	close(s.done)
	s.db.Sync() // Final sync before close
	return s.db.Close()
}
