package storage

import (
	"encoding/json"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

const (
	msgPrefix   = "msg:"
	queuePrefix = "queue:"
)

// BadgerStorage implements Storage using BadgerDB.
type BadgerStorage struct {
	db *badger.DB
}

// NewBadgerStorage opens or creates a BadgerDB at the given path.
func NewBadgerStorage(path string) (*BadgerStorage, error) {
	opts := badger.DefaultOptions(path).
		WithLogger(nil).
		// Memory optimization: reduce memtable count and size
		WithNumMemtables(2).             // Default: 5 → 2 (saves ~192MB)
		WithMemTableSize(32 << 20).      // Default: 64MB → 32MB
		WithValueLogFileSize(64 << 20).  // Default: 1GB → 64MB
		WithNumLevelZeroTables(2).       // Faster L0 compaction
		WithNumLevelZeroTablesStall(4).  // Stall writes earlier
		WithBlockCacheSize(32 << 20).    // 32MB block cache (default: 256MB)
		WithIndexCacheSize(16 << 20).    // 16MB index cache
		WithCompression(0).              // No compression (CPU tradeoff)
		WithSyncWrites(true).            // Sync writes for durability
		// Value log optimization: store small values in LSM tree
		WithValueThreshold(1 << 10).     // Values < 1KB in LSM (faster reads)
		WithNumCompactors(2).            // Fewer compactors to save resources (default: 4)
		WithNumVersionsToKeep(1)         // Only keep latest version
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerStorage{db: db}, nil
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

// Close closes the database.
func (s *BadgerStorage) Close() error {
	return s.db.Close()
}
