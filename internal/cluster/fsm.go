package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jonas/qwer-q/internal/broker"
)

// CommandType identifies the type of replicated command.
type CommandType uint8

const (
	CmdPublish        CommandType = 1
	CmdAck            CommandType = 2
	CmdNack           CommandType = 3
	CmdCreateQueue    CommandType = 4
	CmdDeleteQueue    CommandType = 5
	CmdSchemaRegister CommandType = 6
)

// Command is a replicated log entry applied to the FSM.
type Command struct {
	Type CommandType `json:"type"`
	Data json.RawMessage `json:"data"`
}

// PublishCommand replicates a message publish.
type PublishCommand struct {
	Queue       string            `json:"queue"`
	MessageID   string            `json:"message_id"`
	Payload     []byte            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	PublishedAt int64             `json:"published_at"` // UnixMilli
}

// AckCommand replicates a message ack.
type AckCommand struct {
	Queue     string `json:"queue"`
	MessageID string `json:"message_id"`
}

// NackCommand replicates a message nack.
type NackCommand struct {
	Queue     string `json:"queue"`
	MessageID string `json:"message_id"`
	Requeue   bool   `json:"requeue"`
}

// CreateQueueCommand replicates queue creation.
type CreateQueueCommand struct {
	Name string `json:"name"`
}

// SchemaRegisterCommand replicates schema registration.
type SchemaRegisterCommand struct {
	Queue       string `json:"queue"`
	Descriptor  []byte `json:"descriptor"`
	MessageType string `json:"message_type"`
}

// FSMResponse is returned by Apply to communicate results.
type FSMResponse struct {
	Error     error
	MessageID string // for publish responses
	Version   uint32 // for schema register responses
}

// FSM implements raft.FSM. It applies replicated commands to the local broker.
type FSM struct {
	mu     sync.Mutex
	broker *broker.Broker
	logger *slog.Logger
}

// NewFSM creates a new FSM backed by the given broker.
func NewFSM(b *broker.Broker, logger *slog.Logger) *FSM {
	return &FSM{
		broker: b,
		logger: logger,
	}
}

// Apply is called by Raft when a log entry is committed by a majority.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		f.logger.Error("failed to unmarshal raft command", "error", err)
		return &FSMResponse{Error: fmt.Errorf("unmarshal command: %w", err)}
	}

	switch cmd.Type {
	case CmdPublish:
		return f.applyPublish(cmd.Data)
	case CmdAck:
		return f.applyAck(cmd.Data)
	case CmdNack:
		return f.applyNack(cmd.Data)
	case CmdCreateQueue:
		return f.applyCreateQueue(cmd.Data)
	case CmdSchemaRegister:
		return f.applySchemaRegister(cmd.Data)
	default:
		return &FSMResponse{Error: fmt.Errorf("unknown command type: %d", cmd.Type)}
	}
}

func (f *FSM) applyPublish(data json.RawMessage) *FSMResponse {
	var cmd PublishCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return &FSMResponse{Error: err}
	}

	msg := &broker.Message{
		ID:          cmd.MessageID,
		Queue:       cmd.Queue,
		Payload:     cmd.Payload,
		Headers:     cmd.Headers,
		Attempt:     0,
		PublishedAt: time.UnixMilli(cmd.PublishedAt),
		VisibleAt:   time.UnixMilli(cmd.PublishedAt),
	}

	q := f.broker.GetOrCreateQueue(cmd.Queue)
	if err := q.Enqueue(msg); err != nil {
		return &FSMResponse{Error: err}
	}

	// Persist to storage if configured
	if f.broker.Storage() != nil {
		if err := f.broker.Storage().SaveMessage(msg); err != nil {
			f.logger.Error("failed to persist replicated message", "error", err)
		}
	}

	return &FSMResponse{MessageID: cmd.MessageID}
}

func (f *FSM) applyAck(data json.RawMessage) *FSMResponse {
	var cmd AckCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return &FSMResponse{Error: err}
	}

	q := f.broker.GetQueue(cmd.Queue)
	if q == nil {
		return &FSMResponse{Error: fmt.Errorf("queue not found: %s", cmd.Queue)}
	}
	q.Ack(cmd.MessageID)

	if f.broker.Storage() != nil {
		f.broker.Storage().DeleteMessage(cmd.MessageID)
	}

	return &FSMResponse{}
}

func (f *FSM) applyNack(data json.RawMessage) *FSMResponse {
	var cmd NackCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return &FSMResponse{Error: err}
	}

	q := f.broker.GetQueue(cmd.Queue)
	if q == nil {
		return &FSMResponse{Error: fmt.Errorf("queue not found: %s", cmd.Queue)}
	}
	q.Nack(cmd.MessageID, cmd.Requeue)

	return &FSMResponse{}
}

func (f *FSM) applyCreateQueue(data json.RawMessage) *FSMResponse {
	var cmd CreateQueueCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return &FSMResponse{Error: err}
	}

	f.broker.GetOrCreateQueue(cmd.Name)
	return &FSMResponse{}
}

func (f *FSM) applySchemaRegister(data json.RawMessage) *FSMResponse {
	var cmd SchemaRegisterCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return &FSMResponse{Error: err}
	}
	// Schema registration is applied but the actual registry is managed at the
	// Server level, not the Broker level. For now we just ensure the queue exists.
	f.broker.GetOrCreateQueue(cmd.Queue)
	return &FSMResponse{}
}

// Snapshot is used to support log compaction. Returns an FSMSnapshot that
// can serialize the current state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Capture current queue names and their messages
	queues := f.broker.ListQueues()
	snapshot := &fsmSnapshot{
		Queues: make(map[string][]*snapshotMessage),
	}

	for _, name := range queues {
		q := f.broker.GetQueue(name)
		if q == nil {
			continue
		}
		// We snapshot the queue name even if empty
		snapshot.Queues[name] = nil
	}

	// If storage is available, snapshot from storage (more complete)
	if f.broker.Storage() != nil {
		for _, name := range queues {
			msgs, err := f.broker.Storage().LoadMessages(name)
			if err != nil {
				f.logger.Error("snapshot: failed to load messages", "queue", name, "error", err)
				continue
			}
			for _, msg := range msgs {
				snapshot.Queues[name] = append(snapshot.Queues[name], &snapshotMessage{
					ID:          msg.ID,
					Queue:       msg.Queue,
					Payload:     msg.Payload,
					Headers:     msg.Headers,
					Attempt:     msg.Attempt,
					PublishedAt: msg.PublishedAt.UnixMilli(),
					VisibleAt:   msg.VisibleAt.UnixMilli(),
				})
			}
		}
	}

	return snapshot, nil
}

// Restore replaces the broker state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	// Re-create queues and enqueue messages
	for name, msgs := range snapshot.Queues {
		q := f.broker.GetOrCreateQueue(name)
		for _, sm := range msgs {
			msg := &broker.Message{
				ID:          sm.ID,
				Queue:       sm.Queue,
				Payload:     sm.Payload,
				Headers:     sm.Headers,
				Attempt:     sm.Attempt,
				PublishedAt: time.UnixMilli(sm.PublishedAt),
				VisibleAt:   time.UnixMilli(sm.VisibleAt),
			}
			q.EnqueueDirect(msg)
			if f.broker.Storage() != nil {
				f.broker.Storage().SaveMessage(msg)
			}
		}
	}

	return nil
}

// snapshotMessage is the serializable form of a message for snapshots.
type snapshotMessage struct {
	ID          string            `json:"id"`
	Queue       string            `json:"queue"`
	Payload     []byte            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attempt     uint32            `json:"attempt"`
	PublishedAt int64             `json:"published_at"`
	VisibleAt   int64             `json:"visible_at"`
}

// fsmSnapshot implements raft.FSMSnapshot.
type fsmSnapshot struct {
	Queues map[string][]*snapshotMessage `json:"queues"`
}

// Persist writes the snapshot to the given sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release is called when the snapshot is no longer needed.
func (s *fsmSnapshot) Release() {}
