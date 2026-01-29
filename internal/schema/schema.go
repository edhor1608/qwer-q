package schema

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	ErrSchemaNotFound      = errors.New("schema not found")
	ErrInvalidDescriptor   = errors.New("invalid descriptor")
	ErrMessageTypeNotFound = errors.New("message type not found in descriptor")
	ErrValidationFailed    = errors.New("message validation failed")
)

// Schema represents a registered schema for a queue.
type Schema struct {
	Queue       string
	MessageType string
	Descriptor  []byte
	Version     uint32
	CreatedAt   time.Time
	msgDesc     protoreflect.MessageDescriptor // cached for validation
}

// Registry holds schemas for queues.
type Registry struct {
	mu      sync.RWMutex
	schemas map[string]*Schema
}

// NewRegistry creates a new schema registry.
func NewRegistry() *Registry {
	return &Registry{
		schemas: make(map[string]*Schema),
	}
}

// Register stores a schema for a queue. Returns version number.
func (r *Registry) Register(queue string, descriptor []byte, messageType string) (uint32, error) {
	// Parse and validate descriptor
	msgDesc, err := parseDescriptor(descriptor, messageType)
	if err != nil {
		return 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var version uint32 = 1
	if existing, ok := r.schemas[queue]; ok {
		version = existing.Version + 1
	}

	r.schemas[queue] = &Schema{
		Queue:       queue,
		MessageType: messageType,
		Descriptor:  descriptor,
		Version:     version,
		CreatedAt:   time.Now(),
		msgDesc:     msgDesc,
	}
	return version, nil
}

// Get retrieves a schema for a queue.
func (r *Registry) Get(queue string) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.schemas[queue]
	if !ok {
		return nil, ErrSchemaNotFound
	}
	return s, nil
}

// Validate validates a payload against a queue's schema using protoreflect.
// If no schema is registered for the queue, validation passes (permissive mode).
func (r *Registry) Validate(queue string, payload []byte) error {
	r.mu.RLock()
	schema, ok := r.schemas[queue]
	r.mu.RUnlock()
	if !ok {
		return nil // No schema = no validation
	}

	// Create dynamic message and unmarshal
	msg := dynamicpb.NewMessage(schema.msgDesc)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return fmt.Errorf("%w: %v", ErrValidationFailed, err)
	}
	return nil
}

// List returns all registered queue names sorted alphabetically.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.schemas))
	for name := range r.schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// CheckCompatibility checks if a new schema is backward compatible.
// For now: simple validation only. Full compatibility can be added later.
func (r *Registry) CheckCompatibility(queue string, descriptor []byte, messageType string) (bool, error) {
	_, err := parseDescriptor(descriptor, messageType)
	if err != nil {
		return false, err
	}
	// TODO: Add actual backward compatibility checking
	return true, nil
}

// parseDescriptor parses a FileDescriptorSet and finds the message type.
func parseDescriptor(data []byte, messageType string) (protoreflect.MessageDescriptor, error) {
	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(data, &fds); err != nil {
		return nil, ErrInvalidDescriptor
	}

	files, err := protodesc.NewFiles(&fds)
	if err != nil {
		return nil, ErrInvalidDescriptor
	}

	desc, err := files.FindDescriptorByName(protoreflect.FullName(messageType))
	if err != nil {
		return nil, ErrMessageTypeNotFound
	}

	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, ErrMessageTypeNotFound
	}

	return msgDesc, nil
}
