package schema

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// createTestDescriptor creates a valid FileDescriptorSet for testing.
func createTestDescriptor(msgName string) []byte {
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("test"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String(msgName),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:   proto.String("id"),
								Number: proto.Int32(1),
								Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
								Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
							},
							{
								Name:   proto.String("name"),
								Number: proto.Int32(2),
								Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
								Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
							},
						},
					},
				},
				Syntax: proto.String("proto3"),
			},
		},
	}
	data, _ := proto.Marshal(fds)
	return data
}

func TestRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	desc := createTestDescriptor("TestMessage")

	version, err := r.Register("test-queue", desc, "test.TestMessage")
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}
	if version != 1 {
		t.Fatalf("expected version 1, got %d", version)
	}

	schema, err := r.Get("test-queue")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if schema.Queue != "test-queue" {
		t.Fatalf("expected queue 'test-queue', got %s", schema.Queue)
	}
	if schema.Version != 1 {
		t.Fatalf("expected version 1, got %d", schema.Version)
	}
	if schema.MessageType != "test.TestMessage" {
		t.Fatalf("expected message type 'test.TestMessage', got %s", schema.MessageType)
	}
}

func TestGetNotFound(t *testing.T) {
	r := NewRegistry()
	_, err := r.Get("nonexistent")
	if err != ErrSchemaNotFound {
		t.Fatalf("expected ErrSchemaNotFound, got %v", err)
	}
}

func TestVersionIncrement(t *testing.T) {
	r := NewRegistry()
	desc := createTestDescriptor("TestMessage")

	v1, _ := r.Register("test-queue", desc, "test.TestMessage")
	if v1 != 1 {
		t.Fatalf("expected version 1, got %d", v1)
	}

	v2, _ := r.Register("test-queue", desc, "test.TestMessage")
	if v2 != 2 {
		t.Fatalf("expected version 2, got %d", v2)
	}

	v3, _ := r.Register("test-queue", desc, "test.TestMessage")
	if v3 != 3 {
		t.Fatalf("expected version 3, got %d", v3)
	}

	schema, _ := r.Get("test-queue")
	if schema.Version != 3 {
		t.Fatalf("expected stored version 3, got %d", schema.Version)
	}
}

func TestList(t *testing.T) {
	r := NewRegistry()
	desc := createTestDescriptor("TestMessage")

	r.Register("charlie", desc, "test.TestMessage")
	r.Register("alpha", desc, "test.TestMessage")
	r.Register("bravo", desc, "test.TestMessage")

	names := r.List()
	if len(names) != 3 {
		t.Fatalf("expected 3 schemas, got %d", len(names))
	}
	if names[0] != "alpha" || names[1] != "bravo" || names[2] != "charlie" {
		t.Fatalf("expected sorted names, got %v", names)
	}
}

func TestInvalidDescriptor(t *testing.T) {
	r := NewRegistry()
	_, err := r.Register("test-queue", []byte("invalid"), "test.TestMessage")
	if err != ErrInvalidDescriptor {
		t.Fatalf("expected ErrInvalidDescriptor, got %v", err)
	}
}

func TestMessageTypeNotFound(t *testing.T) {
	r := NewRegistry()
	desc := createTestDescriptor("TestMessage")

	_, err := r.Register("test-queue", desc, "test.WrongMessage")
	if err != ErrMessageTypeNotFound {
		t.Fatalf("expected ErrMessageTypeNotFound, got %v", err)
	}
}

func TestValidateValidMessage(t *testing.T) {
	r := NewRegistry()
	desc := createTestDescriptor("TestMessage")

	_, err := r.Register("test-queue", desc, "test.TestMessage")
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	// Empty message is valid in proto3
	if err := r.Validate("test-queue", []byte{}); err != nil {
		t.Fatalf("expected valid empty message, got error: %v", err)
	}

	// Valid payload: Field 1 (id) = 42, Field 2 (name) = "test"
	validPayload := []byte{0x08, 0x2a, 0x12, 0x04, 't', 'e', 's', 't'}
	if err := r.Validate("test-queue", validPayload); err != nil {
		t.Fatalf("expected valid message, got error: %v", err)
	}
}

func TestValidateInvalidMessage(t *testing.T) {
	r := NewRegistry()
	desc := createTestDescriptor("TestMessage")

	r.Register("test-queue", desc, "test.TestMessage")

	// Truncated varint - invalid protobuf
	invalidPayload := []byte{0x08, 0x80}
	err := r.Validate("test-queue", invalidPayload)
	if err == nil {
		t.Fatal("expected validation error for invalid message")
	}
}

func TestValidateNoSchema(t *testing.T) {
	r := NewRegistry()
	// No schema = permissive mode, validation passes
	err := r.Validate("nonexistent", []byte{})
	if err != nil {
		t.Fatalf("expected nil (permissive mode), got %v", err)
	}
}

func TestCheckCompatibility(t *testing.T) {
	r := NewRegistry()
	desc := createTestDescriptor("TestMessage")

	// No existing schema - should be compatible
	compatible, err := r.CheckCompatibility("test-queue", desc, "test.TestMessage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !compatible {
		t.Fatal("expected compatible for new schema")
	}

	r.Register("test-queue", desc, "test.TestMessage")

	// Update should be compatible
	compatible, err = r.CheckCompatibility("test-queue", desc, "test.TestMessage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !compatible {
		t.Fatal("expected compatible for update")
	}
}

func TestCheckCompatibilityInvalidDescriptor(t *testing.T) {
	r := NewRegistry()
	_, err := r.CheckCompatibility("test-queue", []byte("invalid"), "test.TestMessage")
	if err != ErrInvalidDescriptor {
		t.Fatalf("expected ErrInvalidDescriptor, got %v", err)
	}
}
