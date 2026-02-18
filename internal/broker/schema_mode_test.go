package broker

import (
	"net"
	"testing"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func startSchemaModeServer(t *testing.T, mode SchemaMode) string {
	t.Helper()

	b := NewBroker(WithMemoryLimit(0))
	t.Cleanup(b.Close)

	srv := NewServer(b)
	srv.SetSchemaMode(mode)
	go srv.ListenAndServe("127.0.0.1:0")
	t.Cleanup(func() { srv.Close() })

	srv.WaitReady()
	return srv.Addr().String()
}

func publishFrame(t *testing.T, conn net.Conn, queue string, payload []byte) *protocol.Frame {
	t.Helper()
	req := &protocol.PublishRequest{Queue: queue, Payload: payload}
	data, _ := proto.Marshal(req)
	if _, err := conn.Write(protocol.EncodeFrame(protocol.OpPublish, data)); err != nil {
		t.Fatalf("write publish: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	return frame
}

func registerSchema(t *testing.T, conn net.Conn, queue string, descriptor []byte, messageType string) {
	t.Helper()

	req := &protocol.SchemaRegisterRequest{
		Queue:       queue,
		Descriptor_: descriptor,
		MessageType: messageType,
	}
	data, _ := proto.Marshal(req)
	if _, err := conn.Write(protocol.EncodeFrame(protocol.OpSchemaRegister, data)); err != nil {
		t.Fatalf("write schema register: %v", err)
	}

	frame, err := protocol.DecodeFrame(conn)
	if err != nil {
		t.Fatalf("decode schema register response: %v", err)
	}
	if frame.OpCode != protocol.OpSchemaResponse {
		t.Fatalf("expected OpSchemaResponse, got %v", frame.OpCode)
	}
}

func testDescriptor(t *testing.T) []byte {
	t.Helper()

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("test"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Event"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:   proto.String("id"),
								Number: proto.Int32(1),
								Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
								Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
							},
						},
					},
				},
				Syntax: proto.String("proto3"),
			},
		},
	}
	data, err := proto.Marshal(fds)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	return data
}

func TestSchemaModePermissiveAllowsPublishWithoutSchema(t *testing.T) {
	addr := startSchemaModeServer(t, SchemaModePermissive)
	conn := dialTest(t, addr)

	frame := publishFrame(t, conn, "no-schema", []byte("hello"))
	if frame.OpCode != protocol.OpPublishAck {
		t.Fatalf("expected OpPublishAck, got %v", frame.OpCode)
	}
}

func TestSchemaModeStrictRejectsPublishWithoutSchema(t *testing.T) {
	addr := startSchemaModeServer(t, SchemaModeStrict)
	conn := dialTest(t, addr)

	frame := publishFrame(t, conn, "no-schema", []byte("hello"))
	if frame.OpCode != protocol.OpError {
		t.Fatalf("expected OpError, got %v", frame.OpCode)
	}

	var errResp protocol.ErrorResponse
	if err := proto.Unmarshal(frame.Payload, &errResp); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if errResp.Code != 7 {
		t.Fatalf("expected schema error code 7, got %d", errResp.Code)
	}
}

func TestSchemaModeStrictValidatesWhenSchemaRegistered(t *testing.T) {
	addr := startSchemaModeServer(t, SchemaModeStrict)
	conn := dialTest(t, addr)

	registerSchema(t, conn, "strict-queue", testDescriptor(t), "test.Event")

	validPayload := []byte{0x08, 0x2a}
	frame := publishFrame(t, conn, "strict-queue", validPayload)
	if frame.OpCode != protocol.OpPublishAck {
		t.Fatalf("expected OpPublishAck for valid payload, got %v", frame.OpCode)
	}

	invalidPayload := []byte{0x08, 0x80}
	frame = publishFrame(t, conn, "strict-queue", invalidPayload)
	if frame.OpCode != protocol.OpError {
		t.Fatalf("expected OpError for invalid payload, got %v", frame.OpCode)
	}

	var errResp protocol.ErrorResponse
	if err := proto.Unmarshal(frame.Payload, &errResp); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if errResp.Code != 5 {
		t.Fatalf("expected schema validation error code 5, got %d", errResp.Code)
	}
}
