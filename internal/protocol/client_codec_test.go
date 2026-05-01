package protocol

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestEncodeSimplePublishRequestPayload(t *testing.T) {
	payload := EncodeSimplePublishRequestPayload("orders", []byte("hello"))
	var req PublishRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		t.Fatalf("proto.Unmarshal failed: %v", err)
	}
	if req.Queue != "orders" {
		t.Fatalf("queue = %q, want %q", req.Queue, "orders")
	}
	if !bytes.Equal(req.Payload, []byte("hello")) {
		t.Fatal("payload mismatch")
	}
}

func TestEncodeSimpleConsumeRequestPayload(t *testing.T) {
	payload := EncodeSimpleConsumeRequestPayload("jobs", 100, 7)
	var req ConsumeRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		t.Fatalf("proto.Unmarshal failed: %v", err)
	}
	if req.Queue != "jobs" {
		t.Fatalf("queue = %q, want %q", req.Queue, "jobs")
	}
	if req.Prefetch != 100 {
		t.Fatalf("prefetch = %d, want %d", req.Prefetch, 100)
	}
	if req.VisibilityTimeout != 7 {
		t.Fatalf("visibility timeout = %d, want %d", req.VisibilityTimeout, 7)
	}
}

func TestEncodeAckRequestPayload(t *testing.T) {
	payload := EncodeAckRequestPayload("01KP42FSV459XZQ2Q3XZH7QNP9")
	var req AckRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		t.Fatalf("proto.Unmarshal failed: %v", err)
	}
	if req.MessageId != "01KP42FSV459XZQ2Q3XZH7QNP9" {
		t.Fatalf("message id = %q", req.MessageId)
	}
}

func BenchmarkEncodeSimplePublishRequestPayload(b *testing.B) {
	payload := make([]byte, 1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodeSimplePublishRequestPayload("bench-throughput", payload)
	}
}

func BenchmarkProtoMarshalSimplePublishRequest(b *testing.B) {
	req := &PublishRequest{Queue: "bench-throughput", Payload: make([]byte, 1024)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := proto.Marshal(req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeAckRequestPayload(b *testing.B) {
	messageID := "01KP42FSV459XZQ2Q3XZH7QNP9"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodeAckRequestPayload(messageID)
	}
}

func BenchmarkProtoMarshalAckRequest(b *testing.B) {
	req := &AckRequest{MessageId: "01KP42FSV459XZQ2Q3XZH7QNP9"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := proto.Marshal(req)
		if err != nil {
			b.Fatal(err)
		}
	}
}
