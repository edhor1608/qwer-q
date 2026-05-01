package protocol

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestPublishResponsePayloadRoundTrip(t *testing.T) {
	messageID := "01KP42FSV459XZQ2Q3XZH7QNP9"
	payload := EncodePublishResponsePayload(messageID)

	decoded, ok := DecodePublishResponsePayload(payload)
	if !ok {
		t.Fatal("expected fast-path decode to succeed")
	}
	if decoded != messageID {
		t.Fatalf("message id = %q, want %q", decoded, messageID)
	}

	var resp PublishResponse
	if err := proto.Unmarshal(payload, &resp); err != nil {
		t.Fatalf("proto.Unmarshal failed: %v", err)
	}
	if resp.MessageId != messageID {
		t.Fatalf("protobuf message id = %q, want %q", resp.MessageId, messageID)
	}
}

func TestDecodePublishResponsePayloadFallback(t *testing.T) {
	payload, err := proto.Marshal(&PublishResponse{MessageId: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := DecodePublishResponsePayload(append(payload, 0x10, 0x01)); ok {
		t.Fatal("expected fast-path decode to reject payload with extra fields")
	}
}

func BenchmarkEncodePublishResponsePayload(b *testing.B) {
	messageID := "01KP42FSV459XZQ2Q3XZH7QNP9"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodePublishResponsePayload(messageID)
	}
}

func BenchmarkProtoMarshalPublishResponse(b *testing.B) {
	resp := &PublishResponse{MessageId: "01KP42FSV459XZQ2Q3XZH7QNP9"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := proto.Marshal(resp)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodePublishResponsePayload(b *testing.B) {
	payload := EncodePublishResponsePayload("01KP42FSV459XZQ2Q3XZH7QNP9")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, ok := DecodePublishResponsePayload(payload)
		if !ok {
			b.Fatal("decode failed")
		}
	}
}

func BenchmarkProtoUnmarshalPublishResponse(b *testing.B) {
	payload, err := proto.Marshal(&PublishResponse{MessageId: "01KP42FSV459XZQ2Q3XZH7QNP9"})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var resp PublishResponse
		if err := proto.Unmarshal(payload, &resp); err != nil {
			b.Fatal(err)
		}
	}
}
