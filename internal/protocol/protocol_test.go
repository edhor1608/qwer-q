package protocol

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestPublishRoundtrip(t *testing.T) {
	// Create a publish request
	req := &PublishRequest{
		Queue:   "orders",
		Payload: []byte(`{"id": 123}`),
		Headers: map[string]string{"trace_id": "abc123"},
	}

	// Encode to protobuf
	payload, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	// Encode to frame
	frame := EncodeFrame(OpPublish, payload)

	// Decode frame
	decoded, err := DecodeFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify opcode
	if decoded.OpCode != OpPublish {
		t.Errorf("opcode = %v, want %v", decoded.OpCode, OpPublish)
	}

	// Decode protobuf
	var result PublishRequest
	if err := proto.Unmarshal(decoded.Payload, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	// Verify fields
	if result.Queue != req.Queue {
		t.Errorf("queue = %s, want %s", result.Queue, req.Queue)
	}
	if !bytes.Equal(result.Payload, req.Payload) {
		t.Errorf("payload mismatch")
	}
	if result.Headers["trace_id"] != "abc123" {
		t.Errorf("headers mismatch")
	}
}
