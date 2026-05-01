package storage

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEncodeDecodeMessageRoundTrip(t *testing.T) {
	msg := &Message{
		ID:          "msg-001",
		Queue:       "orders",
		Payload:     []byte("hello"),
		Headers:     map[string]string{"x-a": "1", "x-b": "2"},
		Attempt:     3,
		PublishedAt: time.Unix(0, 123456789),
		VisibleAt:   time.Unix(0, 987654321),
		OrderingKey: "customer-42",
		Sequence:    17,
	}

	data := encodeMessage(msg)
	if len(data) == 0 || data[0] != messageEncodingV1 {
		t.Fatalf("expected binary message encoding")
	}

	var decoded Message
	if err := decodeMessage(data, &decoded); err != nil {
		t.Fatalf("decodeMessage failed: %v", err)
	}

	if decoded.ID != msg.ID || decoded.Queue != msg.Queue || decoded.Attempt != msg.Attempt || decoded.OrderingKey != msg.OrderingKey || decoded.Sequence != msg.Sequence {
		t.Fatalf("decoded metadata mismatch: %#v", decoded)
	}
	if string(decoded.Payload) != string(msg.Payload) {
		t.Fatalf("payload mismatch: got %q want %q", decoded.Payload, msg.Payload)
	}
	if len(decoded.Headers) != len(msg.Headers) || decoded.Headers["x-a"] != "1" || decoded.Headers["x-b"] != "2" {
		t.Fatalf("headers mismatch: %#v", decoded.Headers)
	}
	if !decoded.PublishedAt.Equal(msg.PublishedAt) || !decoded.VisibleAt.Equal(msg.VisibleAt) {
		t.Fatalf("timestamp mismatch: got %v/%v want %v/%v", decoded.PublishedAt, decoded.VisibleAt, msg.PublishedAt, msg.VisibleAt)
	}
}

func TestDecodeMessage_LegacyJSON(t *testing.T) {
	msg := &Message{ID: "legacy", Queue: "legacy-q", Payload: []byte("payload")}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	var decoded Message
	if err := decodeMessage(data, &decoded); err != nil {
		t.Fatalf("decodeMessage legacy json failed: %v", err)
	}
	if decoded.ID != msg.ID || decoded.Queue != msg.Queue || string(decoded.Payload) != string(msg.Payload) {
		t.Fatalf("legacy decode mismatch: %#v", decoded)
	}
}
