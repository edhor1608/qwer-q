package storage

import (
	"encoding/binary"
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
	if len(data) == 0 || data[0] != messageEncodingV2 {
		t.Fatalf("expected v2 binary message encoding")
	}

	decoded := Message{ID: msg.ID, Queue: msg.Queue}
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

func TestDecodeMessage_V1Compatibility(t *testing.T) {
	msg := &Message{
		ID:          "compat-id",
		Queue:       "compat-q",
		Payload:     []byte("payload"),
		Headers:     map[string]string{"x-test": "1"},
		Attempt:     2,
		PublishedAt: time.Unix(0, 111),
		VisibleAt:   time.Unix(0, 222),
		OrderingKey: "key",
		Sequence:    9,
	}

	data := []byte{messageEncodingV1}
	data = appendString(data, msg.ID)
	data = appendString(data, msg.Queue)
	data = appendBytes(data, msg.Payload)
	data = binary.AppendUvarint(data, uint64(len(msg.Headers)))
	for k, v := range msg.Headers {
		data = appendString(data, k)
		data = appendString(data, v)
	}
	data = binary.AppendUvarint(data, uint64(msg.Attempt))
	data = binary.AppendVarint(data, msg.PublishedAt.UnixNano())
	data = binary.AppendVarint(data, msg.VisibleAt.UnixNano())
	data = appendString(data, msg.OrderingKey)
	data = binary.AppendUvarint(data, msg.Sequence)

	var decoded Message
	if err := decodeMessage(data, &decoded); err != nil {
		t.Fatalf("decodeMessage v1 failed: %v", err)
	}
	if decoded.ID != msg.ID || decoded.Queue != msg.Queue || decoded.Attempt != msg.Attempt || decoded.OrderingKey != msg.OrderingKey || decoded.Sequence != msg.Sequence {
		t.Fatalf("v1 decode mismatch: %#v", decoded)
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
