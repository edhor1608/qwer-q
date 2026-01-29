package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeFrame(t *testing.T) {
	payload := []byte("hello")
	frame := EncodeFrame(OpPublish, payload)

	// Length = 1 (version) + 1 (opcode) + 5 (payload) = 7
	// Frame = 4 (length) + 7 = 11 bytes
	if len(frame) != 11 {
		t.Errorf("frame length = %d, want 11", len(frame))
	}

	// Check length field (big-endian)
	length := uint32(frame[0])<<24 | uint32(frame[1])<<16 | uint32(frame[2])<<8 | uint32(frame[3])
	if length != 7 {
		t.Errorf("length field = %d, want 7", length)
	}

	// Check version
	if frame[4] != ProtocolVersion {
		t.Errorf("version = %d, want %d", frame[4], ProtocolVersion)
	}

	// Check opcode
	if OpCode(frame[5]) != OpPublish {
		t.Errorf("opcode = %x, want %x", frame[5], OpPublish)
	}

	// Check payload
	if !bytes.Equal(frame[6:], payload) {
		t.Errorf("payload = %v, want %v", frame[6:], payload)
	}
}

func TestDecodeFrame(t *testing.T) {
	original := []byte("test payload")
	encoded := EncodeFrame(OpMessage, original)

	reader := bytes.NewReader(encoded)
	frame, err := DecodeFrame(reader)
	if err != nil {
		t.Fatalf("DecodeFrame error: %v", err)
	}

	if frame.Version != ProtocolVersion {
		t.Errorf("version = %d, want %d", frame.Version, ProtocolVersion)
	}
	if frame.OpCode != OpMessage {
		t.Errorf("opcode = %x, want %x", frame.OpCode, OpMessage)
	}
	if !bytes.Equal(frame.Payload, original) {
		t.Errorf("payload = %v, want %v", frame.Payload, original)
	}
}

func TestDecodeFrameMaxSize(t *testing.T) {
	// Create a frame claiming to be too large
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, MaxFrameSize+1)

	reader := bytes.NewReader(buf)
	_, err := DecodeFrame(reader)
	if err != ErrFrameTooLarge {
		t.Errorf("error = %v, want ErrFrameTooLarge", err)
	}
}
