package protocol

import (
	"bytes"
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
