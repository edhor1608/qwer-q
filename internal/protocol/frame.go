package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// MaxFrameSize is the maximum allowed frame size (16 MB).
const MaxFrameSize = 16 * 1024 * 1024

var (
	ErrFrameTooLarge = errors.New("frame exceeds maximum size")
	ErrFrameTooSmall = errors.New("frame too small for header")
)

// Frame represents a decoded protocol frame.
type Frame struct {
	Version uint8
	OpCode  OpCode
	Payload []byte
}

// EncodeFrame creates a wire-format frame.
// Format: [4-byte length][1-byte version][1-byte opcode][payload]
func EncodeFrame(op OpCode, payload []byte) []byte {
	// Length = version (1) + opcode (1) + payload
	length := uint32(2 + len(payload))

	frame := make([]byte, 4+length)
	binary.BigEndian.PutUint32(frame[0:4], length)
	frame[4] = ProtocolVersion
	frame[5] = byte(op)
	copy(frame[6:], payload)

	return frame
}

// DecodeFrame reads a frame from the reader.
func DecodeFrame(r io.Reader) (*Frame, error) {
	// Read length
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf)

	if length > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if length < 2 {
		return nil, ErrFrameTooSmall
	}

	// Read rest of frame
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return &Frame{
		Version: data[0],
		OpCode:  OpCode(data[1]),
		Payload: data[2:],
	}, nil
}
