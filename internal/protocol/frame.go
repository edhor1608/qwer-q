package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// MaxFrameSize is the maximum allowed frame size (16 MB).
const MaxFrameSize = 16 * 1024 * 1024

// DefaultMaxMessageSize is the default maximum message payload size (1 MB).
// This matches NATS/Kafka defaults and is safe for 512MB containers.
const DefaultMaxMessageSize uint32 = 1 * 1024 * 1024

// maxMessageSize is the configured maximum message size (atomic for thread safety).
// Use SetMaxMessageSize to change it.
var maxMessageSize atomic.Uint32

func init() {
	maxMessageSize.Store(DefaultMaxMessageSize)
}

// SetMaxMessageSize configures the maximum allowed message payload size.
// Messages larger than this are rejected before allocation (zero-cost).
func SetMaxMessageSize(size uint32) {
	maxMessageSize.Store(size)
}

// GetMaxMessageSize returns the current maximum message size.
func GetMaxMessageSize() uint32 {
	return maxMessageSize.Load()
}

var (
	ErrFrameTooLarge   = errors.New("frame exceeds maximum size")
	ErrMessageTooLarge = errors.New("message exceeds maximum size")
	ErrFrameTooSmall   = errors.New("frame too small for header")
)

// Buffer pools for common frame sizes to reduce allocations.
var (
	smallBufPool  = sync.Pool{New: func() any { return make([]byte, 1024) }}
	medBufPool    = sync.Pool{New: func() any { return make([]byte, 16*1024) }}
	largeBufPool  = sync.Pool{New: func() any { return make([]byte, 64*1024) }}
	xlargeBufPool = sync.Pool{New: func() any { return make([]byte, 256*1024) }}
	lengthPool    = sync.Pool{New: func() any { return make([]byte, 4) }}
)

func getBuffer(size int) []byte {
	switch {
	case size <= 1024:
		buf := smallBufPool.Get().([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
		smallBufPool.Put(buf)
	case size <= 16*1024:
		buf := medBufPool.Get().([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
		medBufPool.Put(buf)
	case size <= 64*1024:
		buf := largeBufPool.Get().([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
		largeBufPool.Put(buf)
	case size <= 256*1024:
		buf := xlargeBufPool.Get().([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
		xlargeBufPool.Put(buf)
	}
	return make([]byte, size)
}

// PutBuffer returns a buffer to the pool (call after done with frame payload).
func PutBuffer(buf []byte) {
	size := cap(buf)
	switch {
	case size <= 1024:
		smallBufPool.Put(buf[:size])
	case size <= 16*1024:
		medBufPool.Put(buf[:size])
	case size <= 64*1024:
		largeBufPool.Put(buf[:size])
	case size <= 256*1024:
		xlargeBufPool.Put(buf[:size])
	}
}

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
// The returned Frame's Payload uses a pooled buffer - call PutBuffer(frame.Payload)
// when done processing if you want to return it to the pool.
func DecodeFrame(r io.Reader) (*Frame, error) {
	// Read length using pooled buffer
	lengthBuf := lengthPool.Get().([]byte)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		lengthPool.Put(lengthBuf)
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf)
	lengthPool.Put(lengthBuf)

	if length > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if length < 2 {
		return nil, ErrFrameTooSmall
	}

	// Pre-allocation size check: reject oversized messages before allocating buffer.
	// Payload size = length - 2 (version + opcode headers)
	payloadSize := length - 2
	if payloadSize > GetMaxMessageSize() {
		return nil, ErrMessageTooLarge
	}

	// Read rest of frame using pooled buffer
	data := getBuffer(int(length))
	if _, err := io.ReadFull(r, data); err != nil {
		PutBuffer(data)
		return nil, err
	}

	return &Frame{
		Version: data[0],
		OpCode:  OpCode(data[1]),
		Payload: data[2:],
	}, nil
}
