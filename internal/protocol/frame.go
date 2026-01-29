package protocol

import "encoding/binary"

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
