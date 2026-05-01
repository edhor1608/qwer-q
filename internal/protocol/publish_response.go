package protocol

import "encoding/binary"

const publishResponseMessageIDField = 0x0a

// EncodePublishResponsePayload encodes the PublishResponse payload directly.
func EncodePublishResponsePayload(messageID string) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(messageID))
	buf = append(buf, publishResponseMessageIDField)
	buf = binary.AppendUvarint(buf, uint64(len(messageID)))
	buf = append(buf, messageID...)
	return buf
}

// DecodePublishResponsePayload decodes the PublishResponse payload directly.
// It returns ok=false when the payload does not match the expected fast path.
func DecodePublishResponsePayload(payload []byte) (messageID string, ok bool) {
	if len(payload) == 0 {
		return "", true
	}
	if payload[0] != publishResponseMessageIDField {
		return "", false
	}
	length, n := binary.Uvarint(payload[1:])
	if n <= 0 {
		return "", false
	}
	start := 1 + n
	end := start + int(length)
	if end > len(payload) {
		return "", false
	}
	if end != len(payload) {
		return "", false
	}
	return string(payload[start:end]), true
}
