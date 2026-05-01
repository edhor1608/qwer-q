package protocol

import "encoding/binary"

const (
	fieldQueueString     = 0x0a
	fieldPayloadBytes    = 0x12
	fieldPrefetchUint32  = 0x10
	fieldMessageIDString = 0x0a
)

// EncodeSimplePublishRequestPayload encodes PublishRequest for the simple client path.
func EncodeSimplePublishRequestPayload(queue string, payload []byte) []byte {
	buf := make([]byte, 0, 2+binary.MaxVarintLen64+len(queue)+binary.MaxVarintLen64+len(payload))
	buf = append(buf, fieldQueueString)
	buf = binary.AppendUvarint(buf, uint64(len(queue)))
	buf = append(buf, queue...)
	buf = append(buf, fieldPayloadBytes)
	buf = binary.AppendUvarint(buf, uint64(len(payload)))
	buf = append(buf, payload...)
	return buf
}

// EncodeSimpleConsumeRequestPayload encodes ConsumeRequest for the simple client path.
func EncodeSimpleConsumeRequestPayload(queue string, prefetch, visibilityTimeout uint32) []byte {
	buf := make([]byte, 0, 4+binary.MaxVarintLen64+len(queue)+4)
	buf = append(buf, fieldQueueString)
	buf = binary.AppendUvarint(buf, uint64(len(queue)))
	buf = append(buf, queue...)
	if prefetch != 0 {
		buf = append(buf, fieldPrefetchUint32)
		buf = binary.AppendUvarint(buf, uint64(prefetch))
	}
	if visibilityTimeout != 0 {
		buf = append(buf, 0x18)
		buf = binary.AppendUvarint(buf, uint64(visibilityTimeout))
	}
	return buf
}

// EncodeAckRequestPayload encodes AckRequest for the simple client path.
func EncodeAckRequestPayload(messageID string) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(messageID))
	buf = append(buf, fieldMessageIDString)
	buf = binary.AppendUvarint(buf, uint64(len(messageID)))
	buf = append(buf, messageID...)
	return buf
}
