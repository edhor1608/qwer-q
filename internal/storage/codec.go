package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

const (
	messageEncodingV1 byte = 1
	messageEncodingV2 byte = 2
)

func encodeMessage(msg *Message) []byte {
	buf := make([]byte, 0, encodedMessageSizeV2(msg))
	buf = append(buf, messageEncodingV2)
	buf = appendBytes(buf, msg.Payload)
	buf = binary.AppendUvarint(buf, uint64(len(msg.Headers)))
	for k, v := range msg.Headers {
		buf = appendString(buf, k)
		buf = appendString(buf, v)
	}
	buf = binary.AppendUvarint(buf, uint64(msg.Attempt))
	buf = binary.AppendVarint(buf, msg.PublishedAt.UnixNano())
	buf = binary.AppendVarint(buf, msg.VisibleAt.UnixNano())
	buf = appendString(buf, msg.OrderingKey)
	buf = binary.AppendUvarint(buf, msg.Sequence)
	return buf
}

func encodedMessageSizeV2(msg *Message) int {
	n := 1 + uvarintSize(uint64(len(msg.Payload))) + len(msg.Payload)
	n += uvarintSize(uint64(len(msg.Headers)))
	for k, v := range msg.Headers {
		n += uvarintSize(uint64(len(k))) + len(k)
		n += uvarintSize(uint64(len(v))) + len(v)
	}
	n += uvarintSize(uint64(msg.Attempt))
	n += varintSize(msg.PublishedAt.UnixNano())
	n += varintSize(msg.VisibleAt.UnixNano())
	n += uvarintSize(uint64(len(msg.OrderingKey))) + len(msg.OrderingKey)
	n += uvarintSize(msg.Sequence)
	return n
}

func decodeMessage(data []byte, msg *Message) error {
	if len(data) == 0 {
		return fmt.Errorf("decode message: empty payload")
	}

	switch data[0] {
	case messageEncodingV1:
		return decodeMessageV1(data[1:], msg)
	case messageEncodingV2:
		return decodeMessageV2(data[1:], msg)
	default:
		return json.Unmarshal(data, msg)
	}
}

func decodeMessageV1(buf []byte, msg *Message) error {
	var err error
	if msg.ID, buf, err = readString(buf); err != nil {
		return err
	}
	if msg.Queue, buf, err = readString(buf); err != nil {
		return err
	}
	return decodeMessageFields(buf, msg)
}

func decodeMessageV2(buf []byte, msg *Message) error {
	return decodeMessageFields(buf, msg)
}

func decodeMessageFields(buf []byte, msg *Message) error {
	var err error
	if msg.Payload, buf, err = readBytes(buf); err != nil {
		return err
	}

	headerCount, buf, err := readUvarint(buf)
	if err != nil {
		return err
	}
	if headerCount > 0 {
		msg.Headers = make(map[string]string, int(headerCount))
		for i := uint64(0); i < headerCount; i++ {
			var k, v string
			if k, buf, err = readString(buf); err != nil {
				return err
			}
			if v, buf, err = readString(buf); err != nil {
				return err
			}
			msg.Headers[k] = v
		}
	} else {
		msg.Headers = nil
	}

	attempt, buf, err := readUvarint(buf)
	if err != nil {
		return err
	}
	msg.Attempt = uint32(attempt)

	publishedAt, buf, err := readVarint(buf)
	if err != nil {
		return err
	}
	msg.PublishedAt = time.Unix(0, publishedAt)

	visibleAt, buf, err := readVarint(buf)
	if err != nil {
		return err
	}
	msg.VisibleAt = time.Unix(0, visibleAt)

	if msg.OrderingKey, buf, err = readString(buf); err != nil {
		return err
	}
	msg.Sequence, _, err = readUvarint(buf)
	if err != nil {
		return err
	}
	return nil
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	dst = append(dst, s...)
	return dst
}

func appendBytes(dst, data []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(data)))
	dst = append(dst, data...)
	return dst
}

func uvarintSize(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

func varintSize(v int64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutVarint(buf[:], v)
}

func readUvarint(data []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("decode message: invalid uvarint")
	}
	return v, data[n:], nil
}

func readVarint(data []byte) (int64, []byte, error) {
	v, n := binary.Varint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("decode message: invalid varint")
	}
	return v, data[n:], nil
}

func readString(data []byte) (string, []byte, error) {
	raw, rest, err := readBytes(data)
	if err != nil {
		return "", nil, err
	}
	return string(raw), rest, nil
}

func readBytes(data []byte) ([]byte, []byte, error) {
	length, rest, err := readUvarint(data)
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(rest)) < length {
		return nil, nil, fmt.Errorf("decode message: truncated payload")
	}
	value := append([]byte(nil), rest[:length]...)
	return value, rest[length:], nil
}
