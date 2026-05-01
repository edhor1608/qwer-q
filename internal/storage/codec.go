package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

const messageEncodingV1 byte = 1

func encodeMessage(msg *Message) []byte {
	buf := make([]byte, 0, 1+len(msg.ID)+len(msg.Queue)+len(msg.Payload)+len(msg.OrderingKey)+64)
	buf = append(buf, messageEncodingV1)
	buf = appendString(buf, msg.ID)
	buf = appendString(buf, msg.Queue)
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

func decodeMessage(data []byte, msg *Message) error {
	if len(data) == 0 {
		return fmt.Errorf("decode message: empty payload")
	}
	if data[0] != messageEncodingV1 {
		return json.Unmarshal(data, msg)
	}

	var err error
	buf := data[1:]
	if msg.ID, buf, err = readString(buf); err != nil {
		return err
	}
	if msg.Queue, buf, err = readString(buf); err != nil {
		return err
	}
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
