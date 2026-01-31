package client

import (
	"net"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// Client is a simple client for the QWER-Q broker.
type Client struct {
	conn net.Conn
}

// Dial connects to a broker.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// SchemaRegister registers a schema for a queue.
func (c *Client) SchemaRegister(queue string, descriptor []byte, messageType string) (*protocol.SchemaRegisterResponse, error) {
	req := &protocol.SchemaRegisterRequest{
		Queue:       queue,
		Descriptor_: descriptor,
		MessageType: messageType,
	}
	data, _ := proto.Marshal(req)
	frame := protocol.EncodeFrame(protocol.OpSchemaRegister, data)
	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}

	resp, err := protocol.DecodeFrame(c.conn)
	if err != nil {
		return nil, err
	}
	if resp.OpCode == protocol.OpError {
		var errResp protocol.ErrorResponse
		proto.Unmarshal(resp.Payload, &errResp)
		return nil, &BrokerError{Code: errResp.Code, Message: errResp.Message}
	}

	var result protocol.SchemaRegisterResponse
	if err := proto.Unmarshal(resp.Payload, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// SchemaList lists all registered schemas.
func (c *Client) SchemaList() (*protocol.SchemaListResponse, error) {
	req := &protocol.SchemaListRequest{}
	data, _ := proto.Marshal(req)
	frame := protocol.EncodeFrame(protocol.OpSchemaList, data)
	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}

	resp, err := protocol.DecodeFrame(c.conn)
	if err != nil {
		return nil, err
	}
	if resp.OpCode == protocol.OpError {
		var errResp protocol.ErrorResponse
		proto.Unmarshal(resp.Payload, &errResp)
		return nil, &BrokerError{Code: errResp.Code, Message: errResp.Message}
	}

	var result protocol.SchemaListResponse
	if err := proto.Unmarshal(resp.Payload, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// QueueList lists all queues.
func (c *Client) QueueList() (*protocol.QueueListResponse, error) {
	req := &protocol.QueueListRequest{}
	data, _ := proto.Marshal(req)
	frame := protocol.EncodeFrame(protocol.OpQueueList, data)
	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}

	resp, err := protocol.DecodeFrame(c.conn)
	if err != nil {
		return nil, err
	}
	if resp.OpCode == protocol.OpError {
		var errResp protocol.ErrorResponse
		proto.Unmarshal(resp.Payload, &errResp)
		return nil, &BrokerError{Code: errResp.Code, Message: errResp.Message}
	}

	var result protocol.QueueListResponse
	if err := proto.Unmarshal(resp.Payload, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// BrokerError represents an error from the broker.
type BrokerError struct {
	Code    uint32
	Message string
}

func (e *BrokerError) Error() string {
	return e.Message
}

// Publish sends a message to a queue.
func (c *Client) Publish(queue string, payload []byte) (*protocol.PublishResponse, error) {
	req := &protocol.PublishRequest{
		Queue:   queue,
		Payload: payload,
	}
	data, _ := proto.Marshal(req)
	frame := protocol.EncodeFrame(protocol.OpPublish, data)
	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}

	resp, err := protocol.DecodeFrame(c.conn)
	if err != nil {
		return nil, err
	}
	if resp.OpCode == protocol.OpError {
		var errResp protocol.ErrorResponse
		proto.Unmarshal(resp.Payload, &errResp)
		return nil, &BrokerError{Code: errResp.Code, Message: errResp.Message}
	}

	var result protocol.PublishResponse
	if err := proto.Unmarshal(resp.Payload, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Consume starts consuming from a queue. Messages are delivered to the handler.
// This is a blocking call that reads messages until the context is cancelled or an error occurs.
func (c *Client) Consume(queue string, prefetch uint32, handler func(*protocol.Message) error) error {
	req := &protocol.ConsumeRequest{
		Queue:    queue,
		Prefetch: prefetch,
	}
	data, _ := proto.Marshal(req)
	frame := protocol.EncodeFrame(protocol.OpConsume, data)
	if _, err := c.conn.Write(frame); err != nil {
		return err
	}

	for {
		resp, err := protocol.DecodeFrame(c.conn)
		if err != nil {
			return err
		}
		if resp.OpCode == protocol.OpError {
			var errResp protocol.ErrorResponse
			proto.Unmarshal(resp.Payload, &errResp)
			return &BrokerError{Code: errResp.Code, Message: errResp.Message}
		}
		if resp.OpCode != protocol.OpMessage {
			continue
		}

		var msg protocol.Message
		if err := proto.Unmarshal(resp.Payload, &msg); err != nil {
			return err
		}
		if err := handler(&msg); err != nil {
			return err
		}
	}
}

// Ack acknowledges a message.
func (c *Client) Ack(messageID string) error {
	req := &protocol.AckRequest{MessageId: messageID}
	data, _ := proto.Marshal(req)
	frame := protocol.EncodeFrame(protocol.OpAck, data)
	_, err := c.conn.Write(frame)
	return err
}
