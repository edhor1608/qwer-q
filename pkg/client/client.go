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
