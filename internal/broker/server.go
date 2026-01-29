package broker

import (
	"io"
	"net"
	"sync"

	"github.com/jonas/qwer-q/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// Server is a TCP server for the broker.
type Server struct {
	broker   *Broker
	listener net.Listener
	wg       sync.WaitGroup
	done     chan struct{}
}

// NewServer creates a new server with the given broker.
func NewServer(broker *Broker) *Server {
	return &Server{
		broker: broker,
		done:   make(chan struct{}),
	}
}

// ListenAndServe starts the server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// Addr returns the server's listen address.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close stops the server.
func (s *Server) Close() error {
	close(s.done)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// connState tracks per-connection state.
type connState struct {
	queueName  string
	msgCh      <-chan *Message
	stopCh     chan struct{}
	deliverWg  sync.WaitGroup
	writeMu    sync.Mutex
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	state := &connState{
		stopCh: make(chan struct{}),
	}
	defer func() {
		close(state.stopCh)
		state.deliverWg.Wait()
		if state.msgCh != nil {
			q := s.broker.GetQueue(state.queueName)
			if q != nil {
				q.RemoveConsumer(state.msgCh)
			}
		}
	}()

	for {
		frame, err := protocol.DecodeFrame(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}

		resp := s.handleFrame(frame, state, conn)
		if resp != nil {
			state.writeMu.Lock()
			conn.Write(resp)
			state.writeMu.Unlock()
		}
	}
}

func (s *Server) handleFrame(frame *protocol.Frame, state *connState, conn net.Conn) []byte {
	switch frame.OpCode {
	case protocol.OpPublish:
		return s.handlePublish(frame.Payload)
	case protocol.OpConsume:
		return s.handleConsume(frame.Payload, state, conn)
	case protocol.OpAck:
		return s.handleAck(frame.Payload, state)
	case protocol.OpNack:
		return s.handleNack(frame.Payload, state)
	default:
		return EncodeError(1, "unknown opcode")
	}
}

func (s *Server) handlePublish(payload []byte) []byte {
	var req protocol.PublishRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	resp, err := s.broker.HandlePublish(&req)
	if err != nil {
		return EncodeError(3, err.Error())
	}

	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpPublishAck, data)
}

func (s *Server) handleConsume(payload []byte, state *connState, conn net.Conn) []byte {
	var req protocol.ConsumeRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	state.queueName = req.GetQueue()
	state.msgCh = s.broker.HandleConsume(&req)

	// Start delivery goroutine
	state.deliverWg.Add(1)
	go func() {
		defer state.deliverWg.Done()
		for {
			select {
			case msg, ok := <-state.msgCh:
				if !ok {
					return
				}
				protoMsg := MessageToProto(msg)
				data, _ := proto.Marshal(protoMsg)
				frame := protocol.EncodeFrame(protocol.OpMessage, data)

				state.writeMu.Lock()
				conn.Write(frame)
				state.writeMu.Unlock()
			case <-state.stopCh:
				return
			}
		}
	}()

	return nil // No immediate response
}

func (s *Server) handleAck(payload []byte, state *connState) []byte {
	var req protocol.AckRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	if !s.broker.HandleAck(&req, state.queueName) {
		return EncodeError(4, "message not found")
	}
	return nil
}

func (s *Server) handleNack(payload []byte, state *connState) []byte {
	var req protocol.NackRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	if !s.broker.HandleNack(&req, state.queueName) {
		return EncodeError(4, "message not found")
	}
	return nil
}
