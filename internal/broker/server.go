package broker

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/jonas/qwer-q/internal/protocol"
	"github.com/jonas/qwer-q/internal/schema"
	"google.golang.org/protobuf/proto"
)

// Server is a TCP server for the broker.
type Server struct {
	broker   *Broker
	registry *schema.Registry
	listener net.Listener
	wg       sync.WaitGroup
	done     chan struct{}
}

// NewServer creates a new server with the given broker.
func NewServer(broker *Broker) *Server {
	return &Server{
		broker:   broker,
		registry: schema.NewRegistry(),
		done:     make(chan struct{}),
	}
}

// Registry returns the schema registry.
func (s *Server) Registry() *schema.Registry {
	return s.registry
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
	queueName   string
	clientAddr  string
	msgCh       <-chan *Message
	stopCh      chan struct{}
	deliverWg   sync.WaitGroup
	writeMu     sync.Mutex
	callManager *CallManager
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	LogConnect(clientAddr)

	state := &connState{
		clientAddr: clientAddr,
		stopCh:     make(chan struct{}),
	}
	defer func() {
		LogDisconnect(clientAddr)
		close(state.stopCh)
		state.deliverWg.Wait()
		if state.msgCh != nil {
			q := s.broker.GetQueue(state.queueName)
			if q != nil {
				q.RemoveConsumer(state.msgCh)
			}
		}
		if state.callManager != nil {
			state.callManager.Close()
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
		return s.handlePublish(frame.Payload, state.clientAddr)
	case protocol.OpConsume:
		return s.handleConsume(frame.Payload, state, conn)
	case protocol.OpAck:
		return s.handleAck(frame.Payload, state)
	case protocol.OpNack:
		return s.handleNack(frame.Payload, state)
	case protocol.OpExtendVisibility:
		return s.handleExtendVisibility(frame.Payload, state)
	case protocol.OpSchemaRegister:
		return s.handleSchemaRegister(frame.Payload)
	case protocol.OpSchemaGet:
		return s.handleSchemaGet(frame.Payload)
	case protocol.OpSchemaList:
		return s.handleSchemaList()
	case protocol.OpQueueList:
		return s.handleQueueList()
	case protocol.OpCall:
		return s.handleCall(frame.Payload, state)
	default:
		return EncodeError(1, "unknown opcode")
	}
}

func (s *Server) handlePublish(payload []byte, clientAddr string) []byte {
	start := time.Now()
	var req protocol.PublishRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	// DEC-013: Queue only exists if schema is registered
	// Validate message against schema
	if err := s.registry.Validate(req.GetQueue(), req.GetPayload()); err != nil {
		return EncodeError(5, "schema validation failed: "+err.Error())
	}

	resp, err := s.broker.HandlePublish(&req)
	if err != nil {
		LogError("publish failed", err, "queue", req.GetQueue(), "client", clientAddr)
		return EncodeError(3, err.Error())
	}

	LogPublish(req.GetQueue(), resp.MessageId, clientAddr)
	q := s.broker.GetQueue(req.GetQueue())
	RecordPublish(req.GetQueue(), time.Since(start).Seconds())
	if q != nil {
		UpdateQueueDepth(req.GetQueue(), q.Len())
		UpdateInFlightCount(req.GetQueue(), q.InFlightLen())
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
				LogConsume(state.queueName, msg.ID, state.clientAddr)
				RecordConsume(state.queueName)
				q := s.broker.GetQueue(state.queueName)
				if q != nil {
					UpdateQueueDepth(state.queueName, q.Len())
					UpdateInFlightCount(state.queueName, q.InFlightLen())
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

	LogAck(state.queueName, req.GetMessageId(), state.clientAddr)
	RecordAck(state.queueName)
	q := s.broker.GetQueue(state.queueName)
	if q != nil {
		UpdateQueueDepth(state.queueName, q.Len())
		UpdateInFlightCount(state.queueName, q.InFlightLen())
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

	LogNack(state.queueName, req.GetMessageId(), state.clientAddr, req.GetRequeue())
	RecordNack(state.queueName)
	q := s.broker.GetQueue(state.queueName)
	if q != nil {
		UpdateQueueDepth(state.queueName, q.Len())
		UpdateInFlightCount(state.queueName, q.InFlightLen())
	}
	return nil
}

func (s *Server) handleExtendVisibility(payload []byte, state *connState) []byte {
	var req protocol.ExtendVisibilityRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	newVisibleAt, ok := s.broker.HandleExtendVisibility(&req, state.queueName)
	if !ok {
		return EncodeError(4, "message not found")
	}

	resp := &protocol.ExtendVisibilityResponse{
		NewVisibleAt: newVisibleAt.UnixMilli(),
	}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpExtendVisibilityAck, data)
}

func (s *Server) handleSchemaRegister(payload []byte) []byte {
	var req protocol.SchemaRegisterRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	version, err := s.registry.Register(req.GetQueue(), req.GetDescriptor_(), req.GetMessageType())
	if err != nil {
		return EncodeError(6, "schema registration failed: "+err.Error())
	}

	resp := &protocol.SchemaRegisterResponse{
		SchemaId: 0, // Not used currently
		Version:  version,
	}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpSchemaResponse, data)
}

func (s *Server) handleSchemaGet(payload []byte) []byte {
	var req protocol.SchemaRegisterRequest // Reuse for queue name
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	sch, err := s.registry.Get(req.GetQueue())
	if err != nil {
		return EncodeError(7, "schema not found")
	}

	resp := &protocol.SchemaRegisterResponse{
		Version: sch.Version,
	}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpSchemaResponse, data)
}

func (s *Server) handleSchemaList() []byte {
	names := s.registry.List()
	schemas := make([]*protocol.SchemaInfo, 0, len(names))
	for _, name := range names {
		sch, err := s.registry.Get(name)
		if err != nil {
			continue
		}
		schemas = append(schemas, &protocol.SchemaInfo{
			Queue:       sch.Queue,
			MessageType: sch.MessageType,
			Version:     sch.Version,
		})
	}
	resp := &protocol.SchemaListResponse{Schemas: schemas}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpSchemaListResp, data)
}

func (s *Server) handleQueueList() []byte {
	names := s.broker.ListQueues()
	queues := make([]*protocol.QueueInfo, 0, len(names))
	for _, name := range names {
		q := s.broker.GetQueue(name)
		if q == nil {
			continue
		}
		queues = append(queues, &protocol.QueueInfo{
			Name:          name,
			MessageCount:  uint32(q.Len()),
			InFlightCount: uint32(q.InFlightLen()),
		})
	}
	resp := &protocol.QueueListResponse{Queues: queues}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpQueueListResp, data)
}

func (s *Server) handleCall(payload []byte, state *connState) []byte {
	var req protocol.CallRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	// Create call manager lazily
	if state.callManager == nil {
		state.callManager = NewCallManager(s.broker, state.clientAddr)
	}

	resp, err := state.callManager.Call(&req)
	if err != nil {
		if _, ok := err.(ErrCallTimeout); ok {
			return EncodeError(8, "call timeout")
		}
		return EncodeError(3, err.Error())
	}

	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpCallResponse, data)
}
