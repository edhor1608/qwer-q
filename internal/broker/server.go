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

// Replicator is implemented by cluster.Node to replicate writes via Raft.
// When nil, the server operates in single-node mode (no replication).
type Replicator interface {
	// IsLeader returns true if this node is the Raft leader.
	IsLeader() bool
	// ReplicatePublish replicates a publish through Raft consensus.
	// Returns the message ID assigned by the FSM.
	ReplicatePublish(queue, messageID string, payload []byte, headers map[string]string, publishedAt time.Time) (string, error)
	// ReplicateAck replicates an ack through Raft consensus.
	ReplicateAck(queue, messageID string) error
	// ReplicateNack replicates a nack through Raft consensus.
	ReplicateNack(queue, messageID string, requeue bool) error
}

// Server is a TCP server for the broker.
type Server struct {
	broker     *Broker
	registry   *schema.Registry
	replicator Replicator
	listener   net.Listener
	wg         sync.WaitGroup
	done       chan struct{}
	ready      chan struct{} // closed when server is listening
}

// NewServer creates a new server with the given broker.
func NewServer(broker *Broker) *Server {
	return &Server{
		broker:   broker,
		registry: schema.NewRegistry(),
		done:     make(chan struct{}),
		ready:    make(chan struct{}),
	}
}

// SetReplicator configures a Replicator for clustered operation.
// When set, write operations are replicated via Raft before being applied.
func (s *Server) SetReplicator(r Replicator) {
	s.replicator = r
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
	close(s.ready) // Signal that server is ready

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

// WaitReady blocks until the server is listening.
func (s *Server) WaitReady() {
	<-s.ready
}

// Addr returns the server's listen address.
// Waits for the server to be ready before returning.
func (s *Server) Addr() net.Addr {
	<-s.ready // Wait for server to start
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

	// Clustered mode: replicate through Raft before applying locally.
	// The FSM.Apply on each node handles the actual enqueue + storage.
	if s.replicator != nil {
		if !s.replicator.IsLeader() {
			return EncodeError(9, "not leader")
		}

		msgID := req.GetMessageId()
		if msgID == "" {
			msgID = NewULID()
		}

		replicatedID, err := s.replicator.ReplicatePublish(
			req.GetQueue(), msgID, req.GetPayload(), req.GetHeaders(), time.Now(),
		)
		if err != nil {
			LogError("replicated publish failed", err, "queue", req.GetQueue(), "client", clientAddr)
			return EncodeError(3, err.Error())
		}

		resp := &protocol.PublishResponse{MessageId: replicatedID}
		LogPublish(req.GetQueue(), replicatedID, clientAddr)
		q := s.broker.GetQueue(req.GetQueue())
		RecordPublish(req.GetQueue(), time.Since(start).Seconds())
		if q != nil {
			UpdateQueueDepth(req.GetQueue(), q.Len())
			UpdateInFlightCount(req.GetQueue(), q.InFlightLen())
		}
		data, _ := proto.Marshal(resp)
		return protocol.EncodeFrame(protocol.OpPublishAck, data)
	}

	// Single-node mode: apply directly.
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

	// Clustered mode: replicate ack through Raft.
	if s.replicator != nil {
		if !s.replicator.IsLeader() {
			return EncodeError(9, "not leader")
		}
		if err := s.replicator.ReplicateAck(state.queueName, req.GetMessageId()); err != nil {
			LogError("replicated ack failed", err, "queue", state.queueName)
			return EncodeError(3, err.Error())
		}
	} else {
		// Single-node mode
		if !s.broker.HandleAck(&req, state.queueName) {
			return EncodeError(4, "message not found")
		}
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

	// Clustered mode: replicate nack through Raft.
	if s.replicator != nil {
		if !s.replicator.IsLeader() {
			return EncodeError(9, "not leader")
		}
		if err := s.replicator.ReplicateNack(state.queueName, req.GetMessageId(), req.GetRequeue()); err != nil {
			LogError("replicated nack failed", err, "queue", state.queueName)
			return EncodeError(3, err.Error())
		}
	} else {
		// Single-node mode
		if !s.broker.HandleNack(&req, state.queueName) {
			return EncodeError(4, "message not found")
		}
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
