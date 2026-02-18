package broker

import (
	"crypto/subtle"
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

// SchemaMode controls how publishes behave when no schema is registered.
type SchemaMode string

const (
	// SchemaModePermissive allows publishes to queues without registered schemas.
	// If a schema exists, payload validation is still enforced.
	SchemaModePermissive SchemaMode = "permissive"
	// SchemaModeStrict requires a schema to be registered before publish.
	SchemaModeStrict SchemaMode = "strict"
)

// Server is a TCP server for the broker.
type Server struct {
	broker     *Broker
	registry   *schema.Registry
	replicator Replicator
	listener   net.Listener
	wg         sync.WaitGroup
	done       chan struct{}
	ready      chan struct{} // closed when server is listening
	authToken  string        // if set, clients must authenticate before any other operation
	schemaMode SchemaMode
}

// NewServer creates a new server with the given broker.
func NewServer(broker *Broker) *Server {
	return &Server{
		broker:     broker,
		registry:   schema.NewRegistry(),
		done:       make(chan struct{}),
		ready:      make(chan struct{}),
		schemaMode: SchemaModePermissive,
	}
}

// SetReplicator configures a Replicator for clustered operation.
// When set, write operations are replicated via Raft before being applied.
func (s *Server) SetReplicator(r Replicator) {
	s.replicator = r
}

// SetAuthToken configures the required auth token. When set, clients must
// send OpAuth with a matching token as their first message.
func (s *Server) SetAuthToken(token string) {
	s.authToken = token
}

// SetSchemaMode configures schema enforcement behavior for publishes.
func (s *Server) SetSchemaMode(mode SchemaMode) {
	switch mode {
	case SchemaModeStrict:
		s.schemaMode = SchemaModeStrict
	default:
		s.schemaMode = SchemaModePermissive
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
	queueName      string
	groupName      string // consumer group name (empty for legacy)
	clientAddr     string
	msgCh          <-chan *Message
	stopCh         chan struct{}
	deliverWg      sync.WaitGroup
	writeMu        sync.Mutex
	callManager    *CallManager
	authenticated  bool            // true once OpAuth succeeds (or if auth is disabled)
	streamMode     bool            // true if consuming from a stream queue
	consumerGroup  string          // stream consumer group name
	streamConsumer *StreamConsumer // stream consumer reference
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	LogConnect(clientAddr)

	state := &connState{
		clientAddr:    clientAddr,
		stopCh:        make(chan struct{}),
		authenticated: s.authToken == "", // bypass auth when no token configured
	}
	defer func() {
		LogDisconnect(clientAddr)
		close(state.stopCh)
		state.deliverWg.Wait()
		if state.streamConsumer != nil {
			sq := s.broker.GetStreamQueue(state.queueName)
			if sq != nil {
				sq.RemoveConsumer(state.streamConsumer.Ch)
			}
		} else if state.msgCh != nil {
			q := s.broker.GetQueue(state.queueName)
			if q != nil {
				if state.groupName != "" {
					g := q.GetGroup(state.groupName)
					if g != nil {
						empty := g.RemoveMember(clientAddr)
						if empty {
							q.RemoveGroup(state.groupName)
						}
					}
				} else {
					q.RemoveConsumer(state.msgCh)
				}
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

		// Auth gate: when auth is required, only OpAuth is allowed before authentication
		if !state.authenticated {
			if frame.OpCode != protocol.OpAuth {
				resp := EncodeError(9, "authentication required")
				conn.Write(resp)
				return // disconnect
			}
			resp := s.handleAuth(frame.Payload, state)
			conn.Write(resp)
			if !state.authenticated {
				return // auth failed, disconnect
			}
			continue
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
	case protocol.OpSeek:
		return s.handleSeek(frame.Payload, state, conn)
	case protocol.OpCommitOffset:
		return s.handleCommitOffset(frame.Payload, state)
	case protocol.OpSchemaRegister:
		return s.handleSchemaRegister(frame.Payload)
	case protocol.OpSchemaGet:
		return s.handleSchemaGet(frame.Payload)
	case protocol.OpSchemaList:
		return s.handleSchemaList()
	case protocol.OpQueueList:
		return s.handleQueueList()
	case protocol.OpHeartbeat:
		return s.handleHeartbeat(frame.Payload, state)
	case protocol.OpUnsubscribe:
		return s.handleUnsubscribe(frame.Payload, state)
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

	if s.schemaMode == SchemaModeStrict {
		if _, err := s.registry.Get(req.GetQueue()); err != nil {
			return EncodeError(7, "schema required for queue")
		}
	}

	// Validate message against schema if present. In permissive mode, queues
	// without schemas are accepted.
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

	// Check if this is a stream queue
	if s.broker.IsStreamQueue(req.GetQueue()) {
		resp, _, err := s.broker.HandleStreamPublish(&req)
		if err != nil {
			LogError("stream publish failed", err, "queue", req.GetQueue(), "client", clientAddr)
			return EncodeError(3, err.Error())
		}
		LogPublish(req.GetQueue(), resp.MessageId, clientAddr)
		RecordPublish(req.GetQueue(), time.Since(start).Seconds())
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
	state.groupName = req.GetGroup()
	state.msgCh = s.broker.HandleConsume(&req, state.clientAddr)

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

	// In stream mode, ack is a no-op for message deletion (messages are retained).
	// The consumer tracks progress via offset commits.
	if state.streamMode {
		LogAck(state.queueName, req.GetMessageId(), state.clientAddr)
		RecordAck(state.queueName)
		return nil
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
		if !s.broker.HandleAck(&req, state.queueName, state.groupName) {
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
		if !s.broker.HandleNack(&req, state.queueName, state.groupName) {
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
		// Check if stream queue
		sq := s.broker.GetStreamQueue(name)
		if sq != nil {
			queues = append(queues, &protocol.QueueInfo{
				Name:         name,
				MessageCount: uint32(sq.Len()),
			})
			continue
		}
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

func (s *Server) handleSeek(payload []byte, state *connState, conn net.Conn) []byte {
	var req protocol.SeekRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	queueName := req.GetQueue()
	group := req.GetConsumerGroup()
	if group == "" {
		group = state.clientAddr // default group per connection
	}

	// Ensure stream queue exists
	sq := s.broker.GetStreamQueue(queueName)
	if sq == nil {
		// Create it as a stream queue
		sq = s.broker.GetOrCreateStreamQueue(queueName)
	}

	// Resolve the offset
	var offset uint64
	switch req.GetPosition() {
	case protocol.SeekPosition_SEEK_BEGINNING:
		offset = 1
	case protocol.SeekPosition_SEEK_END:
		offset = sq.NextSequence()
	case protocol.SeekPosition_SEEK_OFFSET:
		offset = req.GetOffset()
		if offset == 0 {
			offset = 1
		}
	case protocol.SeekPosition_SEEK_TIMESTAMP:
		if sq.storage != nil {
			ts, err := sq.storage.GetStreamMessageByTimestamp(queueName, req.GetTimestamp())
			if err != nil {
				return EncodeError(3, err.Error())
			}
			if ts == 0 {
				offset = sq.NextSequence()
			} else {
				offset = ts
			}
		} else {
			offset = 1
		}
	default:
		// Try to resume from committed offset
		committed, err := sq.GetCommittedOffset(group)
		if err != nil {
			return EncodeError(3, err.Error())
		}
		if committed > 0 {
			offset = committed + 1
		} else {
			offset = sq.NextSequence()
		}
	}

	// Remove existing stream consumer for this connection
	if state.streamConsumer != nil {
		sq.RemoveConsumer(state.streamConsumer.Ch)
		state.streamConsumer = nil
	}

	state.queueName = queueName
	state.streamMode = true
	state.consumerGroup = group

	// Subscribe to stream
	sc := sq.Subscribe(group, offset)
	state.streamConsumer = sc
	state.msgCh = sc.Ch

	// Start delivery goroutine for stream messages
	state.deliverWg.Add(1)
	go func() {
		defer state.deliverWg.Done()
		for {
			select {
			case msg, ok := <-sc.Ch:
				if !ok {
					return
				}
				LogConsume(queueName, msg.ID, state.clientAddr)
				RecordConsume(queueName)

				smsg := StreamMessageToProto(msg)
				data, _ := proto.Marshal(smsg)
				frame := protocol.EncodeFrame(protocol.OpStreamMessage, data)

				state.writeMu.Lock()
				conn.Write(frame)
				state.writeMu.Unlock()
			case <-state.stopCh:
				return
			}
		}
	}()

	// Send seek ack
	resp := &protocol.SeekResponse{Offset: offset}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpSeekAck, data)
}

func (s *Server) handleCommitOffset(payload []byte, state *connState) []byte {
	var req protocol.CommitOffsetRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	queueName := req.GetQueue()
	if queueName == "" {
		queueName = state.queueName
	}
	group := req.GetConsumerGroup()
	if group == "" {
		group = state.consumerGroup
	}

	sq := s.broker.GetStreamQueue(queueName)
	if sq == nil {
		return EncodeError(4, "stream queue not found")
	}

	if err := sq.CommitOffset(group, req.GetOffset()); err != nil {
		return EncodeError(3, err.Error())
	}

	resp := &protocol.CommitOffsetResponse{Offset: req.GetOffset()}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpCommitOffsetAck, data)
}

func (s *Server) handleHeartbeat(payload []byte, state *connState) []byte {
	var req protocol.HeartbeatRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	if !s.broker.HandleHeartbeat(&req, state.clientAddr) {
		return EncodeError(4, "group member not found")
	}

	resp := &protocol.HeartbeatResponse{}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpHeartbeatAck, data)
}

func (s *Server) handleUnsubscribe(payload []byte, state *connState) []byte {
	var req protocol.UnsubscribeRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return EncodeError(2, "invalid request")
	}

	if !s.broker.HandleUnsubscribe(&req, state.clientAddr, state.msgCh) {
		return EncodeError(4, "consumer not found")
	}

	// Clear connection state so disconnect cleanup doesn't double-remove
	state.msgCh = nil
	state.groupName = ""
	return nil
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

func (s *Server) handleAuth(payload []byte, state *connState) []byte {
	var req protocol.AuthRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		resp := &protocol.AuthResponse{Success: false, Message: "invalid auth request"}
		data, _ := proto.Marshal(resp)
		return protocol.EncodeFrame(protocol.OpAuthResponse, data)
	}

	tokenBytes := []byte(req.GetToken())
	expectedBytes := []byte(s.authToken)
	if subtle.ConstantTimeCompare(tokenBytes, expectedBytes) == 1 {
		state.authenticated = true
		logger.Info("client authenticated", "addr", state.clientAddr)
		resp := &protocol.AuthResponse{Success: true, Message: "authenticated"}
		data, _ := proto.Marshal(resp)
		return protocol.EncodeFrame(protocol.OpAuthResponse, data)
	}

	logger.Warn("authentication failed", "addr", state.clientAddr)
	resp := &protocol.AuthResponse{Success: false, Message: "invalid token"}
	data, _ := proto.Marshal(resp)
	return protocol.EncodeFrame(protocol.OpAuthResponse, data)
}
