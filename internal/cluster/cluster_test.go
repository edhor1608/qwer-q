package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jonas/qwer-q/internal/broker"
	"log/slog"
)

// testLogger returns a silent logger for tests.
func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// makeRaftLog creates a raft.Log with the given data for FSM testing.
func makeRaftLog(data []byte) *raft.Log {
	return &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  data,
	}
}

// startTestCluster creates and bootstraps a 3-node cluster in temp directories.
// Returns the nodes and a cleanup function.
func startTestCluster(t *testing.T) ([]*Node, []*broker.Broker, func()) {
	t.Helper()

	dirs := make([]string, 3)
	brokers := make([]*broker.Broker, 3)
	nodes := make([]*Node, 3)

	for i := 0; i < 3; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("raft-test-%d-*", i))
		if err != nil {
			t.Fatal(err)
		}
		dirs[i] = dir
		brokers[i] = broker.NewBroker()
	}

	basePorts := []int{19878, 19879, 19880}

	peers := make([]string, 3)
	for i := 0; i < 3; i++ {
		peers[i] = fmt.Sprintf("node%d=127.0.0.1:%d", i, basePorts[i])
	}

	logger := testLogger()

	// Start all nodes
	for i := 0; i < 3; i++ {
		cfg := Config{
			NodeID:        fmt.Sprintf("node%d", i),
			BindAddr:      fmt.Sprintf("127.0.0.1:%d", basePorts[i]),
			AdvertiseAddr: fmt.Sprintf("127.0.0.1:%d", basePorts[i]),
			DataDir:       dirs[i],
			Peers:         peers,
			Bootstrap:     i == 0, // Only first node bootstraps
		}

		node, err := NewNode(cfg, brokers[i], logger)
		if err != nil {
			// Cleanup already started nodes
			for j := 0; j < i; j++ {
				nodes[j].Close()
				brokers[j].Close()
			}
			for _, d := range dirs {
				os.RemoveAll(d)
			}
			t.Fatalf("failed to start node %d: %v", i, err)
		}
		nodes[i] = node
	}

	cleanup := func() {
		for i := 0; i < 3; i++ {
			if nodes[i] != nil {
				nodes[i].Close()
			}
			brokers[i].Close()
			os.RemoveAll(dirs[i])
		}
	}

	return nodes, brokers, cleanup
}

func TestLeaderElection(t *testing.T) {
	nodes, _, cleanup := startTestCluster(t)
	defer cleanup()

	// Wait for a leader
	err := nodes[0].WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Exactly one node should be leader
	leaderCount := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
}

func TestPublishReplication(t *testing.T) {
	nodes, brokers, cleanup := startTestCluster(t)
	defer cleanup()

	err := nodes[0].WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Find the leader
	var leader *Node
	for _, n := range nodes {
		if n.IsLeader() {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader found")
	}

	// Publish a message through Raft
	msgID, err := leader.ReplicatePublish("test-queue", "msg-001", []byte("hello cluster"), nil, time.Now(), false)
	if err != nil {
		t.Fatalf("replicate publish failed: %v", err)
	}
	if msgID != "msg-001" {
		t.Fatalf("expected message ID msg-001, got %s", msgID)
	}

	// Give time for replication to propagate to all followers
	time.Sleep(500 * time.Millisecond)

	// All nodes should have the queue with the message
	for i, b := range brokers {
		q := b.GetQueue("test-queue")
		if q == nil {
			t.Fatalf("node %d: queue not found", i)
		}
		if q.Len() != 1 {
			t.Fatalf("node %d: expected 1 message, got %d", i, q.Len())
		}
	}
}

func TestPublishReplicationStreamMode(t *testing.T) {
	nodes, brokers, cleanup := startTestCluster(t)
	defer cleanup()

	err := nodes[0].WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Find the leader
	var leader *Node
	for _, n := range nodes {
		if n.IsLeader() {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader found")
	}

	// Replicate as stream-mode publish.
	msgID, err := leader.ReplicatePublish("stream-queue", "stream-msg-001", []byte("hello stream cluster"), nil, time.Now(), true)
	if err != nil {
		t.Fatalf("replicate stream publish failed: %v", err)
	}
	if msgID != "stream-msg-001" {
		t.Fatalf("expected message ID stream-msg-001, got %s", msgID)
	}

	time.Sleep(500 * time.Millisecond)

	// All nodes should have stream queue state; none should create regular queue state.
	for i, b := range brokers {
		sq := b.GetStreamQueue("stream-queue")
		if sq == nil {
			t.Fatalf("node %d: stream queue not found", i)
		}
		if sq.Len() != 1 {
			t.Fatalf("node %d: expected 1 stream message, got %d", i, sq.Len())
		}
		if q := b.GetQueue("stream-queue"); q != nil {
			t.Fatalf("node %d: unexpected regular queue created for stream queue", i)
		}
	}
}

func TestNonLeaderRejectWrites(t *testing.T) {
	nodes, _, cleanup := startTestCluster(t)
	defer cleanup()

	err := nodes[0].WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Find a follower
	var follower *Node
	for _, n := range nodes {
		if !n.IsLeader() {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	// Attempt to write on follower should fail
	_, err = follower.ReplicatePublish("test-queue", "msg-001", []byte("should fail"), nil, time.Now(), false)
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestFailover(t *testing.T) {
	nodes, brokers, cleanup := startTestCluster(t)
	defer cleanup()

	err := nodes[0].WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader election failed: %v", err)
	}

	// Find the leader index
	leaderIdx := -1
	for i, n := range nodes {
		if n.IsLeader() {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader found")
	}

	// Publish a message before failover
	_, err = nodes[leaderIdx].ReplicatePublish("failover-queue", "msg-before", []byte("before failover"), nil, time.Now(), false)
	if err != nil {
		t.Fatalf("publish before failover failed: %v", err)
	}

	// Kill the leader
	nodes[leaderIdx].Close()
	nodes[leaderIdx] = nil

	// Wait for new leader election (should be within 2 seconds per requirement)
	var newLeader *Node
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if n != nil && i != leaderIdx && n.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected after failover")
	}

	// New leader should be able to accept writes
	_, err = newLeader.ReplicatePublish("failover-queue", "msg-after", []byte("after failover"), nil, time.Now(), false)
	if err != nil {
		t.Fatalf("publish after failover failed: %v", err)
	}

	// Give time for replication
	time.Sleep(500 * time.Millisecond)

	// Surviving nodes should have both messages
	for i, b := range brokers {
		if i == leaderIdx {
			continue // Skip the killed node
		}
		q := b.GetQueue("failover-queue")
		if q == nil {
			t.Fatalf("node %d: queue not found", i)
		}
		if q.Len() != 2 {
			t.Fatalf("node %d: expected 2 messages, got %d", i, q.Len())
		}
	}
}

func TestFSMApplyPublish(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	fsm := NewFSM(b, testLogger())

	pubCmd := PublishCommand{
		Queue:       "fsm-queue",
		MessageID:   "msg-100",
		Payload:     []byte("test payload"),
		PublishedAt: time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(pubCmd)
	cmd := Command{Type: CmdPublish, Data: data}
	cmdData, _ := json.Marshal(cmd)

	resp := fsm.Apply(makeRaftLog(cmdData))
	fsmResp := resp.(*FSMResponse)
	if fsmResp.Error != nil {
		t.Fatalf("FSM apply failed: %v", fsmResp.Error)
	}
	if fsmResp.MessageID != "msg-100" {
		t.Fatalf("expected msg-100, got %s", fsmResp.MessageID)
	}

	q := b.GetQueue("fsm-queue")
	if q == nil {
		t.Fatal("queue not created")
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 message, got %d", q.Len())
	}
}

func TestFSMApplyPublishStream(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	fsm := NewFSM(b, testLogger())

	pubCmd := PublishCommand{
		Queue:       "fsm-stream-queue",
		MessageID:   "stream-msg-100",
		Payload:     []byte("stream payload"),
		PublishedAt: time.Now().UnixMilli(),
		Stream:      true,
	}
	data, _ := json.Marshal(pubCmd)
	cmd := Command{Type: CmdPublish, Data: data}
	cmdData, _ := json.Marshal(cmd)

	resp := fsm.Apply(makeRaftLog(cmdData))
	fsmResp := resp.(*FSMResponse)
	if fsmResp.Error != nil {
		t.Fatalf("FSM stream publish failed: %v", fsmResp.Error)
	}

	sq := b.GetStreamQueue("fsm-stream-queue")
	if sq == nil {
		t.Fatal("stream queue not created")
	}
	if sq.Len() != 1 {
		t.Fatalf("expected 1 stream message, got %d", sq.Len())
	}
	if q := b.GetQueue("fsm-stream-queue"); q != nil {
		t.Fatal("regular queue should not be created for stream publish")
	}
}

func TestFSMApplyAck(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	fsm := NewFSM(b, testLogger())

	// First publish
	pubCmd := PublishCommand{
		Queue:       "ack-queue",
		MessageID:   "msg-200",
		Payload:     []byte("to ack"),
		PublishedAt: time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(pubCmd)
	cmd := Command{Type: CmdPublish, Data: data}
	cmdData, _ := json.Marshal(cmd)
	fsm.Apply(makeRaftLog(cmdData))

	// Start consumer to move message to in-flight
	q := b.GetQueue("ack-queue")
	ch := q.Dequeue(30 * time.Second)
	<-ch

	// Now ack via FSM
	ackCmd := AckCommand{Queue: "ack-queue", MessageID: "msg-200"}
	data, _ = json.Marshal(ackCmd)
	cmd = Command{Type: CmdAck, Data: data}
	cmdData, _ = json.Marshal(cmd)

	resp := fsm.Apply(makeRaftLog(cmdData))
	fsmResp := resp.(*FSMResponse)
	if fsmResp.Error != nil {
		t.Fatalf("FSM ack failed: %v", fsmResp.Error)
	}

	if q.InFlightLen() != 0 {
		t.Fatalf("expected 0 in-flight, got %d", q.InFlightLen())
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	b1 := broker.NewBroker()
	defer b1.Close()

	fsm1 := NewFSM(b1, testLogger())

	// Publish some messages
	for i := 0; i < 5; i++ {
		pubCmd := PublishCommand{
			Queue:       "snap-queue",
			MessageID:   fmt.Sprintf("msg-%d", i),
			Payload:     []byte(fmt.Sprintf("payload-%d", i)),
			PublishedAt: time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(pubCmd)
		cmd := Command{Type: CmdPublish, Data: data}
		cmdData, _ := json.Marshal(cmd)
		fsm1.Apply(makeRaftLog(cmdData))
	}

	// Take snapshot -- since no storage, snapshot will have empty messages
	// but queue names should be preserved
	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	// Persist snapshot to buffer
	sink := &mockSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	// Restore to a new FSM
	b2 := broker.NewBroker()
	defer b2.Close()
	fsm2 := NewFSM(b2, testLogger())

	if err := fsm2.Restore(&mockReadCloser{data: sink.data}); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Queue should exist in restored broker
	q := b2.GetQueue("snap-queue")
	if q == nil {
		t.Fatal("queue not restored")
	}
}

func TestFSMRestoreClearsExistingState(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	// Seed stale state.
	q := b.GetOrCreateQueue("stale-queue")
	if err := q.Enqueue(&broker.Message{
		ID:          "stale-msg",
		Queue:       "stale-queue",
		Payload:     []byte("stale"),
		PublishedAt: time.Now(),
		VisibleAt:   time.Now(),
	}); err != nil {
		t.Fatalf("seed stale queue failed: %v", err)
	}
	b.GetOrCreateStreamQueue("stale-stream")

	fsm := NewFSM(b, testLogger())
	snapshot := fsmSnapshot{
		Queues: map[string][]*snapshotMessage{
			"fresh-queue": nil,
		},
		StreamQueues: []string{"fresh-stream"},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	if err := fsm.Restore(&mockReadCloser{data: data}); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if b.GetQueue("stale-queue") != nil {
		t.Fatal("stale regular queue should be removed during restore")
	}
	if b.GetStreamQueue("stale-stream") != nil {
		t.Fatal("stale stream queue should be removed during restore")
	}
	if b.GetQueue("fresh-queue") == nil {
		t.Fatal("fresh queue from snapshot not restored")
	}
	if b.GetStreamQueue("fresh-stream") == nil {
		t.Fatal("fresh stream queue from snapshot not restored")
	}
}

func TestParsePeer(t *testing.T) {
	tests := []struct {
		input   string
		id      string
		addr    string
		wantErr bool
	}{
		{"node1=host1:9878", "node1", "host1:9878", false},
		{"abc=10.0.0.1:1234", "abc", "10.0.0.1:1234", false},
		{"bad-format", "", "", true},
		{"=host:port", "", "", true},
		{"id=", "", "", true},
	}

	for _, tt := range tests {
		id, addr, err := parsePeer(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parsePeer(%q): expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parsePeer(%q): unexpected error: %v", tt.input, err)
			continue
		}
		if id != tt.id || addr != tt.addr {
			t.Errorf("parsePeer(%q) = (%q, %q), want (%q, %q)", tt.input, id, addr, tt.id, tt.addr)
		}
	}
}

// mockSnapshotSink captures snapshot data in memory.
type mockSnapshotSink struct {
	data []byte
}

func (s *mockSnapshotSink) Write(p []byte) (int, error) {
	s.data = append(s.data, p...)
	return len(p), nil
}

func (s *mockSnapshotSink) Close() error  { return nil }
func (s *mockSnapshotSink) ID() string    { return "mock" }
func (s *mockSnapshotSink) Cancel() error { return nil }

// mockReadCloser wraps a byte slice as io.ReadCloser.
type mockReadCloser struct {
	data []byte
	pos  int
}

func (r *mockReadCloser) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *mockReadCloser) Close() error { return nil }
