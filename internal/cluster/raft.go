package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/jonas/qwer-q/internal/broker"
)

var (
	ErrNotLeader   = errors.New("not leader")
	ErrNoQuorum    = errors.New("cluster lost quorum")
	ErrApplyFailed = errors.New("raft apply failed")
)

// Config holds clustering configuration.
type Config struct {
	// NodeID is this node's unique identifier.
	NodeID string
	// BindAddr is the address for Raft communication (e.g., "0.0.0.0:9878").
	BindAddr string
	// AdvertiseAddr is the address advertised to peers (defaults to BindAddr).
	AdvertiseAddr string
	// DataDir is where Raft stores logs and snapshots.
	DataDir string
	// Peers is the initial cluster membership (e.g., ["node1=host1:9878", "node2=host2:9878"]).
	Peers []string
	// Bootstrap indicates whether this node should bootstrap a new cluster.
	Bootstrap bool
}

// Node wraps a Raft node and provides the interface for the broker to replicate operations.
type Node struct {
	raft   *raft.Raft
	fsm    *FSM
	config Config
	logger *slog.Logger

	transport *raft.NetworkTransport
	logStore  *raftboltdb.BoltStore
	stable    *raftboltdb.BoltStore
	snapStore raft.SnapshotStore
}

// NewNode creates and starts a Raft node.
func NewNode(cfg Config, b *broker.Broker, logger *slog.Logger) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("cluster: node ID is required")
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("cluster: bind address is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("cluster: data directory is required")
	}

	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.BindAddr
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("cluster: create data dir: %w", err)
	}

	fsm := NewFSM(b, logger)

	// Raft configuration
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	// Tuning for fast failover (requirement: <2 second election)
	raftCfg.HeartbeatTimeout = 500 * time.Millisecond
	raftCfg.ElectionTimeout = 500 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 250 * time.Millisecond
	raftCfg.CommitTimeout = 100 * time.Millisecond
	raftCfg.SnapshotInterval = 5 * time.Minute
	raftCfg.SnapshotThreshold = 8192

	// Suppress raft's own logging, route through our logger
	raftCfg.LogOutput = io.Discard

	// TCP transport
	advertise, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: resolve advertise addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, advertise, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("cluster: create transport: %w", err)
	}

	// Log store (bolt)
	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("cluster: create log store: %w", err)
	}

	// Stable store (reuses bolt for simplicity)
	stableStorePath := filepath.Join(cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		logStore.Close()
		transport.Close()
		return nil, fmt.Errorf("cluster: create stable store: %w", err)
	}

	// Snapshot store
	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, io.Discard)
	if err != nil {
		stableStore.Close()
		logStore.Close()
		transport.Close()
		return nil, fmt.Errorf("cluster: create snapshot store: %w", err)
	}

	r, err := raft.NewRaft(raftCfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		stableStore.Close()
		logStore.Close()
		transport.Close()
		return nil, fmt.Errorf("cluster: create raft: %w", err)
	}

	node := &Node{
		raft:      r,
		fsm:       fsm,
		config:    cfg,
		logger:    logger,
		transport: transport,
		logStore:  logStore,
		stable:    stableStore,
		snapStore: snapStore,
	}

	// Bootstrap if this is a new cluster
	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.NodeID),
				Address: raft.ServerAddress(cfg.AdvertiseAddr),
			},
		}
		// Add peers to the initial configuration
		for _, peer := range cfg.Peers {
			id, addr, err := parsePeer(peer)
			if err != nil {
				logger.Warn("skipping invalid peer", "peer", peer, "error", err)
				continue
			}
			if id == cfg.NodeID {
				continue // Skip self
			}
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: raft.ServerAddress(addr),
			})
		}

		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil {
			// ErrCantBootstrap means already bootstrapped, which is fine
			if !errors.Is(err, raft.ErrCantBootstrap) {
				node.Close()
				return nil, fmt.Errorf("cluster: bootstrap: %w", err)
			}
		}
	}

	logger.Info("cluster node started",
		"node_id", cfg.NodeID,
		"bind", cfg.BindAddr,
		"advertise", cfg.AdvertiseAddr,
	)

	return node, nil
}

// parsePeer parses "id=host:port" into (id, addr).
func parsePeer(peer string) (string, string, error) {
	for i, c := range peer {
		if c == '=' {
			id := peer[:i]
			addr := peer[i+1:]
			if id == "" || addr == "" {
				return "", "", fmt.Errorf("invalid peer format: %q (expected id=host:port)", peer)
			}
			return id, addr, nil
		}
	}
	return "", "", fmt.Errorf("invalid peer format: %q (expected id=host:port)", peer)
}

// IsLeader returns true if this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader, or empty if unknown.
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// State returns the current Raft state as a string.
func (n *Node) State() string {
	return n.raft.State().String()
}

// Apply proposes a command to the Raft cluster. Only succeeds on the leader.
// The command is replicated to a majority before returning.
func (n *Node) Apply(cmd Command, timeout time.Duration) (*FSMResponse, error) {
	if n.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, ErrNotLeader
		}
		return nil, fmt.Errorf("%w: %v", ErrApplyFailed, err)
	}

	resp, ok := future.Response().(*FSMResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected FSM response type")
	}

	return resp, resp.Error
}

// WaitForLeader blocks until a leader is elected or the timeout expires.
func (n *Node) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			addr, _ := n.raft.LeaderWithID()
			if addr != "" {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for leader election")
		}
	}
}

// HasQuorum returns true if the cluster has enough nodes for a majority.
func (n *Node) HasQuorum() bool {
	// If we can get the leader, the cluster has quorum
	addr, _ := n.raft.LeaderWithID()
	return addr != ""
}

// Close shuts down the Raft node.
func (n *Node) Close() error {
	if err := n.raft.Shutdown().Error(); err != nil {
		return err
	}
	n.transport.Close()
	n.logStore.Close()
	n.stable.Close()
	return nil
}
