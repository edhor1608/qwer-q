# Clustering & High Availability Research

**Date:** 2026-02-05
**Status:** Research Complete
**Author:** Research Expert (AI Agent)
**Blocks:** Task #15 — Implement clustering foundation

---

## Executive Summary

This document evaluates consensus and replication strategies for adding clustering and HA to qwer-q. The recommendation is a **phased Raft-based approach using hashicorp/raft**, with gossip (hashicorp/memberlist) for peer discovery. This matches qwer-q's "single binary, docker-first" philosophy while providing strong consistency guarantees suitable for a message queue.

**Key finding:** For a queue (messages deleted after ack), Raft's strong consistency is essential. Eventually-consistent replication risks duplicate delivery or message loss. The latency overhead of Raft (1-3ms per commit on local network) is acceptable given qwer-q's current ~2-5K msgs/sec throughput.

---

## 1. Consensus Algorithm Comparison

### 1.1 Approaches Overview

| Approach | Consistency | Latency Overhead | Complexity | Best For |
|---|---|---|---|---|
| **Raft** | Strong (linearizable) | 1-3ms per commit (LAN) | Medium | Leader-based writes, queue semantics |
| **Gossip (SWIM)** | Eventual | <1ms (no write barrier) | Low | Membership, metadata, config |
| **Primary-Backup** | Strong (if sync) | 1 RTT to backup | Low | Simple 2-node setups |
| **Multi-Raft** | Strong (per-group) | 1-3ms per commit | High | Per-queue replication groups |

### 1.2 Raft (Recommended for Data Replication)

**How it works:** Leader election, log replication to majority quorum, committed once majority acks. All writes go through leader; reads can be served by leader (linearizable) or followers (stale).

**Latency impact:**
- Each publish requires leader to replicate to quorum before ack
- LAN: 1-3ms added latency per message
- At 2K msgs/sec current throughput, Raft batching can amortize this significantly
- RabbitMQ quorum queues use Raft and achieve production-grade throughput

**Go Libraries:**

| Library | Stars | Last Release | Module | Notes |
|---|---|---|---|---|
| **hashicorp/raft** | 8,913 | v1.7.3 (2025-03-20) | `github.com/hashicorp/raft` | Battle-tested. Used by Consul, Vault, Nomad, rqlite. 2,236 importers. Actively maintained. |
| **etcd-io/raft** | 983 | - | `go.etcd.io/etcd/raft/v3` | Core Raft only — no transport, no snapshots. User must build everything. Powers etcd, CockroachDB. Minimal API by design. |
| **lni/dragonboat** | 5,299 | v3.3.8 (2023-09-25) | `github.com/lni/dragonboat/v3` | Multi-group Raft. 9M writes/sec with RocksDB. v4 in development, v3 stable. Less ecosystem adoption than hashicorp. |

**Recommendation: hashicorp/raft**

Rationale:
- Most battle-tested in production (Consul manages millions of nodes)
- Clean FSM interface maps directly to qwer-q's broker state
- Built-in snapshot/restore for state transfer to new nodes
- Existing `raft-badger` log store (since qwer-q already uses BadgerDB)
- Extensive documentation and reference implementations (rqlite, hraftd)
- Active maintenance (v1.7.3 released March 2025)
- etcd/raft requires building transport + snapshot + log storage yourself
- dragonboat is higher performance but less ecosystem support and stale releases

### 1.3 Gossip / SWIM (Recommended for Discovery)

**How it works:** Nodes periodically probe random peers. Failed probes trigger indirect probes through other nodes. Membership changes propagate via piggyback gossip messages.

**Library: hashicorp/memberlist**
- 4,009 stars, actively maintained (last push Jan 2026)
- Used by Consul, Serf, and many Go projects
- SWIM protocol with Lifeguard extensions for robustness
- Configurable convergence speed
- Supports user-defined metadata broadcast

**Use for qwer-q:**
- Peer discovery (nodes find each other without hardcoded addresses)
- Metadata propagation (which node is leader, queue assignments)
- Health monitoring (supplement Raft's built-in failure detection)
- NOT for data replication (eventual consistency unsuitable for queue data)

### 1.4 Primary-Backup (Not Recommended)

**How it works:** Primary handles all writes, synchronously or asynchronously replicates to backup(s). On primary failure, backup promotes.

**Why not for qwer-q:**
- No built-in leader election (need external coordination or manual failover)
- Split-brain risk without quorum (two primaries accepting writes)
- Async replication loses messages; sync replication is basically Raft without the benefits
- Only viable for 2-node setups, doesn't scale to 3+ nodes cleanly
- Redis Sentinel uses this model but still needs 3 Sentinel nodes for quorum

### 1.5 Multi-Raft (Future Consideration)

**How it works:** Each queue (or set of queues) gets its own Raft group. Different queues can have leaders on different nodes, distributing write load.

**Why defer:**
- CockroachDB uses this for range-based replication (tens of thousands of groups)
- Dragonboat supports this natively, hashicorp/raft requires per-group instances
- Adds significant complexity (group management, resource allocation per group)
- Only beneficial when single-leader bottleneck is hit
- qwer-q at 2-5K msgs/sec is nowhere near needing this

**Revisit trigger:** When single-leader throughput becomes a bottleneck (>50K msgs/sec or >100 queues with independent write patterns).

---

## 2. How Existing Systems Do Clustering

### 2.1 NATS JetStream

**Architecture:**
- Custom Raft implementation (not hashicorp/raft, custom-built)
- Three Raft group tiers:
  1. **Meta Group** — All servers. Manages API, server placement, stream assignment
  2. **Stream Group** — Per stream. Replicates message data. Leader handles publishes + acks
  3. **Consumer Group** — Per consumer. Replicates consumer state (offsets, acks)
- Raft data plane combined with message replication (optimization — single round-trip for both)

**Key insight for qwer-q:** NATS combines Raft consensus messages with data replication into a single network round-trip. This is an optimization we could consider in Phase 3, but it requires deep protocol integration.

**Cluster size:** 3 or 5 nodes recommended. Odd numbers for quorum math.

**Consistency:** Linearizable for writes. Reads from leader are consistent; follower reads may be stale.

**References:**
- [JetStream Clustering Docs](https://docs.nats.io/running-a-nats-service/configuration/clustering/jetstream_clustering)
- [NATS Architecture](https://github.com/nats-io/nats-general/blob/main/architecture/ARCHITECTURE.md)

### 2.2 RabbitMQ Quorum Queues

**Architecture:**
- Raft-based (using Ra, Erlang Raft library)
- Per-queue Raft group (multi-Raft)
- Shared WAL per node (all queue groups on a node share one write-ahead log)
- Leader handles publishes, replicates to followers, confirms after quorum ack

**Key insight for qwer-q:** RabbitMQ deprecated mirrored queues in favor of Raft-based quorum queues. The industry is converging on Raft for message queue replication. Shared WAL per node is an important optimization — reduces fsync calls.

**Performance:** Quorum queues have ~10-20% lower throughput than classic queues due to replication overhead, but much stronger durability guarantees.

**References:**
- [Quorum Queues Documentation](https://www.rabbitmq.com/docs/quorum-queues)
- [Quorum Queues Internals — CloudAMQP](https://www.cloudamqp.com/blog/quorum-queues-internals-a-deep-dive.html)

### 2.3 Redis

**Two modes:**
- **Sentinel:** Primary-backup with external failover coordination. 3 Sentinel processes monitor 1 primary + N replicas. Async replication (data loss on failover).
- **Cluster:** Hash-slot based sharding. Each slot range has primary + replicas. Gossip protocol for cluster state. Minimum 6 nodes (3 primary + 3 replica).

**Key insight for qwer-q:** Redis Sentinel's simplicity is appealing but its async replication means message loss on failover — unacceptable for a durable queue. Redis Cluster's sharding model doesn't apply well to queues (queues are whole units, not shardable by key range).

### 2.4 rqlite (Most Relevant Reference Implementation)

**Architecture:**
- SQLite + hashicorp/raft — single binary, distributed
- 17,284 GitHub stars, actively maintained
- HTTP API for all operations
- Leader handles writes, redirects clients to leader on follower
- Read consistency levels: none, weak, strong

**Key insight for qwer-q:** rqlite is the closest analog to what we're building. It proves hashicorp/raft works well for single-binary distributed systems. Their approach to client redirect (HTTP 301 to leader) maps to our protocol redirect approach.

**References:**
- [rqlite GitHub](https://github.com/rqlite/rqlite)
- [rqlite Architecture](https://rqlite.io/docs/features/)

### 2.5 CockroachDB

**Architecture:**
- Multi-Raft with range-based replication
- Each 64MB range is an independent Raft group
- Based on etcd/raft library
- Leaseholder (== Raft leader) handles reads and writes
- Joint consensus for membership changes

**Key insight for qwer-q:** Multi-Raft is overkill for our scale. But their concept of "leaseholder" is interesting — a node holds a time-limited lease to serve reads without going through Raft, reducing read latency.

---

## 3. BadgerDB in Clustered Context

### 3.1 Dual Role of BadgerDB

In a clustered qwer-q, BadgerDB serves two distinct purposes:

1. **Raft Log Store** — Stores Raft consensus log entries (which operations happened in which order)
2. **Application Data Store** — Stores actual queue messages (the FSM state)

These should be **separate BadgerDB instances** to avoid contention and enable independent tuning.

### 3.2 Raft Log Store Options

| Option | Maturity | Notes |
|---|---|---|
| **BBVA/raft-badger** | Low (120 stars, last commit 2023) | Implements LogStore + StableStore for hashicorp/raft using BadgerDB. Works but unmaintained. |
| **hashicorp/raft-boltdb** | High (official) | Default for hashicorp/raft. BoltDB-based. Simple, proven, slightly slower on truncation. |
| **hashicorp/raft-wal** | Medium (v0.4.2, Jan 2025) | Purpose-built WAL. Better truncation performance. Used in Consul 1.20+, Vault. Newer but HashiCorp-maintained. |
| **Fork raft-badger** | N/A | Fork BBVA/raft-badger, update to BadgerDB v4, maintain ourselves. |

**Recommendation: hashicorp/raft-boltdb for Phase 1, migrate to raft-wal in Phase 2**

Rationale:
- raft-boltdb is the default, well-tested, zero-risk choice for getting clustering working
- raft-wal is the future (Consul and Vault are migrating to it) and solves BoltDB's truncation performance issues
- Using BadgerDB for Raft logs (raft-badger) seems logical since we already use BadgerDB, but the library is unmaintained and mixing Raft log store + app data store creates coupling risk
- Separate storage engines for Raft log vs app data is the pattern used by every production system

### 3.3 Application State Machine (FSM)

The FSM is the core integration point. qwer-q's broker state must be expressed as a deterministic state machine:

```
Apply(log *raft.Log) interface{}   // Apply a committed log entry
Snapshot() (FSMSnapshot, error)    // Capture current state
Restore(io.ReadCloser) error       // Restore from snapshot
```

Operations that go through Raft (must be deterministic):
- Publish message (add to queue)
- Ack message (remove from queue)
- Create/delete queue
- Register schema
- Update queue configuration

Operations that do NOT go through Raft:
- Consumer subscribe/unsubscribe (local routing, not replicated state)
- Read queue stats (read from local state)
- Consumer message delivery (leader delivers from local state)

---

## 4. Client Failover & Leader Discovery

### 4.1 Approaches

| Approach | How It Works | Pros | Cons |
|---|---|---|---|
| **Protocol redirect** | Client connects to any node. Non-leader returns REDIRECT with leader address. | Simple. Client retries with leader. Works with custom protocol. | Extra round-trip on first connect. Client must handle redirect. |
| **Proxy/forward** | Any node accepts writes, forwards to leader internally. | Transparent to client. No client changes. | Higher latency (extra hop). More complex server. Leader must return response via proxy. |
| **Gossip metadata** | Client subscribes to cluster state via gossip. Knows leader directly. | No extra hops. Fast failover. | Client needs gossip library. Complex client. |
| **DNS-based** | DNS record points to current leader. Updated on failover. | Simple client. Standard DNS. | Slow propagation (TTL). Stale during failover. |
| **Client seed list** | Client has list of all nodes. Tries each until finds leader. | Simple. No infrastructure needed. | Slow failover (must try all nodes). |

**Recommendation: Protocol redirect (Phase 1) + optional proxy (Phase 2)**

Rationale:
- Protocol redirect is what rqlite does (HTTP 301), adapted to our custom binary protocol
- Minimal client complexity — client just needs retry logic with new address
- No external infrastructure (no DNS, no gossip client)
- Add a new opcode: `REDIRECT` with leader address payload
- In Phase 2, add proxy mode for clients that can't handle redirects (e.g., simple scripts)

### 4.2 Protocol Changes

New opcode needed:
```
REDIRECT (0x20) — Response when client sends write to non-leader
  Payload: leader_host:leader_port (string)
```

Client flow:
1. Client connects to any known node
2. Sends PUBLISH, ACK, or other write command
3. If node is not leader: receives REDIRECT with leader address
4. Client reconnects to leader address
5. Client caches leader address for future requests
6. On connection error: try next node in seed list, handle new REDIRECT

Read operations (STATS, QUEUE_INFO) can be served by any node (eventual consistency acceptable for reads).

---

## 5. Queue Semantics Under Replication

### 5.1 At-Least-Once Delivery with Raft

qwer-q's current guarantee is at-least-once delivery. With Raft:

1. Producer publishes message to leader
2. Leader writes to Raft log, replicates to quorum
3. After quorum commit, message is in FSM state on majority of nodes
4. Leader delivers message to consumer
5. Consumer acks — ack goes through Raft (removes message from state)
6. After quorum commit of ack, message is permanently removed

**If leader crashes between step 3 and 5:** New leader has the message (it was committed), redelivers to a consumer. At-least-once preserved.

**If leader crashes between step 5 and 6:** Message was delivered but ack not committed. New leader redelivers. Duplicate delivery possible. At-least-once preserved. (This is the same behavior as non-clustered mode with visibility timeout.)

### 5.2 Message Ordering Across Replicas

With single-leader Raft, message ordering is naturally preserved:
- All publishes go through leader
- Raft log is strictly ordered
- FSM applies entries in order
- All replicas see the same order

This is actually **stronger** than the current single-node best-effort FIFO — Raft gives us a total order on all operations.

### 5.3 Split-Brain Handling

Raft's quorum requirement prevents split-brain by design:
- 3-node cluster: requires 2 nodes to agree (tolerates 1 failure)
- 5-node cluster: requires 3 nodes to agree (tolerates 2 failures)
- Partitioned minority cannot elect a leader, stops accepting writes
- Partitioned majority continues operating normally
- On partition heal: minority catches up from leader's log

**No split-brain is possible** as long as quorum requirements are met. This is Raft's core guarantee.

### 5.4 Consumer Behavior During Failover

When leader fails:
1. Raft elects new leader (typically 1-5 seconds with default timeouts)
2. New leader has all committed state (queue contents, schemas)
3. Connected consumers lose TCP connection to old leader
4. Consumers reconnect (to any node), get redirected to new leader
5. New leader begins delivering from its local state
6. In-flight messages (delivered but not acked) are re-visible after visibility timeout

**Consumer groups (future):** When consumer groups are added, group membership and offset tracking must also go through Raft. On failover, the new leader rebalances consumers based on replicated group state.

### 5.5 Durability Window

With Raft, the durability guarantee is stronger than single-node:
- Single-node: message durable after fsync (configurable interval, default 100ms)
- Clustered: message durable after quorum commit (in-memory on majority, fsynced per node's sync interval)
- A message committed to Raft log on 2-of-3 nodes survives any single node failure, even before fsync

This means the `--sync-interval` trade-off becomes less critical in clustered mode — even with a longer sync interval, data survives single-node crashes because it exists on multiple nodes.

---

## 6. Recommended Implementation Plan

### Phase 1: Basic Raft Clustering (2-3 weeks)

**Goal:** 3-node cluster with leader election, replicated publish/ack, client redirect.

**Components:**
1. FSM implementation wrapping existing broker state
2. hashicorp/raft integration with raft-boltdb log store
3. Cluster bootstrap and join protocol
4. REDIRECT opcode for client leader discovery
5. CLI flags: `--join`, `--peers`, `--node-id`, `--raft-addr`
6. Docker Compose example for 3-node cluster

**What goes through Raft:**
- Publish, Ack, Nack
- Queue create/delete
- Schema register/update
- DLQ operations

**What stays local:**
- Consumer connections and message delivery
- Read operations (stats, queue list)
- Metrics collection

**Estimated changes:**
- New package: `internal/cluster/` (~500-800 lines)
- Modified: `internal/broker/broker.go` (FSM interface, ~200 lines)
- Modified: `internal/protocol/` (REDIRECT opcode, ~50 lines)
- Modified: `cmd/qwer-q/` (CLI flags, ~100 lines)
- New: Docker Compose 3-node example

### Phase 2: Production Hardening (2-3 weeks)

**Goal:** Robust clustering suitable for production use.

1. Gossip-based peer discovery (hashicorp/memberlist)
2. Migrate Raft log store from raft-boltdb to raft-wal
3. Snapshot optimization (efficient state transfer for new nodes)
4. Proxy mode (forward writes to leader transparently)
5. Graceful leadership transfer on shutdown
6. Cluster health endpoint (which nodes are up, who is leader)
7. Read consistency levels (strong/eventual)

### Phase 3: Advanced Features (3-4 weeks)

**Goal:** Operational excellence and performance optimization.

1. Multi-queue Raft groups (optional per-queue replication factor)
2. Witness/non-voting replicas (for read scaling)
3. Cross-datacenter awareness (prefer local reads)
4. Raft log batching (amortize fsync across multiple messages)
5. Combined data+consensus replication (NATS-style optimization)
6. Auto-scaling cluster membership

---

## 7. Risk Factors & Mitigation

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| Raft latency degrades throughput | High | Medium | Batch Raft commits. Current 2-5K msgs/sec has headroom. Benchmark early. |
| FSM bugs cause state divergence | Critical | Low | Extensive testing. Deterministic replay. Checksum FSM state periodically. |
| Snapshot transfer slow for large queues | Medium | Medium | Incremental snapshots. Stream snapshot data. Limit queue size. |
| Client library complexity increases | Medium | High | Keep redirect simple. Provide reference client updates. |
| BadgerDB + BoltDB in same process | Low | Low | Separate data directories. Independent tuning. Both are embedded, no port conflicts. |
| Docker networking complicates clustering | Medium | High | Document Docker Compose setup. Test with host networking and overlay networks. |
| hashicorp/raft license change | Medium | Low | MPL-2.0 license is permissive for our use. Monitor for changes (BSL trend at HashiCorp). |

---

## 8. Go Library Recommendations

### Primary Stack

| Purpose | Library | Version | Stars | Last Active |
|---|---|---|---|---|
| Consensus | `hashicorp/raft` | v1.7.3 | 8,913 | Jan 2026 |
| Raft Log (Phase 1) | `hashicorp/raft-boltdb` | v2 | - | Official |
| Raft Log (Phase 2) | `hashicorp/raft-wal` | v0.4.2 | - | Jan 2025 |
| Peer Discovery | `hashicorp/memberlist` | latest | 4,009 | Jan 2026 |
| App Data | `dgraph-io/badger/v4` | (existing) | - | Existing |

### Why Not Others

| Library | Why Not |
|---|---|
| `etcd-io/raft` | Minimal API — must build transport, snapshot, log store yourself. Good for experts (CockroachDB), overkill complexity for qwer-q. |
| `lni/dragonboat` | Stale releases (last: Sep 2023). v4 in development with no stable release. Multi-group is premature for our scale. Smaller community. |
| `BBVA/raft-badger` | Unmaintained (last commit May 2023, 120 stars). Using BadgerDB for both app data and Raft log creates coupling risk. |
| Embedded NATS | Embedding NATS server is possible but means qwer-q becomes a NATS wrapper, not its own MQ. Defeats the purpose. |

---

## 9. Architecture Decision: Single Raft Group vs Multi-Raft

**Decision: Single Raft group for Phase 1-2. Multi-Raft as Phase 3 option.**

### Single Raft Group (Recommended Start)
- One leader for all queues on the cluster
- Simpler implementation, simpler operations
- Leader is single point for writes (scales vertically with hardware)
- Sufficient for qwer-q's target throughput (10-50K msgs/sec)
- This is how rqlite works, and it handles thousands of writes/sec

### Multi-Raft (Future)
- Each queue (or group of queues) gets its own Raft group
- Different queues can have leaders on different nodes (write distribution)
- Needed when: single leader can't handle write volume across all queues
- CockroachDB, NATS JetStream, RabbitMQ all use multi-Raft
- Adds: group management, resource allocation, more complex failure handling

**Revisit trigger:** Single leader write throughput exceeds node capacity, or users need per-queue replication factor configuration.

---

## 10. Docker-First Clustering

Maintaining the "docker run and it works" philosophy with clustering:

### Single Node (Unchanged)
```bash
docker run -p 9876:9876 qwer-q
```
Works exactly as today. No clustering, no Raft overhead.

### 3-Node Cluster
```yaml
# docker-compose.yml
services:
  node1:
    image: qwer-q
    command: serve --node-id=node1 --raft-addr=node1:9877 --bootstrap
    ports: ["9876:9876"]
  node2:
    image: qwer-q
    command: serve --node-id=node2 --raft-addr=node2:9877 --join=node1:9877
  node3:
    image: qwer-q
    command: serve --node-id=node3 --raft-addr=node3:9877 --join=node1:9877
```

**Key:** Single node mode has zero clustering overhead. Raft only activates when `--join` or `--bootstrap` flags are used.

---

## 11. References

### Documentation
- [hashicorp/raft GitHub](https://github.com/hashicorp/raft)
- [hashicorp/raft documentation](https://github.com/hashicorp/raft/blob/main/docs/README.md)
- [hashicorp/memberlist GitHub](https://github.com/hashicorp/memberlist)
- [hashicorp/raft-wal GitHub](https://github.com/hashicorp/raft-wal)
- [NATS JetStream Clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering/jetstream_clustering)
- [RabbitMQ Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)
- [Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)
- [CockroachDB Replication Layer](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer)
- [rqlite GitHub](https://github.com/rqlite/rqlite)

### Reference Implementations
- [hraftd](https://github.com/otoolep/hraftd) — Minimal hashicorp/raft example
- [rqlite](https://github.com/rqlite/rqlite) — Distributed SQLite using hashicorp/raft (17K stars)
- [BBVA/raft-badger](https://github.com/BBVA/raft-badger) — BadgerDB as Raft log store

### Deep Dives
- [Quorum Queues Internals — CloudAMQP](https://www.cloudamqp.com/blog/quorum-queues-internals-a-deep-dive.html)
- [NATS Raft Consensus — DeepWiki](https://deepwiki.com/nats-io/nats-server/4.1-raft-consensus)
- [Joint Consensus in CockroachDB](https://www.cockroachlabs.com/blog/joint-consensus-raft/)
- [Raft Consensus Algorithm](https://raft.github.io/)
- [What is Multi-Raft](https://sergeiturukin.com/2017/06/09/multiraft.html)
- [Creating Distributed KV Database with Raft in Go](https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28)

### Papers & Analysis
- [Performance Analysis of Raft for Private Blockchains](https://arxiv.org/pdf/1808.01081)
- [Evaluating Persistent Replicated Message Queues](https://softwaremill.com/mqperf/)
- [RabbitMQ vs Kafka Fault Tolerance](https://jack-vanlightly.com/blog/2018/9/2/rabbitmq-vs-kafka-part-6-fault-tolerance-and-high-availability-with-kafka)
