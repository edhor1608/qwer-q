---
title: Configuration
description: All QWER-Q configuration flags, environment variables, and defaults.
---

QWER-Q follows a "docker run and it works" philosophy. Configuration is primarily via CLI flags, with a few environment variable overrides.

## Broker Flags (`qwer-q serve`)

| Flag | Default | Description |
|------|---------|-------------|
| `--port` / `-p` | `9876` | TCP port for the broker protocol |
| `--metrics-port` | `9877` | HTTP port for Prometheus metrics and health endpoint |
| `--data-dir` | `/data` | Directory for BadgerDB persistence. Set empty to disable persistence (in-memory only). |
| `--max-message-size` | `1MB` | Maximum allowed message payload size. Accepts suffixes: `B`, `KB`, `MB`, `GB`. |
| `--batch-interval` | `0` | Write batch flush interval. `0` disables batching. |
| `--auth-token` | _(empty)_ | Shared token required for client authentication. |
| `--schema-mode` | `permissive` | Schema enforcement mode: `permissive` (no schema required) or `strict` (schema required before publish). |
| `--cluster-node-id` | _(empty)_ | Enable cluster mode for this node (preview). |
| `--cluster-bind` | `0.0.0.0:9878` | Raft bind address (preview). |
| `--cluster-advertise` | _(empty)_ | Raft advertise address (defaults to bind) (preview). |
| `--cluster-peers` | _(none)_ | Initial peers in `id=host:port` format (preview). |
| `--cluster-data-dir` | `<data-dir>/raft` | Raft data directory (preview). |
| `--cluster-bootstrap` | `false` | Bootstrap a new cluster (preview). |

## Environment Variable Overrides

| Variable | Equivalent Flag | Description |
|----------|------------------|-------------|
| `QWERQ_AUTH_TOKEN` | `--auth-token` | Shared token required for client authentication |
| `QWERQ_SCHEMA_MODE` | `--schema-mode` | Schema enforcement mode (`permissive` or `strict`) |

## Size Format

The `--max-message-size` flag accepts human-readable sizes:

| Input | Bytes |
|-------|-------|
| `512` or `512B` | 512 |
| `512KB` | 524,288 |
| `1MB` | 1,048,576 |
| `1GB` | 1,073,741,824 |

Case-insensitive (`kb`, `KB`, `Kb` all work). Maximum value: ~4 GB (uint32 limit).

## Internal Defaults

These values are compiled into the broker and not currently configurable via flags. They are documented here for operational awareness.

### Queue Defaults

| Setting | Value | Description |
|---------|-------|-------------|
| Max queue size | 10,000 messages | Publishes are rejected when a queue reaches this limit |
| Default visibility timeout | 30 seconds | How long a delivered message stays invisible |
| Max retries | 5 attempts | Delivery attempts before the failure policy kicks in |
| Failure policy | `dlq` | Move to dead letter queue after max retries (`dlq`, `drop`, or `infinite`) |

### Storage Defaults

| Setting | Value | Description |
|---------|-------|-------------|
| Sync interval | 100ms | How often BadgerDB fsyncs to disk. Trade-off: lower = safer, higher = faster. |
| Memtable count | 2 | BadgerDB in-memory tables (reduced from default 5 to save memory) |
| Memtable size | 32 MB | Size of each memtable |
| Value log file size | 64 MB | BadgerDB value log segment size |
| Block cache size | 32 MB | BadgerDB read cache |
| Index cache size | 16 MB | BadgerDB index cache |
| Value threshold | 1 KB | Values smaller than this are stored inline in the LSM tree |
| Compression | None | Disabled for CPU efficiency |

### Protocol Defaults

| Setting | Value | Description |
|---------|-------|-------------|
| Protocol version | 1 | Current wire protocol version |
| Max frame size | 16 MB | Maximum wire frame size (hard limit) |
| Default message size | 1 MB | Default max message payload (configurable) |

### Memory Management

| Setting | Value | Description |
|---------|-------|-------------|
| Memory limit | 400 MB | Total Go heap allocation limit before rejecting publishes |
| Memory check interval | 100ms | How often memory stats are sampled |
| Large message threshold | 64 KB | Messages larger than this trigger eager memory checks |

### Idempotency

| Setting | Value | Description |
|---------|-------|-------------|
| Dedup TTL | 5 minutes | How long idempotency keys are tracked |
| Max tracked keys | 100,000 | Forced cleanup when this many keys are tracked |
| Cleanup interval | 10 seconds | How often expired keys are pruned |

### Request/Reply (CALL)

| Setting | Value | Description |
|---------|-------|-------------|
| Default timeout | 30 seconds | CALL timeout if not specified by client |
| Reply queue max size | 1,000 | Maximum pending replies per connection |
| Reply queue prefix | `_reply.` | Prefix for auto-created reply queues |

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 9876 | TCP | Broker protocol (custom binary) |
| 9877 | HTTP | Prometheus metrics (`/metrics`) and health check (`/health`) |
| 9878 | TCP | Raft transport in cluster mode (preview) |

## Docker Volumes

| Path | Purpose |
|------|---------|
| `/data` | BadgerDB data directory. Mount a volume here for persistence across container restarts. |

## Performance Tuning

### Write Batching Trade-offs

`--batch-interval` controls how long writes are accumulated before a flush.

| `--batch-interval` | Effect |
|--------------------|--------|
| `0` | No batching (lower throughput, simplest durability semantics) |
| `5ms` to `20ms` | Common throughput boost with low extra delay |
| `100ms+` | Higher throughput, larger buffering window |

Badger's sync interval is currently compile-time (`internal/storage/badger.go`, default `100ms`).

### Container Sizing

| Container Memory | Recommended Use |
|-----------------|-----------------|
| 256 MB | Development/testing only |
| 512 MB | Default. Good for moderate workloads. |
| 1 GB+ | High-throughput production workloads |

The broker's 400 MB memory limit is designed for a 512 MB container, leaving ~100 MB headroom for BadgerDB caches and Go runtime overhead.
