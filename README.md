# QWER-Q

A typed, docker-first message queue.

## Quick Start

```bash
# Docker (recommended)
docker run -p 9876:9876 -p 9877:9877 ghcr.io/jonas/qwer-q

# Or build from source
go install github.com/jonas/qwer-q/cmd/qwer-q@latest
qwer-q serve
```

## Why?

- **Kafka is too heavy** - Complex ops, multiple components
- **NATS is too minimal** - Missing advanced guarantees
- **Typing breaks** - tRPC gives great DX for HTTP, but MQ loses type safety

QWER-Q fills the gap: simple deployment, real durability, types as a first-class feature.

## Features

- Single binary, single container
- Built-in schema registry with protobuf validation
- Configurable schema enforcement mode: `permissive` (default) or `strict`
- At-least-once delivery with visibility timeouts
- Dead letter queues for failed messages
- Consumer groups and ordering keys
- Stream mode (preview)
- Raft-based clustering (preview)
- Token auth (`--auth-token` / `QWERQ_AUTH_TOKEN`)
- REST API + embedded dashboard on metrics port
- Prometheus metrics endpoint

## Architecture

```
+------------------+
|   TCP Server     |  :9876
|   (Binary Proto) |
+--------+---------+
         |
+--------v---------+
|     Broker       |
| +------+------+  |
| |Schema|Dedup |  |
| |Reg.  |      |  |
| +------+------+  |
| | Queues/Stream| |
| +-------------+  |
+--------+---------+
         |
+--------v---------+
|  BadgerDB Store  |
+------------------+

+------------------+
|  HTTP Server     |  :9877
| metrics/api/ws   |
| dashboard        |
+------------------+

+------------------+
|  Raft (optional) |  :9878
+------------------+
```

## CLI Commands

```bash
# Start the broker
qwer-q serve [flags]
  -p, --port int          broker port (default 9876)
      --metrics-port int  metrics port (default 9877)
      --data-dir string   data directory for persistence
      --schema-mode string  schema enforcement: permissive|strict (default permissive)
      --batch-interval duration  write batch flush interval (default 0 = off)
      --auth-token string  require client auth token
      --cluster-node-id string   enable clustering mode (preview)
      --cluster-bind string      raft bind addr (default 0.0.0.0:9878)
      --cluster-advertise string raft advertised addr
      --cluster-peers strings    initial peers id=host:port
      --cluster-data-dir string  raft data dir
      --cluster-bootstrap        bootstrap new cluster

# List queues
qwer-q queue list
  -b, --broker string     broker address (default "localhost:9876")

# Schema management
qwer-q schema list
qwer-q schema register -q <queue> -p <proto-file> -m <message-type>
  -q, --queue string      queue name (required)
  -p, --proto string      proto file path (required)
  -m, --message string    message type (required)
```

## Docker Compose

```yaml
version: "3.8"
services:
  qwer-q:
    image: ghcr.io/jonas/qwer-q
    ports:
      - "9876:9876"
      - "9877:9877"
    volumes:
      - qwer-q-data:/data
volumes:
  qwer-q-data:
```

## Wire Protocol

Binary protocol over TCP with the following frame format:

```
+--------+--------+--------+------------+
| Opcode | Flags  | Length | Payload    |
| 1 byte | 1 byte | 4 bytes| N bytes    |
+--------+--------+--------+------------+
```

Operations: PUBLISH, CONSUME, ACK, NACK, CALL, SCHEMA_REGISTER, QUEUE_LIST, etc.

## Metrics

Prometheus metrics available at `:9877/metrics`:

- `qwerq_messages_published_total` - Total published messages
- `qwerq_messages_consumed_total` - Total consumed messages
- `qwerq_messages_acked_total` - Total acknowledged messages
- `qwerq_messages_nacked_total` - Total negative-acknowledged messages
- `qwerq_queue_depth` - Current queue depth by queue name
- `qwerq_in_flight_count` - Messages currently in-flight

## Goals

- Single binary, single container
- `docker run` and it works
- Typed queue contracts (runtime schema enforcement, strict mode available)
- At-least-once delivery
- Sub-millisecond latency
- Durable (survives restarts)

## Non-Goals (Current)

- Exactly-once delivery
- Built-in mTLS (not implemented yet)
- Managed cloud service
- App-framework gateway concerns inside the broker (separate companion project)

## Documentation

See [docs/plans/](docs/plans/) for design documents.

## License

Apache 2.0

<!-- status:start -->
## Status
- State: active
- Summary: Define current milestone.
- Next: Define next concrete step.
- Updated: 2026-02-21
- Branch: `codex/docs-truth-sync-v020`
- Working Tree: dirty (3 files)
- Last Commit: 7b0af20 (2026-02-19) docs: address third-round PR20 markdown review nits
<!-- status:end -->
