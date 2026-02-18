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
- Request/reply (RPC) pattern support
- Prometheus metrics endpoint
- Sub-millisecond latency

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
| |    Queues   |  |
| +-------------+  |
+--------+---------+
         |
+--------v---------+
|  BadgerDB Store  |
+------------------+

+------------------+
|  Metrics Server  |  :9877
|   (Prometheus)   |
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
      - qwer-q-data:/var/lib/qwer-q
    environment:
      - QWERQ_DATA_DIR=/var/lib/qwer-q
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
- `qwerq_inflight_messages` - Messages currently in-flight

## Goals

- Single binary, single container
- `docker run` and it works
- Typed queue contracts (schema registry built-in, strict mode available)
- At-least-once delivery
- Sub-millisecond latency
- Durable (survives restarts)

## Non-Goals (v1)

- Exactly-once delivery
- Stream/log semantics (Kafka-style replay)
- Multi-node clustering
- Consumer groups

## Documentation

See [docs/plans/](docs/plans/) for design documents.

## License

Apache 2.0
