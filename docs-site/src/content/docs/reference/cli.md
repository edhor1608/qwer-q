---
title: CLI Reference
description: Complete reference for all qwer-q CLI commands, flags, and examples.
---

The `qwer-q` CLI is the primary interface for managing the broker and interacting with queues.

## Global Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--broker` | `-b` | `localhost:9876` | Broker address (host:port) |
| `--version` | | | Print version and exit |
| `--help` | `-h` | | Show help |

## `qwer-q serve`

Start the broker server.

```bash
qwer-q serve [flags]
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--port` | `-p` | `9876` | Broker TCP port |
| `--metrics-port` | | `9877` | Metrics/health HTTP port |
| `--data-dir` | | `/data` | Data directory for BadgerDB persistence |
| `--max-message-size` | | `1MB` | Maximum message payload size (e.g., `1MB`, `512KB`, `2GB`) |
| `--schema-mode` | | `permissive` | Schema enforcement mode: `permissive` or `strict` |

### Examples

```bash
# Start with defaults
qwer-q serve

# Custom ports
qwer-q serve --port 5555 --metrics-port 5556

# Custom data directory (e.g., for local development)
qwer-q serve --data-dir ./my-data

# Allow larger messages
qwer-q serve --max-message-size 10MB

# Require schemas before publish
qwer-q serve --schema-mode strict
```

---

## `qwer-q queue list`

List all queues with their current message counts.

```bash
qwer-q queue list [flags]
```

### Output

```
NAME        MESSAGES  IN-FLIGHT
orders      42        3
tasks       0         0
orders.dlq  2         0
```

### Examples

```bash
# List queues on local broker
qwer-q queue list

# List queues on remote broker
qwer-q queue list -b prod-broker:9876
```

---

## `qwer-q schema register`

Register a Protobuf schema for a queue. The queue is created automatically when the first message is published.

```bash
qwer-q schema register [flags]
```

### Required Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--queue` | `-q` | Queue name to bind the schema to |
| `--proto` | `-p` | Path to the `.proto` file |
| `--message` | `-m` | Fully qualified Protobuf message name |

### Prerequisites

Requires `protoc` (the Protocol Buffer compiler) to be installed and available in your PATH. The CLI uses `protoc` to compile the `.proto` file into a `FileDescriptorSet`.

### Examples

```bash
# Register a schema
qwer-q schema register -q orders -p order.proto -m "myapp.Order"

# Output:
# Schema registered for queue "orders" (version 1)

# Update a schema (must be backward compatible)
qwer-q schema register -q orders -p order_v2.proto -m "myapp.Order"

# Output:
# Schema registered for queue "orders" (version 2)
```

---

## `qwer-q schema list`

List all registered schemas.

```bash
qwer-q schema list [flags]
```

### Output

```
QUEUE    MESSAGE TYPE  VERSION
orders   myapp.Order   2
tasks    myapp.Task    1
```

### Examples

```bash
# List schemas on local broker
qwer-q schema list

# List schemas on remote broker
qwer-q schema list -b prod-broker:9876
```
