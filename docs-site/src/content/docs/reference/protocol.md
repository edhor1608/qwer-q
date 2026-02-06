---
title: Protocol Reference
description: QWER-Q wire protocol — frame format, opcodes, and Protobuf message definitions.
---

QWER-Q uses a custom binary protocol over TCP. Message payloads are encoded with Protocol Buffers (protobuf).

## Wire Format

Every message on the wire is a **frame** with this structure:

```
┌──────────────┬─────────┬────────┬─────────────┐
│ Length (4B)   │ Version │ OpCode │ Payload      │
│ uint32 BE    │ (1B)    │ (1B)   │ (variable)   │
└──────────────┴─────────┴────────┴─────────────┘
```

| Field | Size | Description |
|-------|------|-------------|
| Length | 4 bytes | Total size of Version + OpCode + Payload (big-endian uint32) |
| Version | 1 byte | Protocol version (currently `1`) |
| OpCode | 1 byte | Operation code (see table below) |
| Payload | Variable | Protobuf-encoded message (depends on opcode) |

**Limits:**
- Maximum frame size: 16 MB
- Maximum message payload size: 1 MB (configurable via `--max-message-size`)

## OpCodes

### Client to Server

| OpCode | Hex | Name | Payload | Response |
|--------|-----|------|---------|----------|
| `PUBLISH` | `0x01` | Publish a message | `PublishRequest` | `PublishResponse` via `PUBLISH_ACK` |
| `CONSUME` | `0x03` | Start consuming from a queue | `ConsumeRequest` | Messages streamed via `MESSAGE` |
| `ACK` | `0x05` | Acknowledge a message | `AckRequest` | None (fire-and-forget) |
| `NACK` | `0x06` | Negatively acknowledge | `NackRequest` | None |
| `EXTEND_VISIBILITY` | `0x08` | Extend visibility timeout | `ExtendVisibilityRequest` | `ExtendVisibilityResponse` via `EXTEND_VISIBILITY_ACK` |
| `SCHEMA_REGISTER` | `0x10` | Register a schema | `SchemaRegisterRequest` | `SchemaRegisterResponse` via `SCHEMA_RESPONSE` |
| `SCHEMA_GET` | `0x11` | Get a schema | `SchemaRegisterRequest` (queue field only) | `SchemaRegisterResponse` via `SCHEMA_RESPONSE` |
| `CALL` | `0x20` | RPC-style request/reply | `CallRequest` | `CallResponse` via `CALL_RESPONSE` |

### Server to Client

| OpCode | Hex | Name | Payload | Description |
|--------|-----|------|---------|-------------|
| `PUBLISH_ACK` | `0x02` | Publish confirmed | `PublishResponse` | Sent after successful publish |
| `MESSAGE` | `0x04` | Message delivery | `Message` | Pushed to consumers |
| `ERROR` | `0x07` | Error response | `ErrorResponse` | Sent on any error |
| `EXTEND_VISIBILITY_ACK` | `0x09` | Extension confirmed | `ExtendVisibilityResponse` | New visibility timestamp |
| `SCHEMA_RESPONSE` | `0x12` | Schema response | `SchemaRegisterResponse` | Schema info |
| `CALL_RESPONSE` | `0x21` | RPC reply | `CallResponse` | Reply to CALL |

### Admin Operations

| OpCode | Hex | Name | Payload | Response |
|--------|-----|------|---------|----------|
| `SCHEMA_LIST` | `0x30` | List all schemas | `SchemaListRequest` | `SchemaListResponse` via `SCHEMA_LIST_RESP` |
| `SCHEMA_LIST_RESP` | `0x31` | Schema list response | `SchemaListResponse` | — |
| `QUEUE_LIST` | `0x32` | List all queues | `QueueListRequest` | `QueueListResponse` via `QUEUE_LIST_RESP` |
| `QUEUE_LIST_RESP` | `0x33` | Queue list response | `QueueListResponse` | — |

## Error Codes

| Code | Meaning |
|------|---------|
| `1` | Unknown opcode |
| `2` | Invalid request (malformed protobuf) |
| `3` | Operation failed (publish error, internal error) |
| `4` | Message not found (ack/nack for unknown message) |
| `5` | Schema validation failed |
| `6` | Schema registration failed |
| `7` | Schema not found |
| `8` | Call timeout |

## Protobuf Message Definitions

All payloads use the following Protobuf definitions (from `proto/qwerq.proto`):

### PublishRequest

```protobuf
message PublishRequest {
  string queue = 1;
  bytes payload = 2;
  map<string, string> headers = 3;
  optional string message_id = 4;      // Client-provided, or broker generates ULID
  optional string idempotency_key = 5; // For deduplication (5 min window)
}
```

### PublishResponse

```protobuf
message PublishResponse {
  string message_id = 1;
}
```

### ConsumeRequest

```protobuf
message ConsumeRequest {
  string queue = 1;
  uint32 prefetch = 2;           // Max unacked messages (default 1)
  uint32 visibility_timeout = 3; // Seconds before redelivery (default 30)
}
```

### Message

Delivered to consumers when messages are available.

```protobuf
message Message {
  string message_id = 1;
  string queue = 2;
  bytes payload = 3;
  map<string, string> headers = 4;
  uint32 attempt = 5;           // Delivery attempt number (1-based)
  int64 published_at = 6;       // Unix timestamp millis
}
```

### AckRequest

```protobuf
message AckRequest {
  string message_id = 1;
}
```

### NackRequest

```protobuf
message NackRequest {
  string message_id = 1;
  bool requeue = 2;  // If false, send to DLQ (if DLQ policy)
}
```

### ExtendVisibilityRequest / Response

```protobuf
message ExtendVisibilityRequest {
  string message_id = 1;
  uint32 extension_seconds = 2;  // Additional seconds to extend
}

message ExtendVisibilityResponse {
  int64 new_visible_at = 1;  // New visibility timestamp (Unix millis)
}
```

### CallRequest / CallResponse

```protobuf
message CallRequest {
  string queue = 1;
  bytes payload = 2;
  map<string, string> headers = 3;
  uint32 timeout_ms = 4;  // How long to wait for reply (default 30s)
}

message CallResponse {
  bytes payload = 1;
  map<string, string> headers = 2;
}
```

### SchemaRegisterRequest / Response

```protobuf
message SchemaRegisterRequest {
  string queue = 1;
  bytes descriptor = 2;  // Protobuf FileDescriptorSet
  string message_type = 3; // Fully qualified message name
}

message SchemaRegisterResponse {
  uint32 schema_id = 1;
  uint32 version = 2;
}
```

### ErrorResponse

```protobuf
message ErrorResponse {
  uint32 code = 1;
  string message = 2;
}
```

### Admin Messages

```protobuf
message SchemaListRequest {}

message SchemaInfo {
  string queue = 1;
  string message_type = 2;
  uint32 version = 3;
}

message SchemaListResponse {
  repeated SchemaInfo schemas = 1;
}

message QueueListRequest {}

message QueueInfo {
  string name = 1;
  uint32 message_count = 2;
  uint32 in_flight_count = 3;
}

message QueueListResponse {
  repeated QueueInfo queues = 1;
}
```

## Connection Lifecycle

1. Client opens TCP connection to broker (default port 9876)
2. Client sends frames; broker responds with frames
3. For consuming: client sends `CONSUME` once, then broker pushes `MESSAGE` frames as they become available
4. Client sends `ACK` or `NACK` for each received message
5. Connection closes when either side closes the TCP socket
6. On disconnect, the broker removes the consumer and any in-flight messages become visible again after their timeout expires

## Buffer Pools

The protocol implementation uses tiered buffer pools to minimize garbage collection pressure:

| Pool | Size | Use Case |
|------|------|----------|
| Small | up to 1 KB | ACK/NACK, small responses |
| Medium | up to 16 KB | Typical messages |
| Large | up to 64 KB | Larger payloads |
| XLarge | up to 256 KB | Large messages |

Messages exceeding 256 KB are allocated on the heap without pooling.
