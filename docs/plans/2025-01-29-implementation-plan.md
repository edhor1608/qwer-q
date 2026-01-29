# QWER-Q Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a typed, docker-first message queue with custom binary protocol, embedded storage, and schema validation.

**Architecture:** Single Go binary containing: TCP server with custom framed protocol, embedded BadgerDB for persistence, protobuf-based schema registry, and CLI interface. Queue-first semantics with visibility timeout for at-least-once delivery.

**Tech Stack:** Go 1.22+, BadgerDB, Protocol Buffers, ULID, Prometheus client

---

## Milestone Overview

| Milestone | Description | Outcome |
|-----------|-------------|---------|
| **M0** | Project setup | Go module, directory structure, tooling |
| **M1** | Wire protocol | Frame format, encoder/decoder, tests |
| **M2** | In-memory broker | Pub/sub/ack working in memory |
| **M3** | Persistence | BadgerDB integration, crash recovery |
| **M4** | Schema registry | Protobuf validation, registration |
| **M5** | CLI | Server + schema commands |
| **M6** | Advanced features | Request/reply, DLQ, idempotency |
| **M7** | Observability | Metrics, health, logging |
| **M8** | Docker & release | Dockerfile, CI, v0.1 release |

---

## M0: Project Setup

### Task 0.1: Initialize Go Module

**Files:**
- Create: `go.mod`
- Create: `go.sum`

**Step 1: Initialize module**

```bash
cd /Users/jonas/repos/qwer-q
go mod init github.com/jonas/qwer-q
```

**Step 2: Verify**

```bash
cat go.mod
```

Expected: Module path shown

**Step 3: Commit**

```bash
git add go.mod
git commit -m "chore: initialize Go module"
```

---

### Task 0.2: Create Directory Structure

**Files:**
- Create: `cmd/qwer-q/main.go`
- Create: `internal/protocol/protocol.go`
- Create: `internal/broker/broker.go`
- Create: `internal/storage/storage.go`
- Create: `internal/schema/schema.go`
- Create: `pkg/client/client.go`

**Step 1: Create directories and placeholder files**

```bash
mkdir -p cmd/qwer-q internal/protocol internal/broker internal/storage internal/schema pkg/client
```

**Step 2: Create main.go placeholder**

```go
// cmd/qwer-q/main.go
package main

import "fmt"

func main() {
	fmt.Println("qwer-q message queue")
}
```

**Step 3: Create internal package placeholders**

```go
// internal/protocol/protocol.go
package protocol

// Wire protocol encoding/decoding
```

```go
// internal/broker/broker.go
package broker

// Core broker logic
```

```go
// internal/storage/storage.go
package storage

// Persistence layer
```

```go
// internal/schema/schema.go
package schema

// Schema registry
```

```go
// pkg/client/client.go
package client

// Client library (thin core)
```

**Step 4: Verify it builds**

```bash
go build ./...
```

Expected: No errors

**Step 5: Commit**

```bash
git add .
git commit -m "chore: create project directory structure"
```

---

### Task 0.3: Add Makefile

**Files:**
- Create: `Makefile`

**Step 1: Create Makefile**

```makefile
.PHONY: build test run clean

BINARY=bin/qwer-q

build:
	go build -o $(BINARY) ./cmd/qwer-q

test:
	go test -v ./...

run: build
	./$(BINARY)

clean:
	rm -rf bin/

lint:
	golangci-lint run

.DEFAULT_GOAL := build
```

**Step 2: Test it works**

```bash
make build
./bin/qwer-q
```

Expected: "qwer-q message queue"

**Step 3: Add bin/ to gitignore**

Append to `.gitignore`:
```
bin/
```

**Step 4: Commit**

```bash
git add Makefile .gitignore
git commit -m "chore: add Makefile"
```

---

### Task 0.4: Add Core Dependencies

**Files:**
- Modify: `go.mod`

**Step 1: Add dependencies**

```bash
go get github.com/oklog/ulid/v2
go get github.com/dgraph-io/badger/v4
go get github.com/prometheus/client_golang/prometheus
go get github.com/spf13/cobra
go get google.golang.org/protobuf
```

**Step 2: Tidy**

```bash
go mod tidy
```

**Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add core dependencies"
```

---

## M1: Wire Protocol

### Protocol Design

Before implementing, here's the wire protocol spec:

```
Frame Format:
+----------+----------+----------+------------------+
| Length   | Version  | OpCode   | Payload          |
| 4 bytes  | 1 byte   | 1 byte   | (Length-2) bytes |
+----------+----------+----------+------------------+

Length: uint32, big-endian, includes Version + OpCode + Payload (excludes itself)
Version: uint8, protocol version (start at 1)
OpCode: uint8, command type
Payload: protobuf-encoded command-specific data
```

**OpCodes (v1):**

| Code | Name | Direction | Description |
|------|------|-----------|-------------|
| 0x01 | PUBLISH | C→S | Publish message to queue |
| 0x02 | PUBLISH_ACK | S→C | Publish confirmed |
| 0x03 | CONSUME | C→S | Start consuming from queue |
| 0x04 | MESSAGE | S→C | Deliver message to consumer |
| 0x05 | ACK | C→S | Acknowledge message |
| 0x06 | NACK | C→S | Negative ack (requeue) |
| 0x07 | ERROR | S→C | Error response |
| 0x10 | SCHEMA_REGISTER | C→S | Register schema |
| 0x11 | SCHEMA_GET | C→S | Get schema |
| 0x12 | SCHEMA_RESPONSE | S→C | Schema data |
| 0x20 | CALL | C→S | Request/reply publish |
| 0x21 | CALL_RESPONSE | S→C | Reply to CALL |

---

### Task 1.1: Define Protocol Constants

**Files:**
- Create: `internal/protocol/opcodes.go`
- Test: `internal/protocol/opcodes_test.go`

**Step 1: Write the test**

```go
// internal/protocol/opcodes_test.go
package protocol

import "testing"

func TestOpCodeValues(t *testing.T) {
	// Verify opcodes don't accidentally change
	if OpPublish != 0x01 {
		t.Errorf("OpPublish = %x, want 0x01", OpPublish)
	}
	if OpError != 0x07 {
		t.Errorf("OpError = %x, want 0x07", OpError)
	}
}

func TestOpCodeString(t *testing.T) {
	if OpPublish.String() != "PUBLISH" {
		t.Errorf("OpPublish.String() = %s, want PUBLISH", OpPublish.String())
	}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./internal/protocol/... -v
```

Expected: FAIL (OpPublish undefined)

**Step 3: Write implementation**

```go
// internal/protocol/opcodes.go
package protocol

// ProtocolVersion is the current wire protocol version.
const ProtocolVersion uint8 = 1

// OpCode represents a protocol operation code.
type OpCode uint8

// Client → Server operations
const (
	OpPublish        OpCode = 0x01
	OpConsume        OpCode = 0x03
	OpAck            OpCode = 0x05
	OpNack           OpCode = 0x06
	OpSchemaRegister OpCode = 0x10
	OpSchemaGet      OpCode = 0x11
	OpCall           OpCode = 0x20
)

// Server → Client operations
const (
	OpPublishAck     OpCode = 0x02
	OpMessage        OpCode = 0x04
	OpError          OpCode = 0x07
	OpSchemaResponse OpCode = 0x12
	OpCallResponse   OpCode = 0x21
)

var opCodeNames = map[OpCode]string{
	OpPublish:        "PUBLISH",
	OpPublishAck:     "PUBLISH_ACK",
	OpConsume:        "CONSUME",
	OpMessage:        "MESSAGE",
	OpAck:            "ACK",
	OpNack:           "NACK",
	OpError:          "ERROR",
	OpSchemaRegister: "SCHEMA_REGISTER",
	OpSchemaGet:      "SCHEMA_GET",
	OpSchemaResponse: "SCHEMA_RESPONSE",
	OpCall:           "CALL",
	OpCallResponse:   "CALL_RESPONSE",
}

func (op OpCode) String() string {
	if name, ok := opCodeNames[op]; ok {
		return name
	}
	return "UNKNOWN"
}
```

**Step 4: Run test to verify it passes**

```bash
go test ./internal/protocol/... -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add internal/protocol/
git commit -m "feat(protocol): define opcodes and protocol version"
```

---

### Task 1.2: Implement Frame Encoder

**Files:**
- Create: `internal/protocol/frame.go`
- Test: `internal/protocol/frame_test.go`

**Step 1: Write the test**

```go
// internal/protocol/frame_test.go
package protocol

import (
	"bytes"
	"testing"
)

func TestEncodeFrame(t *testing.T) {
	payload := []byte("hello")
	frame := EncodeFrame(OpPublish, payload)

	// Length = 1 (version) + 1 (opcode) + 5 (payload) = 7
	// Frame = 4 (length) + 7 = 11 bytes
	if len(frame) != 11 {
		t.Errorf("frame length = %d, want 11", len(frame))
	}

	// Check length field (big-endian)
	length := uint32(frame[0])<<24 | uint32(frame[1])<<16 | uint32(frame[2])<<8 | uint32(frame[3])
	if length != 7 {
		t.Errorf("length field = %d, want 7", length)
	}

	// Check version
	if frame[4] != ProtocolVersion {
		t.Errorf("version = %d, want %d", frame[4], ProtocolVersion)
	}

	// Check opcode
	if OpCode(frame[5]) != OpPublish {
		t.Errorf("opcode = %x, want %x", frame[5], OpPublish)
	}

	// Check payload
	if !bytes.Equal(frame[6:], payload) {
		t.Errorf("payload = %v, want %v", frame[6:], payload)
	}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./internal/protocol/... -v -run TestEncodeFrame
```

Expected: FAIL (EncodeFrame undefined)

**Step 3: Write implementation**

```go
// internal/protocol/frame.go
package protocol

import "encoding/binary"

// EncodeFrame creates a wire-format frame.
// Format: [4-byte length][1-byte version][1-byte opcode][payload]
func EncodeFrame(op OpCode, payload []byte) []byte {
	// Length = version (1) + opcode (1) + payload
	length := uint32(2 + len(payload))

	frame := make([]byte, 4+length)
	binary.BigEndian.PutUint32(frame[0:4], length)
	frame[4] = ProtocolVersion
	frame[5] = byte(op)
	copy(frame[6:], payload)

	return frame
}
```

**Step 4: Run test to verify it passes**

```bash
go test ./internal/protocol/... -v -run TestEncodeFrame
```

Expected: PASS

**Step 5: Commit**

```bash
git add internal/protocol/
git commit -m "feat(protocol): implement frame encoder"
```

---

### Task 1.3: Implement Frame Decoder

**Files:**
- Modify: `internal/protocol/frame.go`
- Modify: `internal/protocol/frame_test.go`

**Step 1: Write the test**

```go
// Add to internal/protocol/frame_test.go

func TestDecodeFrame(t *testing.T) {
	original := []byte("test payload")
	encoded := EncodeFrame(OpMessage, original)

	reader := bytes.NewReader(encoded)
	frame, err := DecodeFrame(reader)
	if err != nil {
		t.Fatalf("DecodeFrame error: %v", err)
	}

	if frame.Version != ProtocolVersion {
		t.Errorf("version = %d, want %d", frame.Version, ProtocolVersion)
	}
	if frame.OpCode != OpMessage {
		t.Errorf("opcode = %x, want %x", frame.OpCode, OpMessage)
	}
	if !bytes.Equal(frame.Payload, original) {
		t.Errorf("payload = %v, want %v", frame.Payload, original)
	}
}

func TestDecodeFrameMaxSize(t *testing.T) {
	// Create a frame claiming to be too large
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, MaxFrameSize+1)

	reader := bytes.NewReader(buf)
	_, err := DecodeFrame(reader)
	if err != ErrFrameTooLarge {
		t.Errorf("error = %v, want ErrFrameTooLarge", err)
	}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./internal/protocol/... -v -run TestDecodeFrame
```

Expected: FAIL (DecodeFrame undefined)

**Step 3: Write implementation**

```go
// Add to internal/protocol/frame.go

import (
	"encoding/binary"
	"errors"
	"io"
)

// MaxFrameSize is the maximum allowed frame size (16 MB).
const MaxFrameSize = 16 * 1024 * 1024

var (
	ErrFrameTooLarge = errors.New("frame exceeds maximum size")
	ErrFrameTooSmall = errors.New("frame too small for header")
)

// Frame represents a decoded protocol frame.
type Frame struct {
	Version uint8
	OpCode  OpCode
	Payload []byte
}

// DecodeFrame reads a frame from the reader.
func DecodeFrame(r io.Reader) (*Frame, error) {
	// Read length
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf)

	if length > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if length < 2 {
		return nil, ErrFrameTooSmall
	}

	// Read rest of frame
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return &Frame{
		Version: data[0],
		OpCode:  OpCode(data[1]),
		Payload: data[2:],
	}, nil
}
```

**Step 4: Run test to verify it passes**

```bash
go test ./internal/protocol/... -v -run TestDecodeFrame
```

Expected: PASS

**Step 5: Commit**

```bash
git add internal/protocol/
git commit -m "feat(protocol): implement frame decoder"
```

---

### Task 1.4: Define Message Types (Protobuf)

**Files:**
- Create: `proto/qwerq.proto`
- Create: `internal/protocol/messages.pb.go` (generated)

**Step 1: Create proto file**

```protobuf
// proto/qwerq.proto
syntax = "proto3";

package qwerq;

option go_package = "github.com/jonas/qwer-q/internal/protocol";

// PublishRequest is sent by client to publish a message.
message PublishRequest {
  string queue = 1;
  bytes payload = 2;
  map<string, string> headers = 3;
  optional string message_id = 4;      // Client-provided, or broker generates
  optional string idempotency_key = 5; // For deduplication
}

// PublishResponse confirms a publish.
message PublishResponse {
  string message_id = 1;
}

// ConsumeRequest starts consuming from a queue.
message ConsumeRequest {
  string queue = 1;
  uint32 prefetch = 2;           // Max unacked messages (default 1)
  uint32 visibility_timeout = 3; // Seconds before redelivery (default 30)
}

// Message is delivered to consumers.
message Message {
  string message_id = 1;
  string queue = 2;
  bytes payload = 3;
  map<string, string> headers = 4;
  uint32 attempt = 5;           // Delivery attempt number (1-based)
  int64 published_at = 6;       // Unix timestamp millis
}

// AckRequest acknowledges a message.
message AckRequest {
  string message_id = 1;
}

// NackRequest negatively acknowledges (requeue).
message NackRequest {
  string message_id = 1;
  bool requeue = 2;  // If false, send to DLQ
}

// ErrorResponse is sent on errors.
message ErrorResponse {
  uint32 code = 1;
  string message = 2;
}

// SchemaRegisterRequest registers a schema for a queue.
message SchemaRegisterRequest {
  string queue = 1;
  bytes descriptor = 2;  // Protobuf FileDescriptorSet
  string message_type = 3; // Fully qualified message name
}

// SchemaRegisterResponse confirms registration.
message SchemaRegisterResponse {
  uint32 schema_id = 1;
  uint32 version = 2;
}

// CallRequest is RPC-style publish with reply.
message CallRequest {
  string queue = 1;
  bytes payload = 2;
  map<string, string> headers = 3;
  uint32 timeout_ms = 4;  // How long to wait for reply
}

// CallResponse is the reply to a Call.
message CallResponse {
  bytes payload = 1;
  map<string, string> headers = 2;
}
```

**Step 2: Generate Go code**

```bash
mkdir -p proto
# Save the proto file first, then:
protoc --go_out=. --go_opt=paths=source_relative proto/qwerq.proto
```

Note: If protoc not installed, run:
```bash
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

**Step 3: Move generated file**

```bash
mv proto/qwerq.pb.go internal/protocol/messages.pb.go
```

**Step 4: Verify it compiles**

```bash
go build ./...
```

Expected: No errors

**Step 5: Commit**

```bash
git add proto/ internal/protocol/messages.pb.go
git commit -m "feat(protocol): define protobuf message types"
```

---

### Task 1.5: Protocol Roundtrip Test

**Files:**
- Create: `internal/protocol/protocol_test.go`

**Step 1: Write integration test**

```go
// internal/protocol/protocol_test.go
package protocol

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestPublishRoundtrip(t *testing.T) {
	// Create a publish request
	req := &PublishRequest{
		Queue:   "orders",
		Payload: []byte(`{"id": 123}`),
		Headers: map[string]string{"trace_id": "abc123"},
	}

	// Encode to protobuf
	payload, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	// Encode to frame
	frame := EncodeFrame(OpPublish, payload)

	// Decode frame
	decoded, err := DecodeFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify opcode
	if decoded.OpCode != OpPublish {
		t.Errorf("opcode = %v, want %v", decoded.OpCode, OpPublish)
	}

	// Decode protobuf
	var result PublishRequest
	if err := proto.Unmarshal(decoded.Payload, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	// Verify fields
	if result.Queue != req.Queue {
		t.Errorf("queue = %s, want %s", result.Queue, req.Queue)
	}
	if !bytes.Equal(result.Payload, req.Payload) {
		t.Errorf("payload mismatch")
	}
	if result.Headers["trace_id"] != "abc123" {
		t.Errorf("headers mismatch")
	}
}
```

**Step 2: Run test**

```bash
go test ./internal/protocol/... -v -run TestPublishRoundtrip
```

Expected: PASS

**Step 3: Commit**

```bash
git add internal/protocol/
git commit -m "test(protocol): add roundtrip integration test"
```

---

## M2: In-Memory Broker (Outline)

> Detailed tasks for M2-M8 will be elaborated when we reach them.

### Task 2.1: Message and Queue Types
- Define internal Message struct with ULID
- Define Queue struct with thread-safe operations

### Task 2.2: Broker Core
- Broker struct holding queues map
- CreateQueue, GetQueue, DeleteQueue

### Task 2.3: Publish Handler
- Receive PublishRequest
- Generate ULID if no ID
- Enqueue message
- Return PublishResponse

### Task 2.4: Consume Handler
- Receive ConsumeRequest
- Register consumer on queue
- Start delivery loop with visibility timeout

### Task 2.5: Ack/Nack Handler
- Remove message from in-flight on ack
- Requeue or DLQ on nack

### Task 2.6: TCP Server
- Accept connections
- Frame reader/writer per connection
- Route opcodes to handlers

### Task 2.7: Integration Test
- Start broker
- Publish message
- Consume and ack
- Verify message removed

---

## M3: Persistence (Outline)

### Task 3.1: Storage Interface
### Task 3.2: BadgerDB Implementation
### Task 3.3: Queue Persistence
### Task 3.4: Message Persistence
### Task 3.5: Recovery on Startup
### Task 3.6: Persistence Tests

---

## M4: Schema Registry (Outline)

### Task 4.1: Schema Storage
### Task 4.2: Registration Handler
### Task 4.3: Compatibility Checker
### Task 4.4: Message Validation
### Task 4.5: Schema Tests

---

## M5: CLI (Outline)

### Task 5.1: Cobra Setup
### Task 5.2: `serve` Command
### Task 5.3: `schema register` Command
### Task 5.4: `schema list` Command
### Task 5.5: `queue list` Command

---

## M6: Advanced Features (Outline)

### Task 6.1: Request/Reply (CALL)
### Task 6.2: Dead Letter Queue
### Task 6.3: Idempotency Dedup
### Task 6.4: Backpressure (Max Queue Size)
### Task 6.5: Visibility Timeout Extension

---

## M7: Observability (Outline)

### Task 7.1: Prometheus Metrics
### Task 7.2: Health Endpoint
### Task 7.3: Structured Logging
### Task 7.4: Startup Banner + Warnings

---

## M8: Docker & Release (Outline)

### Task 8.1: Dockerfile
### Task 8.2: docker-compose.yml Example
### Task 8.3: GitHub Actions CI
### Task 8.4: Release v0.1.0

---

## Appendix: Error Codes

| Code | Name | Description |
|------|------|-------------|
| 1000 | UNKNOWN_ERROR | Unexpected error |
| 1001 | INVALID_FRAME | Malformed frame |
| 1002 | UNKNOWN_OPCODE | Unsupported operation |
| 2001 | QUEUE_NOT_FOUND | Queue doesn't exist |
| 2002 | QUEUE_FULL | Backpressure limit reached |
| 2003 | NO_SCHEMA | Queue has no registered schema |
| 3001 | SCHEMA_INVALID | Invalid protobuf descriptor |
| 3002 | SCHEMA_INCOMPATIBLE | Breaks compatibility |
| 3003 | MESSAGE_INVALID | Payload doesn't match schema |
| 4001 | DUPLICATE_MESSAGE | Idempotency key already seen |

---

## Appendix: Configuration (Future)

```yaml
# qwer-q.yaml (optional)
server:
  port: 9876
  metrics_port: 9877

storage:
  path: /var/lib/qwer-q/data

defaults:
  visibility_timeout: 30s
  max_retries: 3
  max_queue_size: 1000000

security:
  enabled: false
  # token: "secret"
```
