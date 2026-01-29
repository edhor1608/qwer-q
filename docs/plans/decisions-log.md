# Decision Log

Architectural decisions with context and rationale.

---

## DEC-001: Queue-First Mental Model
**Date:** 2025-01-29
**Status:** Decided

### Context
Message queues exist on a spectrum:
- **Queue** (RabbitMQ-style): Messages consumed and deleted
- **Stream/Log** (Kafka-style): Messages persisted, replayable

### Decision
Queue-first. Stream features as optional later mode.

### Rationale
- Primary use case is "typed actions" — gateway calling services
- This maps to work distribution, not event sourcing
- Simpler implementation, faster to v1
- Stream semantics can be layered on without breaking the core model

### Consequences
- No replay by default
- No consumer offsets to track
- Simpler storage model (delete after ack)

---

## DEC-002: Embedded Durable Storage
**Date:** 2025-01-29
**Status:** Decided

### Context
Options considered:
1. In-memory only (simplest, not production-ready)
2. Embedded DB (single binary, durable)
3. Pluggable backends (flexible, more config)

### Decision
Embedded durable storage using BadgerDB or bbolt.

### Rationale
- Production-viable from day one
- Still single binary, single container
- Not much harder than in-memory in Go
- Avoids "this is just a toy" perception
- No external dependencies

### Consequences
- Slightly larger binary
- Need to handle storage edge cases (disk full, corruption)
- Need backup/restore story eventually

---

## DEC-003: Simple Competing Consumers
**Date:** 2025-01-29
**Status:** Decided

### Context
Consumer models:
1. Simple competing — round-robin to connected consumers
2. Consumer groups — named groups with membership tracking
3. Both — simple default, groups optional

### Decision
Simple competing consumers for v1.

### Rationale
- Primary use case is work distribution to service pool
- Consumer groups add complexity (membership, rebalancing, heartbeats)
- Can add groups later when users ask
- Keeps v1 scope tight

### Consequences
- No "multiple subscriber types" in v1
- Simpler broker state
- Less coordination logic

---

## DEC-004: Go as Implementation Language
**Date:** 2025-01-29
**Status:** Decided

### Context
Considered: Go, Rust, Zig

### Decision
Go

### Rationale
- Faster iteration with AI-native development (Claude Code)
- Simpler mental model vs Rust lifetimes/ownership
- Excellent story for shipping static binaries
- Great Docker DX
- Strong concurrency primitives
- Can introduce Rust for hot paths later if needed

### Consequences
- Slightly higher memory usage than Rust
- GC pauses (manageable with tuning)
- Large ecosystem of libraries

---

## DEC-005: Visibility Timeout for Redelivery
**Date:** 2025-01-29
**Status:** Decided

### Context
When a consumer receives a message but crashes before ack:
1. Visibility timeout — message invisible for N seconds, then requeued (SQS-style)
2. Explicit nack/requeue — consumer must nack, crash = lost
3. Persistent connection — connection drop = immediate requeue

### Decision
Visibility timeout (SQS-style).

### Rationale
- Simple to understand and implement
- Works even with dumb clients
- Consumer crash = automatic retry after timeout
- Timeout configurable per queue or per consume

### Consequences
- Need to track "invisible until" timestamp per in-flight message
- Consumers should ack quickly or extend visibility
- Potential for duplicate processing if consumer slow (at-least-once)

---

## DEC-006: Custom Binary Protocol
**Date:** 2025-01-29
**Status:** Decided

### Context
Options:
1. gRPC — well-understood, good tooling, codegen
2. Custom binary over TCP — maximum control, build everything
3. HTTP/2 + JSON — simple, less efficient
4. WebSocket — good for web, more complexity

### Decision
Custom binary protocol over TCP.

### Rationale
- Maximum control over every byte
- Optimize in real detail
- Aligns with "build a real alternative, not a wrapper"
- Learning opportunity

### Consequences
- Must design: frame format, command set, versioning, error codes
- Must build client libraries from scratch
- More work, but more control
- Can optimize for specific use cases

---

## DEC-007: Protobuf for Schema Format
**Date:** 2025-01-29
**Status:** Decided

### Context
Options for defining message types:
1. Protobuf — binary, compact, mature tooling
2. JSON Schema — human-readable, web-native
3. Custom DSL — maximum control
4. TypeScript-first — great TS DX, narrows audience

### Decision
Protobuf.

### Rationale
- Already building custom protocol — don't also build schema language
- Battle-tested compatibility rules (backward/forward)
- Binary efficiency matches "optimize in detail" mindset
- Codegen for Go/TS/Python exists
- Schema registry can store .proto files or compiled descriptors

### Consequences
- Users need protoc or buf for codegen
- Schema registry stores protobuf descriptors
- Wire format is protobuf-encoded messages
