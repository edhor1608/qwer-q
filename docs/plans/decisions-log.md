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
