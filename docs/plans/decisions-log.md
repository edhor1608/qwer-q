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

---

## DEC-008: Open by Default, Opt-in Auth
**Date:** 2025-01-29
**Status:** Decided

### Context
Security model options:
1. Open by default, opt-in auth — best DX, risk of running open in prod
2. Secure by default, opt-out for dev — safer, worse first-run DX
3. Auto-detect environment — magic that can surprise

### Decision
Open by default, opt-in auth.

### Rationale
- "docker run and it works" is the headline feature
- Similar to Redis, NATS default behavior
- Clear warnings in logs when running without auth
- Auth enabled via config file or env vars

### Consequences
- Must print clear warnings: "Running without auth - not for production"
- Document secure setup prominently
- Auth options: token-based, mTLS (later)

---

## DEC-009: Prometheus Metrics, Web UI Later
**Date:** 2025-01-29
**Status:** Decided

### Context
Observability options:
1. Prometheus `/metrics` endpoint — industry standard
2. Built-in web dashboard — self-contained
3. Structured logs only — simplest, limited visibility
4. Both metrics + web UI — best visibility, more scope

### Decision
Prometheus `/metrics` endpoint for v1. Web UI as follow-up feature.

### Rationale
- `/metrics` is minimal code (~100 lines)
- Everyone has Prometheus or compatible scraper
- Web UI adds scope — defer to v1.1 or later
- Structured JSON logs included regardless

### Consequences
- Expose `/metrics` on separate HTTP port (or same with path routing)
- Track: queue depth, publish rate, consume rate, ack rate, latency histograms
- Provide example Grafana dashboard in docs

### Follow-up: Built-in Web UI (post-v1)
- Simple dashboard showing queue stats, connected clients
- No external dependencies (embedded static assets)
- Optional — disabled by default or separate binary

---

## DEC-010: CLI + API for Schema Registration
**Date:** 2025-01-29
**Status:** Decided

### Context
How users register message types:
1. API-first — programmatic, requires code
2. File-based — drop files, broker watches
3. CLI tool — explicit, good for CI/CD
4. Embedded in publish — auto-register, implicit magic

### Decision
CLI tool as primary interface, API as underlying mechanism.

### Rationale
- CLI is explicit — fits CI/CD pipelines
- API underneath enables programmatic use when needed
- Avoids implicit magic that's hard to debug
- File-watching can be added later if requested

### Consequences
- Build `qwer-q` CLI with schema subcommands
- Commands: `schema register`, `schema list`, `schema get`, `schema check`
- API exposed via custom protocol (schema management commands)
- Schema stored in embedded DB alongside queue data

---

## DEC-011: Broker-Side Schema Validation
**Date:** 2025-01-29
**Status:** Decided

### Context
Where to validate messages against schema:
1. Producer-side only — fast, but rogue clients can send garbage
2. Broker-side only — guaranteed correctness, CPU cost
3. Both — safest, redundant work
4. Configurable per queue — flexible, more config

### Decision
Broker-side validation as authoritative. Client libs validate as convenience (fail-fast optimization).

### Rationale
- "Typed MQ" promise means broker enforces contracts
- Protobuf validation is fast — not a real bottleneck
- Trusting clients breaks the guarantee
- Client libs can validate to fail fast, but broker is source of truth

### Consequences
- Broker parses and validates every published message
- Invalid messages rejected with clear error (schema mismatch, field type, etc.)
- Client libs should validate before send (optimization, better errors)
- Slight CPU overhead on broker — acceptable for correctness

---

## DEC-012: Backward Compatible Schema Evolution
**Date:** 2025-01-29
**Status:** Decided

### Context
Schema compatibility rules when updating:
1. Backward — new schema reads old messages
2. Forward — old schema reads new messages
3. Full — both directions
4. None — breaking changes allowed
5. Configurable per schema

### Decision
Backward compatible as default.

### Rationale
- Most common real-world pattern
- Protobuf naturally supports this (optional fields, field numbers)
- Consumers can upgrade before producers
- Prevents accidental breaking changes

### Consequences
- Schema registry checks compatibility on update
- Allowed: add optional fields, deprecate fields
- Rejected: remove fields, change field types, renumber fields
- Can add "full" or "none" modes later if needed

---

## DEC-013: Auto-Create Queues with Schema Binding
**Date:** 2025-01-29
**Status:** Decided

### Context
Queue creation model:
1. Explicit only — must create before use
2. Auto-create on first publish — zero setup, typo risk
3. Auto-create with schema binding — dynamic but controlled

### Decision
Auto-create queues only when schema is registered for that queue name.

### Rationale
- Typed MQ means every queue has a schema
- Register schema first (explicit) → queue auto-creates on publish
- Typo in queue name? Rejected — no schema registered
- Convenience of auto-create with safety of explicit binding

### Consequences
- Schema registration binds schema to queue name
- First publish to queue creates it if schema exists
- Publish to unknown queue name → error
- Provides typo protection and enforces schema-first workflow

---

## DEC-014: Configurable Failed Message Handling, DLQ Default
**Date:** 2025-01-29
**Status:** Decided

### Context
What happens when messages fail repeatedly:
1. Dead letter queue — move to DLQ after N retries
2. Drop — discard after N retries
3. Infinite retries — keep trying forever
4. Configurable per queue

### Decision
Configurable per queue, with dead letter queue (DLQ) as default.

### Rationale
- DLQ is industry standard, preserves failed messages
- Some use cases may want drop (high-volume, ephemeral)
- Some may want infinite (must-process guarantees)
- Sensible default with escape hatches

### Consequences
- Default: after N retries (configurable, default 3), move to `<queue>.dlq`
- DLQ auto-created with same schema as source queue
- Queue config options: `dlq` (default), `drop`, `infinite`
- DLQ is a regular queue — can consume, inspect, replay

---

## DEC-015: Built-in Request/Reply Primitive
**Date:** 2025-01-29
**Status:** Decided

### Context
The "typed actions" vision implies request/response, not just fire-and-forget:
1. Correlation ID + reply queue — classic, clunky
2. Built-in CALL command — broker handles plumbing
3. Leave to gateway layer — keeps broker simple

### Decision
Built-in request/reply primitive at broker level.

### Rationale
- "Type-safe RPC over durable queue" is a killer feature
- Gateway can use this for QWER-Q, implement manually for other brokers
- Doesn't break two-project split — enhances it
- Great standalone DX

### Consequences
- New command: `CALL` (publish + wait for response)
- Broker manages correlation IDs and reply routing
- Timeout handling built-in
- Response schema can be part of queue schema definition

---

## DEC-016: Best-Effort FIFO Ordering
**Date:** 2025-01-29
**Status:** Decided

### Context
Message ordering with multiple consumers:
1. Best-effort FIFO — no strict guarantees
2. Ordering key — same key to same consumer
3. Strict single-consumer mode

### Decision
Best-effort FIFO for v1. Ordering keys as future feature.

### Rationale
- Multiple consumers inherently break strict ordering
- Most use cases tolerate this
- Keeps v1 simple
- Can add ordering keys in v1.1 if users need it

### Consequences
- Messages delivered roughly in order within a queue
- No guarantees with competing consumers
- Redelivered messages go to back of queue

### Follow-up: Ordering Keys (post-v1)
- Messages with same key routed to same consumer
- Enables ordered processing for related messages
- Similar to Kafka partition keys

---

## DEC-017: Max Queue Size with Reject on Full
**Date:** 2025-01-29
**Status:** Decided

### Context
Backpressure when queue grows unbounded:
1. Unlimited — disk fills eventually
2. Max size, reject on full — clear error
3. Oldest-drop — lossy
4. Producer blocking — TCP backpressure

### Decision
Configurable max queue size. Reject publishes when full.

### Rationale
- Clear error to producer (can handle/retry)
- No silent data loss
- Predictable resource usage
- Default can be generous (e.g., 1M messages or 1GB)

### Consequences
- Queue config: `max_messages` and/or `max_bytes`
- Publish to full queue returns error
- Producer responsible for backoff/retry
- Monitoring alert: "queue near capacity"

---

## DEC-018: Message ID — Broker Default, Client Override
**Date:** 2025-01-29
**Status:** Decided

### Context
Who generates unique message identifier:
1. Broker-generated — guaranteed unique
2. Client-generated — client controls
3. Both — client optional, broker fills in

### Decision
Broker generates by default (ULID), client can override.

### Rationale
- ULID preferred (sortable, contains timestamp)
- Client-provided enables idempotency patterns
- Guaranteed uniqueness when client doesn't care

### Consequences
- Broker generates ULID if no ID provided
- Client can set `message_id` field on publish
- ID used for dedup if idempotency enabled

---

## DEC-019: Free-Form Message Headers
**Date:** 2025-01-29
**Status:** Decided

### Context
Should messages support arbitrary metadata:
1. Free-form headers — map of string→string
2. No headers — embed in payload
3. Typed headers only — predefined set

### Decision
Free-form string→string headers.

### Rationale
- Essential for distributed tracing (trace_id, span_id)
- Gateway can pass context without touching payload
- HTTP/gRPC have them, users expect them
- Opaque to broker — just passes through

### Consequences
- Message format includes headers map
- Broker preserves headers, doesn't interpret
- Reserved prefix (e.g., `_qwer_`) for broker-set headers

---

## DEC-020: Opt-In Idempotency via Dedup Key
**Date:** 2025-01-29
**Status:** Decided

### Context
Handling duplicate publishes:
1. Client-provided dedup key — opt-in
2. Automatic dedup on message ID — always on
3. Consumer's problem — no broker dedup

### Decision
Client-provided dedup key, opt-in.

### Rationale
- No overhead for fire-and-forget use cases
- Client controls dedup semantics (order ID, request ID)
- Message ID can serve as dedup key
- Configurable TTL window

### Consequences
- Optional `idempotency_key` field on publish
- Broker tracks keys for configurable TTL (default 5 min)
- Duplicate key within window → rejected with specific error
- No dedup overhead when not used

---

## DEC-021: Layered Client Libraries
**Date:** 2025-01-29
**Status:** Decided

### Context
How sophisticated should client libs be:
1. Thin — minimal wrapper, user handles everything
2. Smart — auto-reconnect, pooling, validation, retries
3. Layered — thin core + optional batteries

### Decision
Layered: thin core + optional smart layer.

### Rationale
- Thin core gives gateway project clean foundation
- Power users get low-level control
- Smart layer provides DX for direct users
- Gateway will build its own smart layer anyway

### Consequences
- Core package: connection, protocol, basic pub/sub
- Batteries package: reconnect, pooling, typed wrappers, retry
- Gateway project builds on core, may use or replace batteries
- Documentation covers both levels
