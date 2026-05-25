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
**Status:** Superseded by DEC-026

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

### Superseded Notes (2026-02-12)
This decision was later generalized to support two runtime modes:
- `permissive`: publish without schema is allowed (default)
- `strict`: publish without schema is rejected

See DEC-026.

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

---

## DEC-022: Queue Size 100k Default (Historical Proposal)
**Date:** 2026-02-01
**Status:** Superseded by DEC-027

### Context
Original `DefaultMaxQueueSize = 10,000` was too conservative. Benchmarks showed queue filling in ~2 seconds at 5k msgs/sec, causing 19k errors.

Original math assumed 10KB messages with 3x BadgerDB overhead. Reality: most messages are 1KB or less.

### Decision
Historical proposal: increase `DefaultMaxQueueSize` from 10,000 to 100,000 messages.

### Rationale
- 100k × 1KB × 3x overhead = 300MB, safe for 512MB container
- Allows for burst absorption when consumer lags
- Still provides backpressure protection
- Benchmark improvement: 960/s → 2,258/s (+135%)

### Consequences
- Higher memory usage under load (intended)
- More messages buffered before rejection
- May need tuning for smaller containers

### Superseded Notes (2026-02-18)
Current implementation baseline is `DefaultMaxQueueSize = 10_000`.
This entry is retained as historical exploration, not current behavior.

---

## DEC-023: Consumer Channel Buffer 100 (Historical Proposal)
**Date:** 2026-02-01
**Status:** Not adopted

### Context
Consumer channel had buffer of 1, limiting prefetch to single message. Consumer had to ack before receiving next message.

### Decision
Historical proposal: increase consumer channel buffer from 1 to 100 messages.

### Rationale
- Enables prefetching — consumer can receive messages while processing
- Reduces round-trips between broker and consumer
- Standard pattern in message queues (RabbitMQ prefetch, SQS batch)
- Combined with queue size increase for +135% throughput

### Consequences
- 100 messages can be in-flight per consumer
- Higher memory per consumer connection
- Faster message delivery

### Notes (2026-02-18)
Current queue consumer channel buffer remains `1`. This entry documents a considered optimization, not shipped behavior.

---

## DEC-024: Configurable Sync Interval
**Date:** 2026-02-01
**Status:** Decided

### Context
BadgerDB sync (fsync) was happening every write, limiting throughput. Options:
1. Sync every write — maximum durability, minimum throughput
2. Periodic sync — trade durability window for throughput
3. Manual sync — maximum throughput, application controls

### Decision
Historical proposal: configurable sync interval via `--sync-interval` CLI flag. Default 100ms (safe). Benchmarks use 1s.

### Rationale
- 1s sync is standard for databases (PostgreSQL default)
- Trade-off: may lose up to interval of data on crash
- Benchmark: 100ms → 1s gave 2,258/s → 4,846/s (+115%)
- User controls based on durability requirements

### Consequences
- New CLI flag: `--sync-interval`
- Default: 100ms (reasonable durability)
- Benchmarking: 1s (prioritize throughput)
- Production critical: 0 (sync every write)

### Notes
Current implementation now exposes `--sync-interval` on `serve`, with a default of `100ms` and `0` meaning sync every write.

---

## DEC-025: Memory Pressure Backpressure
**Date:** 2026-02-01
**Status:** Decided

### Context
With larger queue size, memory can grow significantly. Need mechanism to reject messages before OOM.

### Decision
Monitor memory usage, reject publishes with "memory pressure" error when high.

### Rationale
- Prevents OOM crashes
- Clear error message for client to retry
- Works in addition to queue size limits
- Container-aware (respects memory limits)

### Consequences
- New error: "memory pressure: server under load, try again later"
- Broker remains responsive under load
- Clients should implement backoff on this error

---

## DEC-026: Dual Schema Enforcement Modes
**Date:** 2026-02-12
**Status:** Decided

### Context
The product needs to support two valid use cases:
1. Fast adoption and ad-hoc queues without mandatory schema setup
2. Schema-first workflows with hard publish-time guarantees

The codebase and docs had diverged between these two models.

### Decision
Support explicit schema enforcement modes:
- `permissive` (default): publish is allowed even if no schema is registered
- `strict`: publish is rejected when no schema is registered

In both modes, if a schema is registered, payload validation is enforced.

### Rationale
- Preserves ease-of-use for standalone queue workflows
- Enables strict typed contracts where required
- Avoids forcing one workflow across all deployments
- Keeps behavior explicit via configuration (`--schema-mode`)

### Consequences
- New broker setting: `schema-mode` (`permissive|strict`)
- CLI/env support: `--schema-mode`, `QWERQ_SCHEMA_MODE`
- Documentation must describe mode-dependent queue creation and publish behavior
- Tests must cover both permissive and strict semantics

---

## DEC-027: Truth-Synced Operational Baseline
**Date:** 2026-02-18
**Status:** Decided

### Context
Roadmap and decision docs had drifted from shipped behavior after rapid feature merges.

### Decision
Use code-as-source-of-truth for operational defaults and claim docs must reflect implemented behavior:
- Default queue size: `10_000`
- Consumer channel buffer: `1`
- Sync interval: compile-time default `100ms`
- Runtime throughput control: `--batch-interval`
- Stream/clustering marked preview until hardening gates are complete

### Rationale
- Prevent credibility drift between docs and implementation
- Make operational behavior unambiguous for new users
- Separate historical experiments from shipped guarantees

### Consequences
- Decision log entries that are exploratory or superseded must be explicitly labeled
- Docs and README must classify features as stable vs preview
- Benchmark and performance claims must remain reproducible and conservative

---

## DEC-028: Parallel Optimization Lab
**Date:** 2026-04-13
**Status:** Decided

### Context
QWER-Q has clear single-node queue-path performance debt, but the possible fixes span very different risk levels:
- conservative hot-path cleanup inside the existing Go + Badger architecture
- more invasive storage-path changes focused on queue-first durability and throughput
- possible future Rust exploration if Go experiments plateau

A single branch would mix these ideas and make it hard to measure or roll back individual bets.

### Decision
Run optimization work as a parallel lab with isolated git worktrees and branch-specific docs:
- coordinator branch: `perf/parallel-lab`
- hot-path branch/worktree: `perf/hotpath-lab` in `../qwer-q-hotpath`
- storage-path branch/worktree: `perf/storage-lab` in `../qwer-q-storage`

Use shared baseline benchmarks before each experiment branch diverges, then compare measured wins and keep only changes that improve the queue-first use case.

### Rationale
- Keeps aggressive experiments isolated and reviewable
- Allows apples-to-apples comparison against a common baseline
- Prevents one speculative branch from contaminating another
- Matches the project goal of doubling down on the queue-first path

### Consequences
- Every experiment branch must carry its own plan/results doc
- Benchmark commands and environment must stay consistent across branches
- Findings that do not outperform baseline should be documented and abandoned

---

## DEC-029: Per-Message Traffic Logs Default To Debug
**Date:** 2026-04-13
**Status:** Decided

### Context
The live queue benchmark exercises a real producer and consumer over TCP. The broker was logging every publish, delivery, and nack at `INFO`, which adds avoidable write-path overhead and produces the wrong default operator experience for a high-throughput queue.

### Decision
Demote per-message traffic logs from `INFO` to `DEBUG`:
- `message published`
- `message delivered`
- `message nacked`

Connection lifecycle logs remain at `INFO`.

### Rationale
- High-volume queue traffic should not spam default logs
- Per-message `INFO` logging distorts real throughput measurements
- Operators still have access to detailed traces by raising log verbosity deliberately

### Consequences
- Default broker logs become operationally quieter and cheaper
- Benchmarks reflect broker work more than log I/O
- Debug logging remains available for traffic-level investigations

---

## DEC-030: Keep Direct Frame Writes, Reject Early Decode Buffer Release
**Date:** 2026-04-13
**Status:** Decided

### Context
The wire path was doing an extra payload copy after protobuf marshal via `EncodeFrame`, and `DecodeFrame` had an apparent but unused pooling opportunity.

### Decision
Keep direct framed writes via `protocol.WriteFrame` in the hot client/server paths, but reject any early decode-buffer release until ownership semantics are redesigned.

### Rationale
- Direct frame writes remove an unnecessary payload copy with a simple local change
- The protocol microbench showed materially lower framing cost and allocated bytes
- Early decode-buffer release looked attractive but is unsafe because publish payloads and decoded client messages can outlive the frame buffer

### Consequences
- Hot request/delivery writes avoid building a second full frame buffer
- `DecodeFrame` pooling remains a future opportunity, not a safe current optimization
- Any future decode-buffer reuse work must prove field ownership explicitly before release

---

## DEC-031: Use Exact Wire-Compatible Fast Paths For The Simple Publish/Ack Path
**Date:** 2026-04-13
**Status:** Decided

### Context
The Go benchmark client exercises a narrow protobuf subset repeatedly:
- `Publish(queue, payload)`
- `Ack(messageID)`
- `Consume(queue, prefetch)`
- `PublishResponse{message_id}` on every publish ack

The generic protobuf encoder/decoder works, but it adds avoidable overhead on the hottest client/server path.

### Decision
Keep exact, wire-compatible encoders/decoders for the simple publish/ack path:
- server publish ack payload encoded directly
- client publish ack payload decoded via a fast path with fallback to generic protobuf decode
- client publish, consume, and ack requests encoded directly for the field sets that client actually sends

### Rationale
- The message shapes are small and stable
- The optimization is localized and easy to audit against the protobuf schema
- Protocol microbenches showed materially lower encode/decode cost for publish ack and ack request handling
- This preserves wire compatibility without changing the public client API

### Consequences
- The hot producer path depends on small exact encoders/decoders that must stay aligned with `proto/qwerq.proto`
- If the simple client path grows new fields, these fast paths must be updated or the code must fall back to generic protobuf marshaling
- The generic protobuf path remains available for non-hot or shape-changing cases

---

## DEC-034: The Go Client Must Support A Protobuf-Native Typed Workflow
**Date:** 2026-04-14
**Status:** Decided

### Context
QWER-Q's typed contract story is one of its main differentiators, but the Go client still forced users into a low-level byte workflow even when they were already using generated protobuf message types.

### Decision
Add a minimal protobuf-native typed layer to the Go client:
- publish generated protobuf messages directly
- consume and decode protobuf payloads directly
- register queue schemas from generated message descriptors

### Rationale
- Aligns the Go client with the product's typed queue identity
- Reduces byte-level glue code in real applications
- Strengthens the typed USP without introducing a large framework surface

### Consequences
- The Go client now owns a small descriptor-set builder based on protobuf reflection
- Typed client tests must cover the real strict-mode broker path
- Future client ergonomics work can build on this layer instead of reinventing typed publish/consume flows

---

## DEC-032: QWER-Q Is A Queue-First Typed Durable Broker
**Date:** 2026-04-13
**Status:** Decided

### Context
Optimization work had already improved the hot path, but the larger strategic risk was product drift: comparing QWER-Q to systems built for different categories and then optimizing toward their strengths instead of our own intended product.

### Decision
Anchor QWER-Q explicitly as:
- a queue-first broker
- typed at the broker boundary
- self-hosted and simple to operate
- richer than a minimal transport, lighter than Kafka-class platforms

Treat stream mode and clustering as preview capabilities, not the primary product identity.

### Rationale
- Prevents roadmap and benchmark drift
- Clarifies what QWER-Q should and should not try to be best at
- Keeps queue semantics, typed contracts, and product ergonomics at the center

### Consequences
- Future work should be judged against the queue-first typed broker identity
- Features that pull the product toward stream-platform or workflow-engine identity must be treated cautiously
- Product docs and benchmark framing should reflect this category explicitly

---

## DEC-033: Use Product-Shaped Benchmarks As The Main Scoreboard
**Date:** 2026-04-13
**Status:** Decided

### Context
Existing benchmarks measured useful pieces of the system, but they did not yet fully express the category QWER-Q wants to win.

### Decision
Adopt a layered benchmark charter:
- queue-core benchmark
- typed queue benchmark
- operator efficiency benchmark
- product scorecard

Implement the first product-shaped suite as a `queue-core` benchmark inside `bench/`, then use that scoreboard to drive both USP hardening and queue-engine work.

### Rationale
- Measures QWER-Q against its actual product promise
- Separates must-win comparisons from reference-only comparisons
- Gives future optimization work a strategic scoreboard instead of ad-hoc local maxima

### Consequences
- Raw throughput alone is no longer the primary product benchmark
- Benchmarks must state guarantees and competitor class clearly
- Optimization wins that do not improve product-shaped benchmarks should not drive roadmap direction

---

## DEC-035: Queue-Mode Consumer Groups Are Runtime Coordination

**Date:** 2026-05-25
**Status:** Decided

### Context

Queue-mode consumer groups currently fan out each published message to every
connected group, then distribute that group's copy across connected members.
That runtime behavior is useful for live work coordination, but persisting
named group subscriptions would add a second durable work ledger per group.

The existing product direction is queue-first rather than stream-first, and
DEC-003 kept consumer coordination small for the core queue model.

### Decision

Keep queue-mode consumer groups as runtime delivery coordination only.

Durable independent subscriptions are not in scope for the current queue-mode
contract. If users need durable named replay positions, they should use stream
mode offsets or a future durable-subscription PRD.

### Rationale

- Preserves the simple queue-first storage model: one durable queue ledger,
  delete-on-ack.
- Avoids surprising users with Kafka-like durable subscription semantics in
  the queue path.
- Keeps group membership, heartbeats, member assignment, and per-group
  in-flight state local to connected consumers.
- Leaves room for durable subscriptions as an explicit future product area
  instead of accidentally coupling them to queue ack/delete semantics.

### Consequences

- Restarting the broker drops queue-mode group membership and group fan-out
  state.
- Re-created groups do not receive historical per-group fan-out from before
  restart.
- A group ack deletes the underlying durable queue message.
- Consumer-group retry attempt durability is not a separate persistence
  requirement unless durable subscriptions are later scoped.

---

## DEC-036: Visibility-Timeout Attempts Are Runtime-Only

**Date:** 2026-05-25
**Status:** Decided

### Context

Visibility timeout redelivery is driven by the broker's background reaper.
Persisting the delivery attempt counter every time a timeout expires would put
storage writes on that background path and can amplify writes under slow or
crashing consumers.

Explicit nack/requeue is different: it is a client command and can persist the
updated retry state at that command boundary.

### Decision

Keep attempts caused only by visibility-timeout redelivery as runtime-only
state.

Timeout redelivery increments the attempt visible to clients while the broker
process is alive. If the broker restarts before the message is acked, the
message is recovered from durable queue storage and its timeout-only attempt
increments reset to the persisted queue state.

### Rationale

- Avoids unbounded write amplification from the reaper loop.
- Keeps the durable queue ledger focused on message existence and explicit
  command boundaries.
- Preserves at-least-once recovery: the message survives restart even though
  timeout-only attempt increments do not.
- Keeps explicit nack retry durability stronger than passive timeout retry
  accounting.

### Consequences

- A slow consumer can see attempt 2 after a visibility timeout in the same
  broker process.
- The same message can be delivered as attempt 1 after restart if the only
  previous increment came from visibility-timeout redelivery.
- Max-retry enforcement for timeout-only redeliveries is process-local until a
  future persisted timeout-attempt design is explicitly scoped.

---

## DEC-037: Queue Config Uses An Explicit MaxSize Presence Flag

**Date:** 2026-05-25
**Status:** Decided

### Context

Queue config records are JSON-encoded in Badger. `max_size: 0` is ambiguous in
old records because zero can mean either "field absent/default" or the queue
model's explicit unlimited size.

Existing records cannot be reliably reinterpreted without risking accidental
unlimited queues after upgrade.

### Decision

Treat legacy queue config records with `max_size: 0` and no presence marker as
unset/default.

Encode an explicit unlimited queue size as `max_size: 0` plus
`max_size_set: true`. Non-zero `max_size` values remain valid with or without
the marker for backward compatibility.

### Rationale

- Preserves existing Badger data safely.
- Adds one narrow presence flag instead of changing the storage format or
  making `QueueConfig` pointer-heavy.
- Keeps the queue contract unchanged: `MaxSize == 0` still means unlimited when
  explicitly configured.

### Consequences

- Old zero-valued queue config records recover with the default max queue size.
- New explicit unlimited queue configs recover as unlimited.
- Future zero-valued scalar config fields should get their own presence marker
  if they need to distinguish default from explicit zero.
