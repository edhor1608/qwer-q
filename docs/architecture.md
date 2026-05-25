# QWER-Q Architecture

QWER-Q is a queue-first broker for durable async work with typed message
contracts. Its stable product identity is queue mode, embedded storage, auth,
metrics, REST API, dashboard, DLQ workflows, and simple self-hosting.

For product context and decisions, start with:

- [Product charter](plans/2026-04-13-product-charter.md)
- [Decision log](plans/decisions-log.md)
- [Durable queue state contract](plans/2026-05-25-durable-queue-state-contract.md)
- [Performance notes](PERFORMANCE.md)
- [Benchmark claims policy](benchmarks/CLAIMS-POLICY.md)

## Process Boundaries

QWER-Q runs as one broker process:

- TCP broker protocol on port `9876` by default.
- HTTP metrics, REST API, WebSocket stats, and embedded dashboard on port `9877`
  by default.
- BadgerDB storage in the configured data directory.
- Optional Raft clustering on a separate port when cluster flags are enabled.

No external database, Redis, Kafka, or Docker service is required for normal
broker development.

## Runtime Responsibilities

The runtime is organized around a small set of responsibilities:

- **CLI wiring:** starts the broker, storage, HTTP surface, optional auth,
  schema mode, and optional cluster node.
- **TCP server:** accepts connections, decodes frames, applies auth, tracks
  per-connection consume state, and dispatches protocol operations.
- **Protocol codec:** owns the binary frame format and protobuf payload
  encoding/decoding helpers.
- **Schema registry:** stores queue schema descriptors and validates publishes
  when a schema exists or strict mode requires one.
- **Broker:** owns queue lookup, publish/consume/ack/nack handling, DLQ moves,
  stream queues, idempotency, memory pressure, and background reapers.
- **Queue:** owns queue-mode runtime delivery state: pending messages,
  in-flight messages, consumers, ordering-key assignment, visibility timeout,
  retry policy, and consumer groups.
- **Storage:** persists queue metadata and durable messages in BadgerDB.
- **HTTP API:** exposes operator-facing queue, DLQ, schema, stats, consumer,
  and WebSocket surfaces on the metrics server.
- **Clients:** provide Go and TypeScript entry points over the TCP protocol.
- **Benchmarks:** live in a separate `bench` Go module and are measurement
  tooling, not broker runtime surface.

## Queue-Mode Message Lifecycle

Queue mode is the default mental model. Messages are consumed and deleted after
ack; replay is not part of queue mode.

1. A producer sends a publish frame over TCP.
2. The server decodes the frame into a publish request.
3. Auth is checked first when token auth is enabled.
4. Schema validation runs according to schema mode:
   - permissive mode allows queues without schemas.
   - strict mode requires a registered schema before publish.
   - registered schemas are enforced in both modes.
5. The broker checks memory pressure and idempotency.
6. The broker creates or finds the queue.
7. The queue accepts the message into runtime pending state and tries delivery
   to waiting consumers.
8. Storage persists the durable message state.
9. The server returns a publish ack with the message ID.

If a consumer is already waiting, delivery can happen immediately after enqueue.
If not, the message remains pending until a consumer subscribes.

## Consume, Ack, Nack

Consume registers runtime delivery state for the TCP connection.

- Ungrouped consumers compete for messages on the queue.
- Grouped consumers are runtime members of a named consumer group.
- Ordering keys route matching messages to a stable runtime assignment while
  consumers remain connected.

When a message is delivered:

- It moves from pending to in-flight runtime state.
- Its visibility timeout is set.
- The durable message remains in storage until ack or terminal movement.

Ack means the consumer has completed work:

- Runtime in-flight state is removed.
- Durable storage for the message is deleted.
- The message should not reappear after restart.

Nack means the consumer rejected the work:

- With requeue enabled, the message returns to pending runtime state and remains
  durable queue work.
- With DLQ policy, terminal failure moves the durable message from the original
  queue to the DLQ queue.
- With drop policy, terminal failure should durably remove the message.

## Visibility Timeout

Visibility timeout is the queue-mode crash recovery mechanism. If a consumer
receives a message and does not ack or nack it, the broker reaper eventually
moves the message from in-flight back to pending.

Across broker restart, connected consumers and in-flight maps are lost, but the
durable message remains in storage. Restart recovery loads it as queue work
again. This is at-least-once delivery; duplicate processing is allowed.

## Dead Letter Queues

A DLQ is a normal queue with the `.dlq` suffix. It stores failed work for
operator inspection, retry, or purge.

DLQ movement must be durable:

- delete from the original queue storage
- save under the DLQ queue name

Operator actions through the HTTP API use broker methods so runtime state and
durable storage stay aligned for retry and purge.

## Storage And Recovery

BadgerDB is the embedded durable storage adapter. It stores:

- queue metadata
- queue-mode messages
- stream-mode messages and offsets when stream mode is used

On startup, the broker loads queue metadata and queue-mode messages from
storage. Recovered queue-mode messages are loaded as pending work.

The storage contract is intentionally narrower than the broker contract. Storage
does not know about TCP connections, consumers, group members, or visibility
timers; those are broker runtime concepts.

## HTTP Surface

The HTTP server shares the metrics port and exposes:

- Prometheus metrics
- queue list/detail/purge
- message peek
- DLQ list/retry/purge
- schema list/detail
- process stats
- consumer summaries
- dashboard assets
- WebSocket stats updates

HTTP endpoints should preserve the same broker semantics as the TCP protocol.
Admin operations that mutate queue state should go through broker methods rather
than duplicating storage or queue logic in handlers.

## Common Change Paths

Use these paths to keep small changes small. Start at the responsibility that
owns the user-visible behavior, then move outward only when the behavior crosses
another responsibility.

### Protocol Operation

Start with the protocol contract:

- frame opcode and payload shape
- protobuf message fields
- server dispatch behavior
- client support

Good tests exercise a real encoded frame or public client call. Avoid tests that
only assert helper function shape unless the helper is the protocol contract.

### Queue Semantics

Start with broker and queue behavior:

- publish/consume/ack/nack
- visibility timeout and redelivery
- retry policy and DLQ movement
- ordering keys
- consumer groups

Good tests publish and consume through TCP or stable broker methods, then assert
observable delivery behavior. For restart-sensitive changes, use a real storage
directory and restart the broker in the test.

### Storage And Recovery

Start with the durable queue state contract:

- what must survive restart
- what must be deleted durably
- what runtime state is intentionally ephemeral
- how storage errors should affect the public result

Good tests prove behavior before and after restart. Storage unit tests are useful
for adapter details, but they are not enough to prove broker durability.

### HTTP API And Dashboard

Start with the broker operation the endpoint should represent:

- queue purge
- DLQ retry or purge
- schema inspection
- stats and consumer visibility

Operator mutations should call broker methods. The handler should not duplicate
queue/storage state transitions. Good tests use HTTP requests and then verify
the broker-visible result.

### Client Behavior

Start with the public client workflow:

- connection setup
- publish
- consume
- ack/nack
- schema registration
- typed helper behavior

Good tests use a real broker server when the behavior depends on protocol
compatibility. Keep client-only unit tests for local encoding or type-safety
helpers.

### Benchmark Work

Start in the `bench` module. Benchmark dependencies are measurement tooling, not
runtime broker dependencies.

Good benchmark changes keep the root module clean, document the command path,
and state the durability mode behind any number. Release-facing claims must
follow the benchmark claims policy.

## High-Risk Behavior

These areas need stronger behavior tests because they define the product promise
or affect multiple runtime responsibilities:

- restart recovery
- ack/nack storage effects
- DLQ move, retry, and purge
- visibility timeout and redelivery
- queue purge
- consumer group runtime behavior
- ordering guarantees
- schema enforcement mode
- auth gating
- protocol compatibility
- benchmark claim validity

For these areas, prefer TDD with vertical tracer bullets: one externally visible
behavior, one failing test, the smallest code change, then the next behavior.

## Preview Capabilities

Stream mode and clustering are implemented but not product-defining today.

- **Stream mode** adds log-like retention and committed offsets.
- **Clustering** replicates writes through Raft when enabled.

These capabilities should not pull the project away from the durable queue
broker identity unless a future product decision changes the charter.
