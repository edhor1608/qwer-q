---
title: Concepts
description: Core concepts of QWER-Q — queues, consumers, schemas, visibility timeout, dead letter queues, and more.
---

## Mental Model

QWER-Q is queue-first by default, with optional stream mode (preview).

- Queue mode: messages are **consumed and deleted** after acknowledgment
- Stream mode (preview): messages are retained with offset-based reads
- Delivery guarantee: **at-least-once**

## Queues

A queue is a named destination for messages. Queues are auto-created on first publish in `permissive` mode, or when a schema is registered and used in `strict` mode.

```
Producer → [Queue: "orders"] → Consumer
```

Key properties:
- **Max size**: 10,000 messages by default. Publishes are rejected with an error when the queue is full.
- **FIFO ordering**: Strict FIFO at queue head for normal queue delivery.
- **Persistence**: All messages are persisted to BadgerDB and survive broker restarts.

## Producers

A producer publishes messages to a queue. Every published message:
1. Is validated against the queue's registered schema (if present)
2. Receives a ULID (Universally Unique Lexicographically Sortable Identifier) if no custom ID is provided
3. Is persisted to disk before the publish acknowledgment is sent

## Consumers

A consumer subscribes to a queue and receives messages via a persistent TCP connection.

**Competing consumers**: Multiple consumers on the same queue receive messages in round-robin order. This enables horizontal scaling of message processing.

```
                    ┌─→ Consumer A
Producer → [Queue] ─┼─→ Consumer B
                    └─→ Consumer C
```

Each message is delivered to exactly one consumer. If that consumer fails to acknowledge it, the message becomes available again after the visibility timeout expires.

## Schemas

QWER-Q uses Protocol Buffers (Protobuf) for schema validation.

Schema enforcement is configurable:
- `permissive` (default): queues can accept publishes without a schema
- `strict`: publishes are rejected unless a schema is registered for the queue

**Workflow:**
1. Define your message type in a `.proto` file
2. Register it with `qwer-q schema register -q <queue> -p <file.proto> -m <MessageType>`
3. The broker validates every published message against the schema

**Schema evolution**: Backward-compatible changes are allowed (adding optional fields, deprecating fields). Breaking changes (removing fields, changing types) are rejected.

**Why strict mode?** In microservice architectures, untyped messages are a constant source of bugs. Strict mode enforces schema-first workflows when you need hard guarantees.

## Visibility Timeout

When a message is delivered to a consumer, it becomes **invisible** to other consumers for a configurable duration (default: 30 seconds). This is the "visibility timeout" — the same concept as Amazon SQS.

**How it works:**
1. Message delivered to Consumer A → starts visibility timer
2. If Consumer A sends `ACK` within the timeout → message is permanently deleted
3. If Consumer A crashes or doesn't ack → timeout expires → message becomes visible again → delivered to next consumer

This provides **at-least-once delivery** without requiring consumers to implement complex failure handling.

```
Publish → [Queue] → Deliver → [In-Flight / Invisible]
                                    │
                              ACK received? ──→ Yes → Delete message
                                    │
                                    No (timeout)
                                    │
                                    ↓
                              Requeue → [Queue] → Deliver again
```

**Extending the timeout**: If processing takes longer than expected, consumers can extend the visibility timeout using the `EXTEND_VISIBILITY` operation before the current timeout expires.

## Dead Letter Queue (DLQ)

When a message repeatedly fails processing (exceeds the max retry count, default: 5 attempts), it's moved to a **dead letter queue** instead of being retried forever.

The DLQ for a queue named `orders` is automatically named `orders.dlq`.

**Failure policies:**
| Policy | Behavior |
|--------|----------|
| `dlq` (default) | Move to `<queue>.dlq` after max retries |
| `drop` | Discard the message silently |
| `infinite` | Retry forever (no retry limit) |

DLQ messages can be inspected and replayed using the CLI or programmatically consumed like any other queue.

## Request/Reply (CALL)

QWER-Q supports native RPC-style request/reply with the `CALL` operation. This enables type-safe RPC over a durable queue.

**How it works:**
1. Client sends `CALL` with a target queue and payload
2. Broker creates a temporary reply queue and injects `reply_to` and `correlation_id` headers
3. A consumer processes the message and publishes a response to the `reply_to` queue
4. The original client receives the response (or a timeout error)

Default timeout: 30 seconds (configurable per call).

## Idempotency

Producers can include an `idempotency_key` with any publish. If the same key is seen within the deduplication window (default: 5 minutes), the broker rejects the duplicate.

This is opt-in: if no key is provided, no deduplication overhead is incurred.

## Message Headers

Every message can carry arbitrary string key-value headers. The broker treats headers as opaque pass-through data.

Common uses:
- Tracing IDs (e.g., `trace_id`, `span_id`)
- Routing hints
- Application-specific context

## Message IDs

Each message gets a unique ID:
- **Default**: The broker generates a [ULID](https://github.com/ulid/spec) — sortable, timestamp-based, globally unique
- **Override**: Clients can provide their own ID via the `message_id` field

## Backpressure

When a queue reaches its maximum size, publish requests are rejected with a clear error. This prevents unbounded memory growth.

Additionally, the broker monitors total memory usage (default limit: 400MB for a 512MB container). When memory pressure is detected, publishes are temporarily rejected with a "memory pressure" error until usage drops.

## Observability

The broker exposes Prometheus metrics on port 9877:

- `qwerq_messages_published_total` — messages published per queue
- `qwerq_messages_consumed_total` — messages delivered per queue
- `qwerq_messages_acked_total` — messages acknowledged per queue
- `qwerq_messages_nacked_total` — messages negatively acknowledged per queue
- `qwerq_queue_depth` — current message count per queue
- `qwerq_in_flight_count` — in-flight message count per queue
- `qwerq_publish_latency_seconds` — publish operation latency histogram
- `qwerq_messages_dlq_total` — messages sent to DLQ per queue
- `qwerq_duplicate_messages_total` — duplicate messages rejected per queue
- `qwerq_queue_full_errors_total` — queue full rejections per queue
- `qwerq_call_requests_total` — CALL requests per queue
- `qwerq_call_timeouts_total` — CALL timeouts per queue

A health endpoint is available at `GET /health` on the metrics port.
