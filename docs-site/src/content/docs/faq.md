---
title: FAQ & Troubleshooting
description: Frequently asked questions and common issues with QWER-Q.
---

## General

### What is QWER-Q?

A typed, docker-first message queue built in Go. It fills the gap between Kafka (too heavy for simple use cases) and NATS (too minimal for durable task processing).

### When should I use QWER-Q?

QWER-Q is a good fit when you need:
- A durable message queue with minimal operational overhead
- Schema validation on messages (typed queues)
- A single binary / single container deployment
- Task distribution across competing consumers

### When should I NOT use QWER-Q?

Consider alternatives if you need:
- **Event streaming / replay** — Use Kafka or Redpanda (stream mode planned for v2.0)
- **Multi-node clustering** — QWER-Q v1 is single-node (clustering planned for v2.0)
- **Exactly-once delivery** — QWER-Q provides at-least-once with optional idempotency
- **200K+ messages/sec** — NATS or Kafka will be faster for raw throughput
- **Complex routing** — RabbitMQ has more routing options (exchanges, topic routing, etc.)

### What delivery guarantees does QWER-Q provide?

**At-least-once.** Every message is delivered at least once. If a consumer fails to acknowledge within the visibility timeout, the message is redelivered.

For effective exactly-once processing, use the `idempotency_key` field to deduplicate on the producer side, and make your consumer logic idempotent.

### Is QWER-Q production-ready?

v1.0 is suitable for single-node deployments with moderate throughput requirements. Key limitations:
- No authentication (planned for v1.1)
- Single-node only (clustering planned for v2.0)
- No official client libraries outside of Go (TypeScript planned for v1.2)

---

## Troubleshooting

### "schema validation failed" error on publish

**Cause:** The published message payload doesn't match the registered Protobuf schema.

**Fix:**
1. Ensure the `.proto` file used by your producer matches the one registered with the broker
2. Verify you're serializing with `proto.Marshal()`, not JSON or other formats
3. Check the schema version: `qwer-q schema list`

### "queue is full" error

**Cause:** The queue has reached its maximum size (default: 10,000 messages).

**Fix:**
- Add more consumers to drain the queue faster
- Check if consumers are stuck (look at `qwerq_in_flight_count` metric)
- Investigate why messages aren't being acknowledged

### "memory pressure" error

**Cause:** The broker's memory usage exceeded the limit (default: 400 MB for 512 MB container).

**Fix:**
- Increase the container's memory limit
- Reduce queue sizes / message sizes
- Check for message accumulation (consumers not keeping up)
- Look at BadgerDB metrics — compaction or memtable pressure can cause spikes

### Consumer not receiving messages

**Possible causes:**
1. **Queue is empty** — Check `qwer-q queue list` for message count
2. **All messages are in-flight** — Other consumers have claimed them. Check `IN-FLIGHT` column
3. **Strict schema mode enabled** — In `--schema-mode strict`, messages can't be published without a registered schema
4. **Wrong queue name** — Queue names are case-sensitive
5. **Connection issue** — The consumer's TCP connection may have dropped

### Messages going to DLQ

**Cause:** A message exceeded the maximum retry count (default: 5 attempts).

**To investigate:**
1. Check the DLQ: `qwer-q queue list` — look for `<queue>.dlq`
2. Consume from the DLQ to inspect failed messages
3. Check `qwerq_messages_nacked_total` metric for nack frequency

**Common reasons for repeated failures:**
- Consumer code bug causing panics/errors
- Message payload that triggers an edge case
- External dependency down (database, API, etc.)

### Broker not starting

**"failed to open storage" error:**
- Check that the data directory exists and has correct permissions
- Ensure no other process has a lock on the BadgerDB files
- If the data is corrupted, delete the data directory and restart (messages will be lost)

**Port already in use:**
- Another process is using port 9876 or 9877
- Check with `lsof -i :9876` or `netstat -tlnp | grep 9876`

### High memory usage

BadgerDB uses memory for:
- Memtables: 2 x 32 MB = 64 MB
- Block cache: 32 MB
- Index cache: 16 MB
- Total baseline: ~112 MB

Plus actual message data in memory. The broker's memory limit (400 MB) accounts for this.

If memory is consistently high:
1. Check queue depths — messages waiting consume memory
2. Look for message accumulation (consumers slower than producers)
3. BadgerDB compaction can cause temporary memory spikes

### Connection drops

**Possible causes:**
- Network issues between client and broker
- Broker restart (clients need to reconnect)
- Client timeout or idle connection cleanup by intermediate proxies

**Mitigation:**
- Implement reconnection logic in your client code
- Monitor connection count via broker logs
- Keep TCP keepalive enabled

---

## Performance

### What throughput can I expect?

Benchmarked on a 512 MB container with 1 CPU:

| Sync Interval | Throughput | Data Loss Risk |
|---------------|-----------|----------------|
| 1 second | ~5,000 msgs/sec | Up to 1s on crash |
| 100ms (default) | ~2,000 msgs/sec | Up to 100ms on crash |
| Every write | ~500 msgs/sec | None |

Throughput scales roughly linearly with CPU and message size.

### How does QWER-Q compare to NATS?

NATS achieves ~133K msgs/sec but without persistence by default. QWER-Q trades raw speed for durability and schema validation — different design goals for different use cases.

### Can I tune the sync interval?

Currently the sync interval is set at compile time (100ms default). A runtime-configurable `--sync-interval` flag is planned. For now, you can build from source with a modified `DefaultSyncInterval` in `internal/storage/badger.go`.
