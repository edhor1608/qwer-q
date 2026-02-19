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
- **Battle-tested event streaming at scale** — Kafka/Redpanda are still stronger for that class of workload
- **Battle-tested multi-node HA today** — clustering exists in QWER-Q but is currently preview
- **Exactly-once delivery** — QWER-Q provides at-least-once with optional idempotency
- **200K+ messages/sec** — NATS or Kafka will be faster for raw throughput
- **Complex routing** — RabbitMQ has more routing options (exchanges, topic routing, etc.)

### What delivery guarantees does QWER-Q provide?

**At-least-once.** Every message is delivered at least once. If a consumer fails to acknowledge within the visibility timeout, the message is redelivered.

For effective exactly-once processing, use the `idempotency_key` field to deduplicate on the producer side, and make your consumer logic idempotent.

### Is QWER-Q production-ready?

Single-node queue mode is production-ready for moderate throughput workloads.

Current maturity by area:
- Queue mode (core publish/consume/ack/nack): stable
- Auth/token enforcement: stable
- TypeScript client: available
- Stream mode: preview
- Clustering (Raft): preview

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

It depends mostly on message size, consumer speed, and durability settings.

In practice on 1 vCPU / 512MB containers, queue workloads often land in the low-thousands msgs/sec range with persistence enabled. Use the benchmark harness in `bench/` for your exact profile.

### How does QWER-Q compare to NATS?

NATS achieves ~133K msgs/sec but without persistence by default. QWER-Q trades raw speed for durability and schema validation — different design goals for different use cases.

### Can I tune durability vs throughput?

Yes:
- `--batch-interval` controls write batching at runtime (`0` disables batching)
- Badger sync interval is currently compile-time (`internal/storage/badger.go`, default 100ms)

For strictest durability semantics, keep batching off (`--batch-interval=0`).
