# QWER-Q Weakness Analysis

This document tracks weaknesses found during benchmarking. Issues are documented first, then fixed in priority order.

## Process
1. Run benchmark suite
2. Document findings here with reproduction steps
3. Prioritize fixes
4. Fix one issue at a time
5. Re-benchmark to verify fix
6. Repeat

---

## Open Weaknesses

### W-001: Sync Writes Throughput Penalty
**Status:** Open
**Severity:** Medium
**Found:** 2026-01-31

**Description:**
With sync writes enabled for durability, throughput drops from ~7K/s to ~188/s (37x slower).

**Reproduction:**
```bash
# With sync writes (current)
go run bench/cmd/stress/main.go --queues=qwerq --duration=15s --tests=sustained
# Result: ~188/s
```

**Expected vs Actual:**
- Expected: Reasonable throughput with durability
- Actual: 37x slower than async writes

**Possible Fixes:**
1. Batch writes before sync
2. Configurable sync frequency (every N writes)
3. Async mode option for high-throughput, low-durability scenarios

---

### W-002: Single Consumer Per Connection
**Status:** Open
**Severity:** High
**Found:** 2026-01-31

**Description:**
Each connection can only have one active consumer. High concurrency tests (10 producers, 10 consumers) hang because the architecture doesn't support multiple consumers per connection.

**Reproduction:**
```bash
go run bench/cmd/stress/main.go --queues=qwerq --tests=concurrency
# Test hangs
```

**Root Cause:**
Connection state tracks only one queueName and msgCh. Multiple CONSUME commands on same connection overwrite previous consumer.

---

### W-003: Connection Storm - 100% Failure Rate
**Status:** Open
**Severity:** Critical
**Found:** 2026-01-31

**Description:**
QWER-Q fails 100% of rapid concurrent connection attempts, while NATS and Kafka handle them fine.

**Reproduction:**
```bash
go run bench/cmd/weakness/main.go --queues=qwerq,nats,kafka --tests=connections --skip-docker
```

**Results:**
| Queue | Attempted | Successful | Failed |
|-------|-----------|------------|--------|
| QWER-Q | 100 | 0 | 100 |
| NATS | 100 | 100 | 0 |
| Kafka | 100 | 100 | 0 |

**Possible Causes:**
- TCP accept backlog too small
- Connection handling blocking
- Resource exhaustion under concurrent load

---

### W-004: Memory Pressure Crash at 13K Messages
**Status:** Open
**Severity:** High
**Found:** 2026-01-31

**Description:**
With 10KB messages (no consumers), QWER-Q crashes/errors at ~13K messages instead of expected ~50K.

**Reproduction:**
```bash
go run bench/cmd/weakness/main.go --queues=qwerq --tests=memory --skip-docker
```

**Results:**
- Expected: ~50,000 messages (488MB / 10KB)
- Actual: 13,036 published, 101 errors, then stopped
- Container became unresponsive (crash recovery test got "connection refused")

**Possible Causes:**
- BadgerDB memory spikes during writes
- Go runtime memory allocation issues
- Container OOM killed

---

### W-005: Throughput Degrades 80x with Large Messages
**Status:** Open
**Severity:** High
**Found:** 2026-01-31

**Description:**
Message throughput degrades dramatically with larger message sizes, much worse than competitors.

**Reproduction:**
```bash
go run bench/cmd/stress/main.go --queues=qwerq,kafka --tests=sizes
```

**Results:**
| Size | QWER-Q | Kafka | QWER-Q vs Kafka |
|------|--------|-------|-----------------|
| 64B | 321/s | 597/s | 1.8x slower |
| 256KB | 4/s | 310/s | 77x slower |

**Root Cause:**
Sync write per message. No batching. Each large message triggers full disk sync.

---

### W-006: Burst Handling Catastrophically Slow
**Status:** Open
**Severity:** Critical
**Found:** 2026-01-31

**Description:**
Burst of 1000 messages takes 27 seconds to process vs 0.6ms for NATS.

**Reproduction:**
```bash
go run bench/cmd/stress/main.go --queues=qwerq,nats --tests=burst
```

**Results:**
| Queue | Bursts in 30s | Avg Burst Time |
|-------|---------------|----------------|
| NATS | 300 | 0.61ms |
| Kafka | 17 | 1.76s |
| QWER-Q | 2 | 27.45s |

Also: 826 published but only 625 consumed (message loss/stuck)

---

### W-007: Container Crashes Under Stress
**Status:** Open
**Severity:** Critical
**Found:** 2026-01-31

**Description:**
QWER-Q container crashes repeatedly during stress tests, becoming unresponsive.

**Reproduction:**
Run burst test, then any subsequent test fails with "connection refused"

**Observed crashes:**
1. After memory pressure test (13K x 10KB messages)
2. After burst test (1000 msg bursts)
3. After queue depth test

**Possible Causes:**
- OOM killer
- Panic in Go code
- BadgerDB corruption

---

## Fixed Weaknesses

### W-F001: BadgerDB Memory Configuration
**Status:** Fixed
**Fixed:** 2026-01-31

**Description:**
Default BadgerDB settings used ~320MB for memtables alone (5 x 64MB).

**Fix:**
Reduced to 2 memtables x 32MB = 64MB baseline.

**Before:** 505MB idle memory
**After:** 14MB idle memory

---

### W-F002: Unbounded Idempotency Tracker
**Status:** Fixed
**Fixed:** 2026-01-31

**Description:**
Idempotency keys accumulated without limit. At 100K msg/sec with 5-min TTL, could hold 30M keys.

**Fix:**
- Added 100K max key limit
- Increased cleanup frequency (1min → 10sec)
- Force cleanup when at capacity

---

### W-F003: No Buffer Pooling
**Status:** Fixed
**Fixed:** 2026-01-31

**Description:**
Every frame decode allocated new byte slice, causing GC pressure.

**Fix:**
Added sync.Pool for common buffer sizes (1KB, 16KB, 64KB).

---

### W-F004: Ack Doesn't Trigger Delivery
**Status:** Fixed
**Fixed:** 2026-01-31

**Description:**
After ACK, consumer channel had space but no new message delivered. Only ~60 messages consumed in 30s timeout.

**Fix:**
Added `tryDeliver()` call in `Ack()` function.

**Before:** 61/10000 crash recovery (0.6%)
**After:** 10000/10000 crash recovery (100%)

---

### W-F005: Storage Not Enabled in Serve Command
**Status:** Fixed
**Fixed:** 2026-01-31

**Description:**
`--data-dir` flag existed but wasn't used. Messages not persisted.

**Fix:**
- Initialize BadgerStorage when data-dir specified
- Call LoadFromStorage() on startup
- Auto-save queue config on creation

---

## Comparison Notes

### Where Kafka Excels
- Log-based persistence with high throughput (601/s vs QWER-Q 460/s)
- Horizontal scaling with partitions
- Better for stream processing / event sourcing
- Consumer group rebalancing for fault tolerance

### Where NATS Excels
- Pure pub/sub: 721K/s (1500x faster than persistent queues)
- Minimal memory footprint
- Simple deployment
- Great for ephemeral messaging

### Where RabbitMQ Excels
- Flexible routing (exchanges, bindings)
- Optional persistence modes
- Good balance: 4.9K/s with features
- Mature ecosystem

### Where QWER-Q Excels
- Guaranteed durability with sync writes
- Simple queue semantics
- Schema validation (built-in)
- Docker-first deployment
- Lower complexity than Kafka

### Where NATS Excels
- Pure pub/sub throughput: 600K+/s
- Low memory footprint: ~10-20MB
- No persistence overhead

### QWER-Q Target Use Cases
- At-least-once delivery requirement
- Schema validation requirement
- Simpler than Kafka for small/medium scale
- Docker-first deployment
