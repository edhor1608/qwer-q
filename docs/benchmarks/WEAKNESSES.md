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
**Status:** Not a Bug (was misattributed)
**Severity:** N/A
**Found:** 2026-01-31
**Updated:** 2026-01-31

**Description:**
Originally reported as 100% connection failure during concurrent connect attempts.

**Investigation:**
The failure was caused by the container being crashed (OOM killed from prior tests), not connection handling issues. On a healthy container, connection storm succeeds:

| Container State | Attempted | Successful | Failed | Avg Connect |
|-----------------|-----------|------------|--------|-------------|
| Crashed | 100 | 0 | 100 | 0s |
| Healthy | 100 | 100 | 0 | 717µs |

**Root Cause:**
This was a symptom of W-007 (container crashes under stress), not a connection handling bug.

---

### W-004: Memory Pressure Crash at 13K Messages
**Status:** Partially Fixed
**Severity:** Medium (was High)
**Found:** 2026-01-31
**Updated:** 2026-01-31

**Description:**
With 10KB messages (no consumers), QWER-Q crashes when memory exceeds container limit.

**Fix Applied:**
- Added BadgerDB value log optimization (WithValueThreshold, WithNumVersionsToKeep)
- Reduced memory overhead from 6x to 0.5x of data size

**Results After Fix:**
- Before: 4.8K-13K messages before crash
- After: 15.7K messages before crash (~20-200% improvement)
- Sustained load now works at 536/s with stable 1-4MB memory

**Remaining Issue:**
Without consumers, messages accumulate until container OOM. This is expected behavior.
Need backpressure to gracefully reject instead of crash (see W-007)

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
**Status:** Partially Fixed
**Severity:** Medium (was Critical)
**Found:** 2026-01-31
**Updated:** 2026-01-31

**Description:**
Under extreme memory pressure (50K x 10KB messages without consumers), container OOMs.

**Fix Applied:**
- Added `CheckMemoryPressure()` returning ErrMemoryPressure when Go heap > 300MB
- Reduced DefaultMaxQueueSize from 1M to 10K
- Messages rejected gracefully when limits exceeded

**Results After Fix:**
- Before: OOM at 4.8K-13K messages
- After: OOM at 28K messages (~2-6x improvement)
- Normal operation (balanced pub/consume) works perfectly at 536/s

**Remaining Issue:**
BadgerDB mmap memory is invisible to Go's MemStats. Extreme stress without consumers
will eventually OOM. This is expected - you can't store infinite data in finite memory.

**Mitigations:**
1. Keep consumers running (normal operation)
2. Use `WithMemoryLimit()` option to adjust threshold
3. Increase container memory for heavy workloads
4. Configure per-queue max size

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

### W-F006: BadgerDB Value Log Memory Overhead
**Status:** Fixed
**Fixed:** 2026-01-31

**Description:**
Memory grew 6x data size due to BadgerDB value log and version retention.

**Fix:**
```go
WithValueThreshold(1 << 10)  // Values < 1KB in LSM tree
WithNumCompactors(2)          // Faster GC
WithNumVersionsToKeep(1)      // Only keep latest
```

**Before:** 289MB for 48MB data (6x overhead)
**After:** 23MB for 48MB data (0.5x - efficient)

**Impact on throughput:**
- Before: 4 msg/s (stressed container)
- After: 536 msg/s (134x improvement)

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
