# QWER-Q Weakness Analysis

This document tracks weaknesses found during benchmarking. Issues are documented first, then fixed in priority order.

This is an engineering working log, not a release-facing claims document.

## Process
1. Run benchmark suite
2. Document findings here with reproduction steps
3. Prioritize fixes
4. Fix one issue at a time
5. Re-benchmark to verify fix
6. Repeat

---

## Open Weaknesses

(None currently)

---

## Fixed Weaknesses

### W-010: Out-of-Order Message Delivery
**Status:** Fixed
**Severity:** Medium
**Found:** 2026-02-01
**Fixed:** 2026-02-03

**Description:**
6-12% of messages delivered out of order when ACKs are processed concurrently.

**Root Cause:**
`tryDeliver` iterated through the entire queue and would skip invisible messages to deliver later ones first. When concurrent ACK goroutines called `tryDeliver`, the iteration could deliver messages out of their original enqueue order.

**Fix:**
Changed `tryDeliver` to strict FIFO - only deliver the HEAD of the queue (index 0). If head is not visible, stop entirely rather than skipping to later messages.

**Results:**
| Metric | Before | After |
|--------|--------|-------|
| Ordering (concurrent ACKs) | 88-94% | 100% |
| Throughput | 2.9K/s | 3.7K/s |
| Errors | 0 | 0 |

---

### W-009: ReadMemStats Stop-the-World Blocks Consumer
**Status:** Fixed
**Severity:** Critical
**Found:** 2026-02-01
**Fixed:** 2026-02-01

**Description:**
Consumer stops receiving messages after ~3.4K messages, completely stalling (0 msgs/sec).

**Root Cause:**
- `runtime.ReadMemStats` called every 10th publish operation
- ReadMemStats is a stop-the-world (STW) operation that pauses ALL goroutines
- Under high load, hundreds of STW pauses per second
- Consumer delivery goroutine gets blocked repeatedly until it falls behind permanently

**Fix:**
- Move ReadMemStats to dedicated background goroutine
- Update cached memory value every 100ms
- Hot path reads atomic cached value (zero allocation, no STW)
- Increased memory limit from 300MB to 400MB (BadgerDB baseline needs more)

**Results:**
| Metric | Before | After |
|--------|--------|-------|
| Throughput | 460/s | 3.4K/s |
| Total consumed (30s) | 4.1K | 99K+ |
| Consumer stalls | Yes (stops at 3.4K) | No |

---

### W-008: OOM with Large Messages (256KB+)
**Status:** Fixed
**Severity:** Critical
**Found:** 2026-02-01
**Fixed:** 2026-02-01

**Description:**
Container OOM killed (exit 137) when processing 256KB messages.

**Root Cause:**
- Frame decoder allocated buffer BEFORE checking size
- Memory pressure check was throttled (every 10th call)
- Buffer pools only went to 64KB

**Fix:**
- Pre-allocation size check in frame decoder (fail-fast)
- Eager memory check for messages > 64KB (not throttled)
- Extended buffer pools to 256KB
- Added `--max-message-size` CLI flag (default 1MB)

**Results:**
- Before: 10/s → OOM crash
- After: 84/s stable, container healthy

---

### W-001: Sync Writes Throughput Penalty
**Status:** Expected (trade-off)
**Severity:** Medium
**Found:** 2026-01-31
**Updated:** 2026-02-01

**Description:**
With 100ms sync interval (default), throughput is ~500-1000/s. This is a design choice for durability.

**Classification:** This is an **expected trade-off**, not a bug. Default is 100ms sync interval.

**Update:** The `--sync-interval` CLI flag is now implemented on `qwer-q serve`. The broker still defaults to `100ms`, but runtime durability/performance tradeoffs are now operator-controlled.

**Action:** Document trade-offs clearly. No code fix needed.

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

### W-005: Throughput Degrades with Large Messages
**Status:** Partially Fixed
**Severity:** Medium (was High)
**Found:** 2026-01-31
**Updated:** 2026-02-01

**Description:**
Message throughput degrades with larger message sizes.

**Before W-008 fix:**
| Size | QWER-Q | Degradation |
|------|--------|-------------|
| 64B | 630/s | baseline |
| 256KB | 10/s → crash | 63x + OOM |

**After W-008 fix:**
| Size | QWER-Q | Degradation |
|------|--------|-------------|
| 64B | 2.4K/s | baseline |
| 256KB | 84/s | 29x (no crash) |

**Improvement:** 4x better at 64B, 8x better at 256KB, no crashes.

**Remaining cause:** Sync write per message. Larger messages = more data to sync.

**Possible future improvements:**
- Write batching
- Async persistence mode

---

### W-006: Burst Handling Slow
**Status:** Partially Fixed
**Severity:** Medium (was Critical)
**Found:** 2026-01-31
**Updated:** 2026-02-01

**Description:**
Burst of 1000 messages slower than competitors.

**Before (crashed before test completed):**
| Queue | Bursts in 30s | Avg Burst Time |
|-------|---------------|----------------|
| NATS | 300 | 0.61ms |
| QWER-Q | 2 | 27.45s (then crash) |

**After W-008 fix:**
| Queue | Bursts in 30s | Avg Burst Time |
|-------|---------------|----------------|
| NATS | ~300 | ~0.6ms |
| QWER-Q | 56 | 546ms |

**Improvement:** Actually completes now. 28x faster per burst (27s → 546ms).

**Remaining gap:** NATS is 900x faster (in-memory, no persistence).
This is expected - we have durability guarantees NATS doesn't have by default.

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

## Previously Fixed Weaknesses (Loop 1)

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

### QWER-Q Target Use Cases
- At-least-once delivery requirement
- Schema validation requirement
- Simpler than Kafka for small/medium scale
- Docker-first deployment
