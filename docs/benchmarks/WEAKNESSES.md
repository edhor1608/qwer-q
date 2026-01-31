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

## Benchmark Adapter Issues

### Kafka Adapter (Not Working)
**Status:** Needs Fix
**Found:** 2026-01-31

The Kafka adapter has issues:
- 36M publish errors
- 0 messages consumed
- Slow startup (10s before first message)

Likely causes:
1. Topic auto-create not working as expected
2. Consumer group initialization issues
3. Connection/broker discovery problems

**Before comparing Kafka fairly, adapter needs to be fixed.**

---

## Comparison Notes

### Where Kafka Excels
- TODO: Fix adapter and run benchmarks
- Expected: High throughput with persistence (log-based)
- Expected: Better at scale with partitioning

### Where NATS Excels
- Pure pub/sub throughput: 600K+/s
- Low memory footprint: ~10-20MB
- No persistence overhead

### QWER-Q Target Use Cases
- At-least-once delivery requirement
- Schema validation requirement
- Simpler than Kafka for small/medium scale
- Docker-first deployment
