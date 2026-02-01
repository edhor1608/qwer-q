# Optimization Loop 4: Message Ordering Investigation

**Date:** 2026-02-01
**Branch:** main
**Goal:** Investigate out-of-order message delivery

---

## Executive Summary

Loop 4 benchmark revealed an ordering issue: **~1% of messages delivered out of order** even with a single consumer.

**Status:** Under investigation

---

## Step 1: Benchmark

Full test suite run on fresh container:

### Sustained Load ✅
```
| QWER-Q      |      85.7K |      85.7K |       2.9K |         0 |
```
Consumer keeps up perfectly.

### Burst Traffic ✅
```
| QWER-Q      |     95 bursts |  315.90ms avg |
```
Improved from 546ms (Loop 2) to 316ms.

### Message Sizes ✅
```
|      64B |       1.1K |
|     256B |        791 |
|    1.0KB |        385 |
|   256.0KB |         18 |
```
No crashes, stable throughput.

### Backpressure ✅
```
| QWER-Q      |      777 pub/s |       96 con/s |   163817 errors | Rejects |
```
Working as designed - rejects when queue full.

### Ordering ⚠️
```
Run 1: 10000 total | 9985 in order | 15 out-of-order | 0 missing | 99.9%
Run 2: 10000 total | 9891 in order | 0 out-of-order | 109 missing | 98.9%
Run 3: 10000 total | 8395 in order | 2 out-of-order | 1603 missing | 84.0%
```

**Issue:** Variable results, with some runs showing significant out-of-order delivery.

Note: "Missing" in runs 2-3 was due to 10s timeout. With 30s timeout:
```
| QWER-Q      |    10000 |     9886 |      114 |        0 |        0 |   98.9%  |
```
0 missing, but 114 out-of-order (~1.1%).

---

## Step 2: Find Weaknesses

**W-010: Out-of-Order Message Delivery**
- Severity: Medium
- ~1% of messages delivered out of order
- With single producer, single consumer, serial publishing
- FIFO queue should have 100% ordering

**Known Issues (Not Bugs):**
- Concurrency test hangs (W-002: single consumer per connection - architectural)
- Depth test hits 10K queue limit (expected - backpressure working)

---

## Step 3: Reproduce 3x

```
=== Fresh container, 30s timeout ===
Run 1: 114 out-of-order (98.9%)
Run 2: 32 out-of-order (99.7%)
Run 3: 15 out-of-order (99.9%)
```

Consistent: 15-114 out-of-order messages per 10K (~0.1-1.1%).

---

## Step 4: Classify

| Finding | Type | Evidence |
|---------|------|----------|
| Out-of-order delivery | Bug | Consistent across runs |
| Variable rate (0.1-1.1%) | Race condition? | Varies by run |

---

## Step 5: Investigation

### Architecture Review

The delivery path:
1. `Publish` → `Enqueue` → appends to `q.messages` (with lock)
2. `tryDeliver` iterates `q.messages` in order, sends to consumer channel
3. Server goroutine reads channel, writes to TCP connection
4. Client receives, calls handler

### Hypotheses

**H1: Race in tryDeliver with multiple Enqueue calls**
- Test uses serial publishing (waits for response)
- Single connection means single-threaded on server
- **Unlikely** but needs verification

**H2: Channel buffer reordering**
- Consumer channel has buffer size 1
- Messages should queue in order
- **Unlikely**

**H3: TCP or network reordering**
- TCP guarantees order within connection
- **Very unlikely**

**H4: Test measurement bug**
- Test counts "out of order" if seq != lastSeq + 1
- One gap cascades into multiple counts
- **Possible** - but doesn't explain the initial gap

**H5: Visibility timeout in tryDeliver**
- `tryDeliver` skips messages with future `VisibleAt`
- Fresh messages have `VisibleAt = time.Now()`
- But if clock skew or timing issue...
- **Worth investigating**

## Step 5: Root Cause Analysis

### Testing Summary

| Test | ACK Pattern | Result |
|------|-------------|--------|
| Queue FIFO (sync ACKs) | Sequential | ✅ Pass |
| Queue FIFO (concurrent ACKs) | Parallel goroutines | ❌ 6-12% reorder |
| Server/Client | Parallel goroutines | ❌ 7%+ reorder |

### Root Cause

When ACKs are processed concurrently by multiple goroutines, `tryDeliver` can be called from different ACK goroutines racing with each other. Although the mutex serializes access to the queue, the ORDER in which ACK goroutines acquire the mutex is non-deterministic.

The specific race:
1. Consumer reads msg0, msg1, msg2 in rapid succession
2. ACK goroutines for msg0, msg1, msg2 are spawned
3. ACK goroutines compete for mutex in arbitrary order
4. If ACK-2 runs before ACK-1, `tryDeliver` sends msg3 before msg2 would trigger its delivery

This explains why:
- Sequential ACKs work (no race)
- Concurrent ACKs fail (race on mutex acquisition order)
- Reordering increases with message count (more concurrent ACKs)

---

## Current State

**Classification:** Bug in concurrent ACK handling

**Options:**
| Option | Description | Impact |
|--------|-------------|--------|
| A. Fix tryDeliver | Ensure delivery order independent of ACK order | May require redesign |
| B. Document limitation | "Order not guaranteed with high concurrency" | User responsibility |
| C. Strict ordering mode | Single-threaded delivery with lock | Performance penalty |

**Recommendation:** Option A - The queue should maintain FIFO order regardless of ACK timing. This is a correctness bug, not a performance tradeoff.

---

## Raw Benchmark Data

```
Sustained: 85.7K msgs, 2.9K/s, 0 errors
Burst: 95 bursts, 316ms avg
Sizes: 64B=1.1K/s, 256KB=18/s (stable)
Backpressure: Rejects when full (as designed)
Ordering: ~99% in order, ~1% out of order
```
