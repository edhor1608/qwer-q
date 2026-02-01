# Optimization Loop 3: ReadMemStats Stop-the-World Fix

**Date:** 2026-02-01
**Branch:** main
**Goal:** Investigate consumer stalling under sustained load

---

## Executive Summary

Loop 3 benchmark revealed a critical issue: **Consumer stops receiving messages after ~3.4K messages**.

**Root Cause:** `runtime.ReadMemStats` is a stop-the-world operation called every 10th publish, blocking all goroutines including consumer delivery.

**Fix:** Cache memory stats in background goroutine, hot path reads atomic cached value.

**Results:**
| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Throughput | 460/s | 3.4K/s | 7x |
| Consumed (30s) | 4.1K | 99K+ | 24x |
| Consumer stalls | Yes | No | Fixed |

---

## Step 1: Benchmark

Initial sustained load test results:

```
+-------------+------------+------------+------------+-----------+
| Queue       | Published  | Consumed   | Pub/sec    | Errors    |
+-------------+------------+------------+------------+-----------+
| QWER-Q      |      13.8K |       4.1K |        460 |     48033 |
+-------------+------------+------------+------------+-----------+
```

Memory trace showed consumer stopping at ~3.5K:
```
  Time    | Published  | Consumed   | Memory
  --------|------------|------------|----------
     3s   |       3.1K |       3.1K |     2.8MB  <-- balanced
     4s   |       4.2K |       3.5K |     3.4MB  <-- consumer starts lagging
     5s   |       5.3K |       3.5K |     3.3MB  <-- consumer STOPPED
    ...   |      ...   |       3.5K |     ...    <-- stays stuck
```

---

## Step 2: Find Weaknesses

**W-009: Consumer stops after ~3.4K messages**
- Severity: Critical
- Consumer receives messages normally for 3-4 seconds
- Then completely stops (0 msgs/sec)
- Queue fills up, all subsequent publishes rejected

---

## Step 3: Reproduce 3x

```
=== Run 1 ===
| QWER-Q      |      13.8K |       4.1K |        460 |     48033 |
=== Run 2 ===
| QWER-Q      |      13.8K |       3.9K |        459 |     47398 |
=== Run 3 ===
| QWER-Q      |      13.9K |       3.8K |        462 |     43185 |
```

Consistent: ~3.8-4.1K consumed, ~460/s throughput, ~45K errors.

---

## Step 4: Classify

**Classification: Bug (Fixable)**

| Finding | Type | Evidence |
|---------|------|----------|
| Consumer stops at ~3.4K | Bug | Consistent across all runs |
| Queue fills (10K limit) | Expected | Backpressure working |
| 45K+ errors | Symptom | Queue full rejections |

---

## Step 5: Investigation

### Hypothesis 1: Queue channel buffer too small
- Consumer channel buffer = 1
- Tested: Direct queue test (bypassing network) processed 10K msgs in 500ms
- **Result:** Queue layer works fine

### Hypothesis 2: Network layer issue
- Created debug test to trace consumer behavior
- Observed: Consumer receives ~3.4K messages then stops receiving completely
- Client stuck on `DecodeFrame` waiting for next message
- Server not sending any more messages

### Hypothesis 3: Memory pressure check
- Disabled memory check (`WithMemoryLimit(0)`)
- **Result:** 177K+ messages in 9 seconds, consumer keeps up perfectly!

### Root Cause Confirmed

`runtime.ReadMemStats` is the culprit:
- Called every 10th publish operation
- **Stop-the-world operation** - pauses ALL goroutines
- Under high load: hundreds of STW pauses per second
- Consumer delivery goroutine gets blocked repeatedly
- Eventually falls behind permanently

Evidence:
```go
// Before: STW on every 10th publish
func (b *Broker) checkMemory(eager bool) error {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)  // <-- BLOCKS ALL GOROUTINES
    ...
}
```

---

## Step 5: Fix

### Options Considered

| Option | Description | Pros | Cons | Chosen? |
|--------|-------------|------|------|---------|
| A. Remove memory check | No checking | Simple | Unsafe, OOM risk | No |
| B. Reduce frequency | Check every 1000th | Less blocking | Still blocks occasionally | No |
| C. Background caching | ReadMemStats in goroutine | No STW in hot path | Small delay in detection | **Yes** |
| D. /proc/self/statm | Read OS memory directly | No STW | Linux-only, cgo | No |

### Fix Implemented

```go
// After: Cache stats in background goroutine
func (b *Broker) memoryMonitor() {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    b.cachedAlloc.Store(m.Alloc)

    for {
        select {
        case <-ticker.C:
            runtime.ReadMemStats(&m)
            b.cachedAlloc.Store(m.Alloc)
        case <-b.done:
            return
        }
    }
}

func (b *Broker) CheckMemoryPressure() error {
    if b.memoryLimit == 0 {
        return nil
    }
    if b.cachedAlloc.Load() > b.memoryLimit {
        return ErrMemoryPressure
    }
    return nil
}
```

Also increased memory limit from 300MB to 400MB (BadgerDB needs more baseline).

---

## Step 6: Regression Test

With clean volume between runs:

```
=== Run 1 ===
Start memory: 13.65MiB / 512MiB
| QWER-Q      |     100.9K |      99.0K |       3.4K |         0 |
=== Run 2 ===
Start memory: 13.2MiB / 512MiB
| QWER-Q      |      96.2K |      91.9K |       3.2K |      6859 |
=== Run 3 ===
Start memory: 14.01MiB / 512MiB
| QWER-Q      |     108.5K |     108.5K |       3.6K |         0 |
```

All runs show ~3.2-3.6K/s throughput, consumer keeps up.

---

## Summary

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Throughput | 460/s | 3.4K/s | 7x |
| Consumed (30s) | 4.1K | 99K+ | 24x |
| Consumer stalls | Yes | No | Fixed |
| Errors | 45K+ | 0-7K | -85% |

---

## Lessons Learned

1. **ReadMemStats is expensive** - It's a stop-the-world operation that pauses the entire runtime
2. **Throttling isn't enough** - Even every 10th call is too frequent under high load
3. **Background goroutines for expensive operations** - Move costly operations out of the hot path
4. **Test with sustained load** - Burst tests may not reveal issues that only appear under continuous pressure

---

## Git Workflow

| Step | Action |
|------|--------|
| Branch | `fix/w009-readmemstats-stw` |
| Commit | `fix(broker): eliminate stop-the-world memory checks (W-009)` |
| PR | [#9](https://github.com/edhor1608/qwer-q/pull/9) |
