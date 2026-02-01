# QWER-Q Performance Optimizations

This document captures the performance work done to make QWER-Q competitive with other message queues.

## The Problem

Initial benchmarks showed QWER-Q was **37x slower** than competitors:

| Queue | Throughput | Notes |
|-------|------------|-------|
| NATS | 800K/s | No persistence |
| RabbitMQ | 4.9K/s | With persistence |
| Kafka | 600/s | With persistence |
| **QWER-Q** | **188/s** | With persistence |

Root cause: `SyncWrites: true` in BadgerDB — every single message triggered an fsync to disk.

## Research: How Other Queues Handle Durability

We researched how Kafka, RabbitMQ, NATS, and Redis handle the durability vs throughput tradeoff:

| System | Default Behavior | Real Durability |
|--------|------------------|-----------------|
| **Kafka** | No fsync, relies on replication | Can lose data on single-node failure |
| **RabbitMQ Classic** | No fsync before publisher confirm | Can lose recent messages |
| **RabbitMQ Quorum** | fsync on quorum (2/3 nodes) | True durability |
| **NATS JetStream** | fsync every **2 minutes** | Can lose ~30 seconds on power failure |
| **Redis everysec** | fsync once per second | Lose up to 1 second |
| **Redis always** | fsync with group commit | True durability, decent speed |

**Key insight**: Almost nobody fsyncs every write. The common patterns are:
- Replication (Kafka, NATS) - durability through redundancy
- Time-based batching (Redis everysec, NATS) - periodic sync
- Group commit (Redis always) - batch concurrent writes, single fsync

Sources:
- [Why Kafka doesn't need fsync](https://jack-vanlightly.com/blog/2023/4/24/why-apache-kafka-doesnt-need-fsync-to-be-safe)
- [RabbitMQ message storage](https://www.rabbitmq.com/blog/2025/01/17/how-are-the-messages-stored)
- [NATS Jepsen analysis](https://jepsen.io/analyses/nats-2.12.1)
- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)

## The Solution

We implemented **configurable sync interval** (similar to Redis `appendfsync everysec`):

```go
// Before: sync every write (safest, slowest)
WithSyncWrites(true)

// After: background sync every 100ms (fast, slight risk)
const DefaultSyncInterval = 100 * time.Millisecond

func (s *BadgerStorage) syncLoop() {
    ticker := time.NewTicker(s.syncInterval)
    for {
        select {
        case <-ticker.C:
            s.db.Sync()
        case <-s.done:
            return
        }
    }
}
```

**Options available:**
- `WithSyncInterval(0)` — sync every write (safest, ~500/s)
- `WithSyncInterval(100*time.Millisecond)` — default (~3K/s)
- `WithSyncInterval(time.Second)` — faster (~5K/s, lose up to 1 second)

## Other Optimizations

### 1. BadgerDB Memory Configuration (W-004)

**Problem:** Default BadgerDB used ~320MB for memtables alone (5 x 64MB).

**Fix:**
```go
WithNumMemtables(2)           // 5 → 2
WithMemTableSize(32 << 20)    // 64MB → 32MB
WithValueThreshold(1 << 10)   // Small values in LSM tree
WithNumVersionsToKeep(1)      // Only keep latest
```

**Result:** Idle memory 505MB → 14MB

### 2. Memory Backpressure (W-007)

**Problem:** Under extreme load, container OOMs without warning.

**Fix:**
```go
func (b *Broker) CheckMemoryPressure() error {
    if b.memoryLimit == 0 {
        return nil
    }
    // Throttled check (every 10 calls)
    count := b.memCheck.Add(1)
    if count%10 != 0 {
        return nil
    }
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    if m.Alloc > b.memoryLimit {
        return ErrMemoryPressure
    }
    return nil
}
```

**Result:** Graceful rejection instead of OOM crash.

### 3. Queue Size Limits

**Problem:** Unbounded queues consume all memory.

**Fix:** `DefaultMaxQueueSize = 10_000` (was 1M)

### 4. Buffer Pooling

**Problem:** Every frame decode allocated new byte slice.

**Fix:** `sync.Pool` for common buffer sizes (1KB, 16KB, 64KB).

### 5. Idempotency Tracker Limits

**Problem:** Keys accumulated without limit.

**Fix:** 100K max keys, cleanup every 10 seconds.

## Results

### Before vs After

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Sustained throughput | 188/s | 3,000/s | **16x** |
| Burst (1000 msgs) | 27s | 558ms | **49x** |
| Idle memory | 505MB | 14MB | **36x less** |
| Data overhead | 6x | 0.5x | **12x better** |

### vs Competitors (persisted messages)

| Queue | Throughput |
|-------|------------|
| QWER-Q | 1.1-3K/s |
| Kafka | 673/s |

**QWER-Q is now 1.6-4x faster than Kafka** for durable message queuing.

## Tradeoffs

### Sync Interval Risk

With the default 100ms sync interval:
- **Power failure** can lose up to 100ms of unsynced messages
- **Process crash** is safe (data is in OS buffer, survives process death)
- **Kernel panic** can lose data (same as power failure)

This matches Redis `appendfsync everysec` behavior, which is considered production-grade.

### When to Use Sync-Every-Write

For maximum durability (financial transactions, audit logs):
```go
storage.NewBadgerStorage(path, storage.WithSyncInterval(0))
```

This gives ~500/s throughput but guarantees no data loss.

## Lessons Learned

1. **Benchmark before optimizing** — We found the real bottleneck (fsync) through measurement, not guessing.

2. **Research existing solutions** — The sync interval approach is proven by Redis, not invented here.

3. **Make tradeoffs explicit** — Users can choose their durability/speed balance.

4. **Container memory is tricky** — BadgerDB's mmap doesn't show in Go's MemStats. Need multiple defense layers (queue limits, memory checks, container limits).

5. **Fresh state matters for benchmarks** — Stale data from previous tests caused false memory pressure readings.
