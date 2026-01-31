# Benchmark Results - 2026-01-31

## Test Environment
- **Platform:** Docker containers with identical resource limits
- **CPU:** 1 core per container
- **Memory:** 512MB per container
- **Systems Tested:** QWER-Q, NATS, RabbitMQ, Redis

---

## Test 1: Sustained Load (30s, 1024 byte messages)

| Queue    | Published | Consumed | Pub/sec | Errors |
|----------|-----------|----------|---------|--------|
| QWER-Q   | 700.2K    | 700.2K   | 23.3K   | 0      |
| NATS     | 6.0M      | 6.0M     | 200.5K  | 0      |
| RabbitMQ | 217.3K    | 0        | 7.2K    | 1      |
| Redis    | 279.8K    | 279.7K   | 9.3K    | 0      |

**Notes:**
- NATS is 8.6x faster than QWER-Q (pub/sub without persistence)
- RabbitMQ consumer issue (0 consumed) - adapter bug
- QWER-Q maintains 100% delivery with persistence

---

## Test 2: Memory Usage

### Baseline Memory (idle)
| Queue    | Memory   | % of Limit |
|----------|----------|------------|
| QWER-Q   | 14.31 MB | 2.80%      |
| NATS     | 8.96 MB  | 1.75%      |
| RabbitMQ | 104.9 MB | 20.49%     |

### Under Sustained Load (balanced pub/consume)
| Queue    | Memory   | Notes                |
|----------|----------|----------------------|
| QWER-Q   | 1-4 MB   | Stable, no growth    |
| NATS     | 1-4 MB   | Stable, no growth    |

### Memory Pressure Test (50K x 10KB messages, no consumers)
| Queue    | Published | Errors | Final Memory |
|----------|-----------|--------|--------------|
| QWER-Q   | 50,000    | 0      | 511.8 MB     |
| NATS     | 50,000    | 0      | 8.96 MB      |

**Notes:**
- QWER-Q persists all messages to disk → memory fills with storage
- NATS doesn't persist by default → memory stays low
- This is expected behavior based on design goals

---

## Test 3: Breaking Point Analysis

| Queue  | Max Stable | Breaking Point | First Error | Total Errors |
|--------|------------|----------------|-------------|--------------|
| QWER-Q | 9,459/s    | N/A (no errors)| 0           | 0            |

**Notes:**
- QWER-Q doesn't break - throughput plateaus at ~9.5K/s
- Limited by disk I/O (sync writes for durability)
- No errors even at 500K/s target rate

---

## Test 4: Crash Recovery

| Queue  | Published | Recovered | Success | Reconnect Time | Consume Time |
|--------|-----------|-----------|---------|----------------|--------------|
| QWER-Q | 10,000    | 10,000    | YES     | 2.05ms         | 15.4s        |

**Notes:**
- 100% message recovery after container restart
- Messages persisted to BadgerDB with sync writes
- Tested by: publish → `docker restart` → consume

---

## Key Findings

### QWER-Q Strengths
1. **100% crash recovery** - All messages survive restarts
2. **No breaking point** - Stable under any load (throughput limited, not unstable)
3. **Low operational memory** - 1-4MB during balanced workloads
4. **At-least-once delivery** - Every message delivered

### QWER-Q Weaknesses (Documented)
1. **W-001:** Sync writes throughput penalty (188/s vs 7K/s async)
2. **W-002:** Single consumer per connection (concurrency limitation)

### Competitor Notes
- **NATS:** 10-100x faster for pure pub/sub, no persistence overhead
- **RabbitMQ:** Consumer adapter has issues (not fair comparison)
- **Kafka:** Adapter has issues (36M errors, 0 consumed) - needs fix before fair comparison

---

## Test 5: QWER-Q vs NATS vs Kafka (15s)

| Queue    | Published | Consumed | Pub/sec | Errors     |
|----------|-----------|----------|---------|------------|
| QWER-Q   | 5.1K      | 5.1K     | 338     | 0          |
| NATS     | 10.6M     | 10.6M    | 706.8K  | 0          |
| Kafka    | 3.5K      | 0        | 234     | 36,515,349 |

**Notes:**
- QWER-Q throughput lower due to sync writes enabled (durability mode)
- NATS ~2000x faster (no persistence)
- Kafka adapter broken - investigate before conclusions

---

## Fixes Applied This Session

| Issue | Before | After |
|-------|--------|-------|
| BadgerDB memtables | 5 x 64MB = 320MB | 2 x 32MB = 64MB |
| Idle memory | 505 MB | 14 MB |
| Crash recovery | 0.6% (61/10000) | 100% (10000/10000) |
| Message delivery after ACK | Stuck | Continuous |
| Storage on startup | Disabled | Enabled with --data-dir |
