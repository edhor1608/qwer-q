# Benchmark Results - 2026-01-31

> Historical exploratory run.  
> Contains known adapter issues and is not a release-facing benchmark source.  
> Use `docs/benchmarks/CLAIMS-POLICY.md` before citing these numbers externally.

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

## Test 5: Fair Comparison - All Systems (15s, fixed adapters)

| Queue    | Published | Consumed | Pub/sec | Errors | Persistence |
|----------|-----------|----------|---------|--------|-------------|
| NATS     | 10.8M     | 10.8M    | 721.3K  | 0      | No (pub/sub) |
| RabbitMQ | 73.7K     | 73.7K    | 4.9K    | 0      | Optional |
| Kafka    | 9.0K      | 9.0K     | 601     | 9      | Yes (log) |
| QWER-Q   | 6.9K      | 6.9K     | 460     | 0      | Yes (sync) |

**Key Insights:**
- NATS is 1500x faster but doesn't persist by default
- RabbitMQ is 10x faster with optional persistence
- Kafka is ~1.3x faster with log-based persistence
- QWER-Q with sync writes is slowest but guarantees no data loss

**Kafka Consumer Delay:**
- 3 seconds to start consuming (consumer group initialization)
- This is normal Kafka behavior

---

## Fixes Applied This Session

| Issue | Before | After |
|-------|--------|-------|
| BadgerDB memtables | 5 x 64MB = 320MB | 2 x 32MB = 64MB |
| Idle memory | 505 MB | 14 MB |
| Crash recovery | 0.6% (61/10000) | 100% (10000/10000) |
| Message delivery after ACK | Stuck | Continuous |
| Storage on startup | Disabled | Enabled with --data-dir |

---

## Test 6: Weakness Discovery Tests

### Connection Storm (100 concurrent connections)
| Queue  | Attempted | Successful | Failed |
|--------|-----------|------------|--------|
| QWER-Q | 100       | 0          | 100    |
| NATS   | 100       | 100        | 0      |
| Kafka  | 100       | 100        | 0      |

**Weakness W-003:** QWER-Q fails 100% of rapid concurrent connections.

### Memory Pressure (50K x 10KB messages, no consumers)
| Queue  | Target | Published | Errors | Result |
|--------|--------|-----------|--------|--------|
| QWER-Q | 50,000 | 13,036    | 101    | Crashed |
| NATS   | 50,000 | 50,000    | 0      | OK |
| Kafka  | 50,000 | 50,000    | 0      | OK |

**Weakness W-004:** QWER-Q crashes at ~13K messages under memory pressure.

### Message Size Scaling
| Size   | QWER-Q | Kafka | Ratio |
|--------|--------|-------|-------|
| 64B    | 321/s  | 597/s | 1.8x slower |
| 256KB  | 4/s    | 310/s | 77x slower |

**Weakness W-005:** Large messages cause 80x throughput degradation.

### Burst Traffic (1000 msg bursts)
| Queue  | Bursts in 30s | Avg Burst Time |
|--------|---------------|----------------|
| NATS   | 300           | 0.61ms         |
| Kafka  | 17            | 1.76s          |
| QWER-Q | 2             | 27.45s         |

**Weakness W-006:** Burst handling 45,000x slower than NATS.

### Container Stability
| Queue  | Crashes During Tests |
|--------|---------------------|
| QWER-Q | 3 (memory, burst, depth tests) |
| NATS   | 0 |
| Kafka  | 0 |

**Weakness W-007:** Container crashes under stress.

---

## Summary of Open Weaknesses

| ID | Severity | Issue |
|----|----------|-------|
| W-001 | Medium | Sync writes 37x slower than async |
| W-002 | High | Single consumer per connection |
| W-003 | Critical | 100% connection storm failure |
| W-004 | High | Crash at 13K messages under pressure |
| W-005 | High | 80x degradation with large messages |
| W-006 | Critical | Burst handling 45,000x slower |
| W-007 | Critical | Container crashes under stress |

See `WEAKNESSES.md` for reproduction steps and analysis.
