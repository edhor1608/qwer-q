# QWER-Q Benchmark Suite

A comprehensive benchmark suite for testing QWER-Q against other message queues under realistic conditions.

Release-facing benchmark claims are governed by `docs/benchmarks/CLAIMS-POLICY.md`.

## Philosophy

**Find weaknesses, not just good numbers.**

This benchmark suite was built to:
1. Run all systems under identical conditions (Docker, same resources)
2. Test edge cases that break things (not just happy path)
3. Document findings before fixing
4. Verify fixes with re-benchmarking

## Quick Start

```bash
# Start all queue systems
docker compose -f bench/docker-compose.yml up -d

# Run stress tests
go run bench/cmd/stress/main.go --queues=qwerq,nats,kafka --duration=30s

# Run weakness-finding tests
go run bench/cmd/weakness/main.go --queues=qwerq --skip-docker
```

## Architecture

```
bench/
├── adapters/           # Queue-specific implementations
│   ├── adapter.go      # Common interface
│   ├── qwerq.go        # QWER-Q adapter
│   ├── nats.go         # NATS adapter
│   ├── kafka.go        # Kafka adapter
│   ├── rabbitmq.go     # RabbitMQ adapter
│   └── redis.go        # Redis adapter
├── cmd/
│   ├── stress/         # Stress testing CLI
│   ├── weakness/       # Weakness-finding CLI
│   └── bench/          # General benchmark CLI
├── harness/            # Test harness and result types
├── scenarios/          # Test scenario implementations
│   ├── stress.go       # Sustained load, burst, lag tests
│   ├── breaking.go     # Breaking point discovery
│   ├── latency.go      # Latency measurement
│   └── throughput.go   # Throughput tests
└── docker-compose.yml  # All queue systems for comparison
```

## Test Categories

| Test | Purpose | What It Finds |
|------|---------|---------------|
| `sustained` | Throughput over time | Memory leaks, degradation |
| `burst` | Traffic spikes | Backpressure issues |
| `sizes` | Different message sizes | Serialization overhead |
| `depth` | Pre-filled queues | Startup performance |
| `lag` | Slow consumers | Queue growth behavior |
| `concurrency` | Multiple producers/consumers | Connection handling |
| `breaking` | Find limits | Maximum throughput |
| `memory` | Memory pressure | OOM behavior |
| `connections` | Connection storms | Accept handling |
| `recovery` | Crash recovery | Data durability |

## Systems Under Test

| System | Port | Notes |
|--------|------|-------|
| QWER-Q | 9876 | Our queue (persisted) |
| NATS | 4222 | JetStream enabled |
| RabbitMQ | 5672 | Management on 15672 |
| Redis | 6379 | List-based queue |
| Kafka | 9092 | With Zookeeper |

All systems run with:
- 1 CPU
- 512MB memory
- Persistent storage (where applicable)

## Running Specific Tests

```bash
# Compare QWER-Q vs Kafka only
go run bench/cmd/stress/main.go --queues=qwerq,kafka --tests=sustained

# Test burst handling
go run bench/cmd/stress/main.go --queues=qwerq,nats --tests=burst

# Test message sizes
go run bench/cmd/stress/main.go --queues=qwerq,kafka --tests=sizes

# Run all tests for 60 seconds each
go run bench/cmd/stress/main.go --duration=60s --tests=all
```

## What We Learned

### 1. Durability Settings Dominate Throughput

Persistent and non-persistent systems should not be compared as if they provide identical guarantees.

**Lesson:** publish benchmark numbers only with durability mode and environment documented.

### 2. Memory Is Tricky in Containers

BadgerDB uses mmap which doesn't show in Go's `runtime.MemStats`. Our memory checks said "50MB" while the container was at 400MB and about to OOM.

**Lesson:** Need multiple defense layers (queue limits, Go memory checks, container limits).

### 3. Stale Data Corrupts Benchmarks

Running benchmarks without clearing previous test data caused false "memory pressure" errors at 96 messages on a fresh-looking container.

**Lesson:** Always `docker compose down -v` between test runs.

### 4. Fair Comparison Is Hard

Cross-system comparison is only valid when configuration and guarantees are aligned.

Reference set for release-facing comparisons:
- QWER-Q
- NATS (JetStream configured and documented)
- RabbitMQ
- Kafka

### 5. Document Before Fixing

We found 7 weaknesses. By documenting each one with reproduction steps before fixing, we:
- Avoided fixing symptoms instead of causes
- Had regression tests ready
- Could verify fixes with the same benchmark

## Weakness Tracking

See `docs/benchmarks/WEAKNESSES.md` for:
- All discovered weaknesses
- Reproduction steps
- Fix status
- Before/after numbers

## Results History

See `docs/benchmarks/` for dated benchmark results:
- `2026-01-31-benchmark-results.md` - Initial comparison

Important: historical benchmark files may include exploratory runs, adapter bugs, or stale assumptions. Use `docs/benchmarks/CLAIMS-POLICY.md` before citing any number externally.

## Adding New Tests

1. Add scenario in `bench/scenarios/`
2. Add to CLI in `bench/cmd/stress/main.go`
3. Document in this README
4. Run against all queues
5. Document findings in `docs/benchmarks/`

## Adding New Queue Adapters

1. Implement `adapters.Adapter` interface:
```go
type Adapter interface {
    Name() string
    Setup(ctx context.Context) error
    Teardown() error
    Publish(ctx context.Context, queue string, payload []byte) error
    Consume(ctx context.Context, queue string, handler func([]byte) error) error
}
```

2. Add to `docker-compose.yml`
3. Register in `bench/cmd/stress/main.go`

## Tips

- **Fresh state:** Always `docker compose down -v` before benchmarks
- **Warm up:** First few seconds are often slower (JIT, connection setup)
- **Multiple runs:** Results vary; run 3+ times and average
- **Check memory:** Use `docker stats` alongside benchmarks
- **Read logs:** `docker compose logs -f qwer-q` shows errors
