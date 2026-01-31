# Benchmark Process

## Goals
1. Find weaknesses, not just good results
2. Run all systems under identical conditions (Docker, same resources)
3. Document findings before fixing
4. Fix one issue at a time, re-benchmark to verify

---

## Benchmark Suite

### Available Tests

```bash
# Stress tests
go run bench/cmd/stress/main.go --queues=qwerq,nats,kafka --tests=sustained --duration=30s

# Weakness finding
go run bench/cmd/weakness/main.go --queues=qwerq,nats,kafka --tests=all --skip-docker
```

### Test Categories

| Test | Purpose |
|------|---------|
| sustained | Throughput over time with memory tracking |
| concurrency | Multiple producers/consumers |
| sizes | Different message sizes |
| depth | Pre-filled queue performance |
| burst | Handling traffic spikes |
| lag | Consumer catching up |
| breaking | Find throughput limits |
| memory | Memory pressure behavior |
| connections | Connection storm handling |
| recovery | Crash recovery verification |

---

## Systems Under Test

| System | Container | Resources | Notes |
|--------|-----------|-----------|-------|
| QWER-Q | bench-qwerq-1 | 1 CPU, 512MB | Our queue |
| NATS | bench-nats-1 | 1 CPU, 512MB | JetStream enabled |
| RabbitMQ | bench-rabbitmq-1 | 1 CPU, 512MB | Management UI |
| Redis | bench-redis-1 | 1 CPU, 512MB | List-based queue |
| Kafka | bench-kafka-1 | 1 CPU, 512MB | With Zookeeper |

---

## Running Benchmarks

### Start Environment
```bash
docker compose -f bench/docker-compose.yml up -d
```

### Run All Tests
```bash
# Full stress suite
go run bench/cmd/stress/main.go --duration=60s

# Weakness finding
go run bench/cmd/weakness/main.go --skip-docker
```

### Specific Comparisons
```bash
# Just QWER-Q vs NATS
go run bench/cmd/stress/main.go --queues=qwerq,nats --tests=sustained

# Just QWER-Q vs Kafka
go run bench/cmd/stress/main.go --queues=qwerq,kafka --tests=sustained
```

---

## Documentation Workflow

1. Run benchmark suite
2. Copy results to `docs/benchmarks/YYYY-MM-DD-benchmark-results.md`
3. Update `docs/benchmarks/WEAKNESSES.md` with any new findings
4. Create issue for each weakness before fixing
5. Fix one issue, re-run relevant benchmark
6. Update documentation with fix verification

---

## Metrics to Capture

### Throughput
- Messages/second (publish)
- Messages/second (consume)
- End-to-end latency

### Resources
- Memory usage over time
- CPU usage
- Network I/O
- Disk I/O (for persistent queues)

### Reliability
- Message loss rate
- Error rate under load
- Recovery after crash

### Scalability
- Breaking point (when errors start)
- Behavior at limit
- Graceful degradation
