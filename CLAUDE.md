# QWER-Q — Claude Context

## What is this?

A typed, docker-first message queue built in Go. Fills the gap between Kafka (too heavy) and NATS (too minimal).

## Branch Knowledge Documentation

**Each feature branch must document its knowledge.** When working on a branch, create/update markdown files capturing:

- What problem we're solving
- What we tried, what worked, what didn't
- Research findings (benchmarks, external docs, comparisons)
- Design decisions and rationale
- Lessons learned

This ensures knowledge accumulates as branches merge to main. Location:
- `docs/` for feature-specific docs (e.g., `docs/PERFORMANCE.md`)
- `docs/plans/decisions-log.md` for architectural decisions
- `docs/benchmarks/` for benchmark results and analysis

## Key Design Decisions

Read `docs/plans/decisions-log.md` for full context. Summary:

1. **Queue-first** — Not stream/log. Messages deleted after ack.
2. **Embedded storage** — BadgerDB or bbolt. Single binary.
3. **Simple competing consumers** — Round-robin, no consumer groups in v1.
4. **Go** — Not Rust. Faster iteration, great Docker story.

## Project Structure

```
qwer-q/
├── cmd/           # CLI entrypoints
│   └── qwer-q/    # Main broker binary
├── internal/      # Private packages
│   ├── broker/    # Core broker logic
│   ├── storage/   # Persistence layer
│   ├── protocol/  # Wire protocol
│   └── schema/    # Schema registry
├── pkg/           # Public client libraries
├── docs/
│   └── plans/     # Design docs and decisions
└── test/          # Integration tests
```

## Development Commands

```bash
# Build
go build -o bin/qwer-q ./cmd/qwer-q

# Test
go test ./...

# Run locally
./bin/qwer-q serve

# Docker
docker build -t qwer-q .
docker run -p 9876:9876 qwer-q
```

## Current State

MVP complete. Broker runs in Docker with:
- Protobuf wire protocol
- BadgerDB persistence with configurable sync interval
- Memory-based backpressure (300MB default limit)
- Queue size limits (10K messages default)
- Schema validation (JSON Schema)
- CLI with publish/consume/admin commands

**Known tradeoffs:** Default 100ms sync interval balances throughput (~3K/s) with durability (max 100ms data loss on power failure). Use `WithSyncInterval(0)` for sync-every-write mode.

## What's Next

1. Real-world testing and feedback
2. Monitoring and observability
3. Consider write batching if throughput becomes a bottleneck

## Terminology

- **Queue** — Named destination for messages
- **Producer** — Publishes messages to a queue
- **Consumer** — Subscribes to a queue, receives messages
- **Ack** — Consumer confirms message processed
- **Schema** — Type definition for messages on a queue
