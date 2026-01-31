# QWER-Q — Claude Context

## What is this?

A typed, docker-first message queue built in Go. Fills the gap between Kafka (too heavy) and NATS (too minimal).

## Code Philosophy

**Minimize code, maximize understanding.**

Before writing code:
1. **Question first** — Is new code needed? Can existing code be changed? Should wrong code be deleted?
2. **Explain the why** — Verbose reasoning before any change. What problem does this solve?
3. **Simple > clever** — Easy to read, easy to see the concepts. No magic.
4. **One change, one purpose** — Small, focused changes that can be challenged and verified.

Don't just add code to fix symptoms. Find root causes. Delete dead paths. Reuse what exists.

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
- BadgerDB persistence with sync writes
- Memory-based backpressure (300MB default limit)
- Queue size limits (10K messages default)
- Schema validation (JSON Schema)
- CLI with publish/consume/admin commands

**Known tradeoffs:** Sync writes give durability but limit throughput (~500/s). This is intentional — QWER-Q prioritizes data safety over speed.

## What's Next

1. Real-world testing and feedback
2. Consider write batching if throughput becomes a bottleneck
3. Monitoring and observability

## Terminology

- **Queue** — Named destination for messages
- **Producer** — Publishes messages to a queue
- **Consumer** — Subscribes to a queue, receives messages
- **Ack** — Consumer confirms message processed
- **Schema** — Type definition for messages on a queue
