# QWER-Q — Claude Context

## What is this?

A typed, docker-first message queue built in Go. Fills the gap between Kafka (too heavy) and NATS (too minimal).

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

Early design phase. Core decisions made, implementation not started.

## What's Next

1. Finalize remaining design decisions (protocol, schema format, delivery semantics)
2. Create implementation plan
3. Build MVP broker

## Terminology

- **Queue** — Named destination for messages
- **Producer** — Publishes messages to a queue
- **Consumer** — Subscribes to a queue, receives messages
- **Ack** — Consumer confirms message processed
- **Schema** — Type definition for messages on a queue
