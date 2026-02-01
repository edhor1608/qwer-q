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
- `docs/plans/YYYY-MM-DD-<topic>.md` for optimization loops and investigations

## Optimization Loop Documentation

**Every optimization loop must be documented with:**

1. **Executive Summary** — What improved, by how much
2. **Benchmark Progression** — Table showing before/after at each stage
3. **For each finding:**
   - Symptom (what we observed)
   - Root cause (why it happened)
   - Options considered (table with pros/cons/why not chosen)
   - Fix implemented (code snippets)
   - Result (measured improvement)
   - Decision rationale
   - Revisit triggers (when to reconsider this decision)
4. **Alternative approaches not taken** — What we could have done differently
5. **Lessons learned** — What we'd do differently next time
6. **Raw data** — Actual benchmark output for reproducibility

**Why document alternatives?** When debugging future issues, knowing what we *didn't* choose helps identify if we made the wrong call. If option B would have solved today's problem, we can revisit.

**Example:** See `docs/plans/2026-02-01-optimization-loop-1.md`

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
- BadgerDB persistence with configurable sync interval (`--sync-interval` flag)
- Memory-based backpressure
- Queue size limits (100K messages default)
- Consumer prefetch buffer (100 messages)
- Schema validation (JSON Schema)
- CLI with publish/consume/admin commands

**Performance (512MB container, 1 CPU):**
- ~5K msgs/sec with 1s sync interval
- ~2K msgs/sec with 100ms sync interval
- NATS comparison: 133K/s (but no persistence by default)

**Known tradeoffs:**
- Default 100ms sync: ~2K/s throughput, max 100ms data loss on crash
- Benchmark 1s sync: ~5K/s throughput, max 1s data loss on crash
- Use `--sync-interval=0` for sync-every-write (safest, slowest)

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
