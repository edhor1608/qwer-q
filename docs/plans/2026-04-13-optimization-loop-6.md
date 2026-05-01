# Optimization Loop 6: Queue Backlog Dispatch

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

Measure the broker queue backlog path without storage noise and determine whether the current O(n) slice-head removal is worth replacing.

## Benchmark Command

```bash
go test ./internal/broker -run '^$' -bench BenchmarkQueueBacklogDispatchAck -benchmem -count=5
```

## Benchmark Shape

- keep backlog depth fixed while varying backlog size
- consume one delivered message
- ACK it
- append one pre-built replacement message
- repeat

This isolates dispatch + ACK behavior while keeping the queue under sustained backlog.

## Baseline Results

```bash
go test ./internal/broker -run '^$' -bench BenchmarkQueueBacklogDispatchAck -benchmem -count=3
```

Results before the queue rewrite:
- backlog `128`: `164-204 ns/op`
- backlog `1024`: `320-387 ns/op`
- backlog `8192`: `2.02-2.32 us/op`
- backlog `65536`: `22.8-27.8 us/op`

## Profile Finding

CPU profile for backlog `65536`:
- `runtime.memmove`: `80.89%`
- cumulative path: `Queue.Ack -> Queue.tryDeliver -> typedslicecopy/memmove`

The O(n) head-of-slice removal is real and dominant under deep backlog.

## Change Tried

Replaced per-delivery slice shifting with an indexed pending window plus periodic compaction in `internal/broker/queue.go`.

## Synthetic Result After Change

Results after the queue rewrite:
- backlog `128`: `239-620 ns/op`
- backlog `1024`: `238-253 ns/op`
- backlog `8192`: `273-359 ns/op`
- backlog `65536`: `322-391 ns/op`

This is a major synthetic win at deep backlog.

## Why It Was Not Kept

Follow-up broad broker throughput checks did not show a trustworthy improvement and were often lower than earlier runs. The broader benchmark signal is currently noisy enough that this queue rewrite cannot be justified as a kept optimization yet.

Decision:
- keep the new backlog benchmark and profile findings
- revert the queue rewrite for now
- revisit with a better broker-level scoreboard before reintroducing the indexed queue structure

## Current State

- `internal/broker/queue.go`: reverted to the pre-loop implementation
- benchmark coverage kept in `internal/broker/bench_test.go`
- `go test ./...`: pass after revert
