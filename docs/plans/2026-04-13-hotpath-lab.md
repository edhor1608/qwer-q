# Hotpath Lab

**Date:** 2026-04-13
**Branch:** `perf/hotpath-lab`

## Goal

Improve single-node queue-path throughput and reduce per-message overhead without changing the overall Go + Badger architecture.

## Baseline

Reference baseline from coordinator branch:
- `BenchmarkSaveMessage_NoBatch`: `5.19-5.49 us/op`, `45 allocs/op`
- `BenchmarkSaveMessage_Batched`: `704-711 us/op`, `30 allocs/op`
- end-to-end throughput: `15.5K msg/s`
- `go test ./...`: pass

## Initial Hypotheses

1. ACK/delete path is paying an O(n) Badger scan per delete.
2. Queue message keys and message IDs still allocate more than necessary.
3. Message ID generation can reduce lock contention under concurrent publish.
4. Repeated `time.Now()` on publish is small but free to remove.

## Planned Changes

- add direct message-ID index for O(1) delete lookup with legacy fallback
- reduce key-building allocations in storage
- shard ULID entropy sources
- collapse duplicate publish timestamps into a single `now`

## Results

Implemented:
- direct message-ID index for O(1) delete lookup with legacy scan fallback
- smaller key-building helpers in storage
- sharded ULID entropy sources
- single `now` capture in publish paths
- storage tests for direct-delete and legacy fallback

Measured:
- `BenchmarkSaveMessage_NoBatch`: `5.81-6.42 us/op`, `5011-5522 B/op`, `60 allocs/op`
- `BenchmarkSaveMessage_Batched`: `767-770 us/op`, `7965-8125 B/op`, `46 allocs/op`
- end-to-end throughput runs: `15.3K-16.3K msg/s`
- `go test ./...`: pass

Readout:
- ACK/delete complexity is fixed structurally, but the added lookup write cost mostly cancels it out in the current benchmark shape.
- This branch is a correctness-preserving cleanup with at best a marginal throughput win and no convincing step-change.
- The best throughput run was above baseline, but the range overlaps baseline enough that this branch should be treated as inconclusive rather than the winning direction.

## Guardrails

- preserve current semantics and storage behavior for existing data where feasible
- keep all existing tests green
- measure against the same baseline commands used by the coordinator branch
