# Optimization Loop 5: Focused Queue-Path Benchmark

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

Add a tighter broker-level benchmark for the queue-first durable path, then use profiling from that benchmark to drive the next optimization.

## Scope

Measured path:
- broker publish
- queue delivery to consumer
- storage save
- ACK
- storage delete

This loop intentionally avoids the full network/client harness so we can see broker + storage costs more directly.

## Benchmark Command

```bash
go test ./internal/broker -run '^$' -bench BenchmarkPersistentPublishDequeueAck -benchmem -count=3
```

## Baseline Results

Initial focused benchmark:

```bash
go test ./internal/broker -run '^$' -bench BenchmarkPersistentPublishDequeueAck -benchmem -count=3
```

Results:
- `14.97-15.50 us/op`
- `6560-6576 B/op`
- `91 allocs/op`

## Profile Findings

Allocation profile before the first change showed:
- `bytes.growSlice`: `116.63MB`
- `internal/storage.encodeMessage`: `94.60MB`
- `internal/storage.msgKey`: `8.50MB`
- `Broker.HandlePublish` cumulative allocation share dominated by storage encode/write cost

CPU profile was noisy because Badger background work and sync activity show up strongly in this short benchmark, so alloc-space was the more useful signal for this loop.

## Change Implemented

Tightened the queue-message codec again:
- moved from binary v1 to binary v2 for queue messages
- stopped storing `ID` and `Queue` redundantly in the message payload because both are derivable from the Badger key
- pre-sized the v2 buffer exactly before encoding
- kept decode compatibility for:
  - legacy JSON payloads
  - binary v1 payloads

Files changed:
- `internal/storage/codec.go`
- `internal/storage/badger.go`
- `internal/storage/codec_test.go`
- `internal/broker/bench_test.go`

## Results After Change

Focused benchmark reruns were noisy:
- `43.2-52.3 us/op` in one sample set
- the short benchmark is sensitive to Badger background flush timing

But the allocation profile improved materially:
- `bytes.growSlice`: `33.53MB` (down from `116.63MB`)
- `internal/storage.encodeMessage`: `27.53MB` (down from `94.60MB`)

## Readout

What worked:
- the focused benchmark exposed a real broker+storage round-trip path we can profile directly
- the codec tightening significantly reduced the encode-side allocation hotspot

What did not work well enough yet:
- the new benchmark is too noisy to use as the sole optimization scoreboard while background Badger work is active
- per-op allocation count stayed at `91 allocs/op`, so the transaction/storage backend still dominates the round-trip path

## Decision

Keep the new benchmark and codec changes.

For the next loop, add a second benchmark that isolates queue backlog/dispatch overhead without Badger timing noise, then attack the broker queue data structure if the O(n) head-shift shows up as expected.
