# Optimization Loop 7: Delete Batching Symmetry

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

Test whether extending the existing queue-message batcher from publish writes to ACK-path deletes improves the queue-first durable round trip.

## Why This Was Worth Testing

Queue saves already had optional batching, but queue deletes always used a standalone Badger transaction.
The obvious hypothesis was that making batching symmetric would reduce ACK-path transaction overhead.

## Benchmarks Added

```bash
go test ./internal/storage -run '^$' -bench 'Benchmark(DeleteMessage|SaveDeleteMessage)_' -benchmem -count=3
```

## Baseline Results

Before changing delete behavior:
- `BenchmarkDeleteMessage_NoBatch`: `4.09-4.49 us/op`, `36-37 allocs/op`
- `BenchmarkDeleteMessage_Batched`: `3.82-4.29 us/op`, `36 allocs/op`
- `BenchmarkSaveDeleteMessage_NoBatch`: `8.38-8.54 us/op`, `76 allocs/op`
- `BenchmarkSaveDeleteMessage_Batched`: `700-707 us/op`, `61 allocs/op`

The delete benchmark showed that `DeleteMessage` ignored the batcher even when the storage instance was configured with `WithBatchInterval(...)`.

## Change Tried

Generalized the write batcher to support both:
- queue message `Set`
- queue message `Delete`

The API still blocked until flush completion, preserving the existing durability contract for callers.

## Results After Change

After true batched deletes were enabled:
- `BenchmarkDeleteMessage_Batched`: `697-703 us/op`, `22 allocs/op`
- `BenchmarkSaveDeleteMessage_Batched`: `1.40-1.41 ms/op`, `46 allocs/op`

## Readout

The experiment answered the important question clearly:
- batching deletes reduced allocation count
- but under the current blocking semantics it inserted an additional batch-flush wait into the publish/dequeue/ack lifecycle
- that made the queue-first round trip materially slower

This is the wrong trade-off for QWER-Q’s current queue semantics.

## Decision

Reject delete batching for now and revert it.

Keep:
- the new delete and save+delete benchmarks in `internal/storage/batcher_test.go`

Do not keep:
- batched `DeleteMessage`
- batcher changes that mix deletes into the same blocking queue-operation flush path

## Follow-Up Trigger

Revisit only if QWER-Q adopts a different durability contract for ACK deletes or a fundamentally different write pipeline that can batch without adding a second caller-visible wait.
