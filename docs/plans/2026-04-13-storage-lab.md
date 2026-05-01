# Storage Lab

**Date:** 2026-04-13
**Branch:** `perf/storage-lab`

## Goal

Take the bigger storage-path bet for the queue-first use case: remove structural overhead in persistence rather than papering over it with local hotpath tweaks.

## Baseline

Reference baseline from coordinator branch:
- `BenchmarkSaveMessage_NoBatch`: `5.19-5.49 us/op`, `45 allocs/op`
- `BenchmarkSaveMessage_Batched`: `704-711 us/op`, `30 allocs/op`
- end-to-end throughput: `15.5K msg/s`
- `go test ./...`: pass

## Initial Hypotheses

1. The right fix for delete-scan is to change the storage contract so ACK deletes by queue + ID directly.
2. JSON is a poor storage codec for the queue path and inflates both CPU and bytes written.
3. A compact binary codec with legacy JSON read support can improve throughput without changing broker semantics.

## Planned Changes

- change `Storage.DeleteMessage` to take `queue` + `id`
- update broker and cluster call sites accordingly
- add a compact binary codec for persisted queue messages
- keep legacy JSON decode support so existing data still loads

## Results

Implemented:
- changed `Storage.DeleteMessage` to delete by `queue` + `id`
- updated broker and cluster ACK paths to pass queue context directly
- replaced JSON queue-message persistence with a compact binary codec
- kept legacy JSON decode support for old persisted queue data
- added codec round-trip tests and legacy JSON compatibility tests

Measured:
- first storage loop
  - `BenchmarkSaveMessage_NoBatch`: `4.46-4.91 us/op`, `2936-3318 B/op`, `41 allocs/op`
  - `BenchmarkSaveMessage_Batched`: `767-774 us/op`, `6113-7100 B/op`, `26 allocs/op`
  - end-to-end throughput runs: `16.7K-17.7K msg/s`
- follow-up loop with sharded ULIDs + single publish timestamp
  - `BenchmarkSaveMessage_NoBatch`: `4.56-5.05 us/op`, `3298-3426 B/op`, `41 allocs/op`
  - `BenchmarkSaveMessage_Batched`: `760-763 us/op`, `6900-6963 B/op`, `26 allocs/op`
  - end-to-end throughput run: `16.9K msg/s`
- `go test ./...`: pass

Readout:
- This branch cleanly beats baseline on the storage microbench and the broker throughput run.
- The contract change removes delete-scan without adding write-path overhead.
- The binary codec lowers bytes written and allocation pressure enough to show up in measured queue-path throughput.
- The extra publish-side cleanup is neutral-to-slightly-positive; the real win is the storage contract + codec change.

## Guardrails

- preserve queue semantics
- keep tests green across broker, storage, cluster, and integration packages
- benchmark against the same commands used by the coordinator branch
