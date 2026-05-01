# Optimization Loop 8: Logging Noise And Direct Frame Writes

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

Attack the server/client path that the storage microbench does not see:
- per-message runtime logging
- extra frame-copy work after protobuf marshal

## Part 1: Logging Noise

### Finding

The network benchmark uses a live consumer and producer. The server was logging every:
- publish
- delivery
- nack

at `INFO` by default.

That is the wrong default for a queue expected to process large message volumes.

### Change

Demoted these logs from `INFO` to `DEBUG`:
- `LogPublish`
- `LogConsume`
- `LogNack`

Files:
- `internal/broker/logging.go`
- `internal/broker/logging_test.go`

### Readout

Recent degraded 5-second throughput runs moved from the `~5.5K-7.6K msg/s` range back into the `~11.7K-13.5K msg/s` range immediately after the log-level change.

This is a real production-path fix, not a synthetic microbench improvement.

## Part 2: Direct Frame Writes

### Finding

On the hot wire path, payloads were already protobuf-encoded, but then `EncodeFrame` allocated a second buffer and copied that payload again just to prepend a 6-byte frame header.

### Change

Added `protocol.WriteFrame` to write a frame directly without building a second combined buffer.

Hot paths switched to it:
- client `Publish`
- client `Consume`
- client `Ack`
- server queue delivery writes
- server stream delivery writes

Files:
- `internal/protocol/frame.go`
- `internal/protocol/frame_test.go`
- `pkg/client/client.go`
- `internal/broker/server.go`

### Protocol Microbench

```bash
go test ./internal/protocol -run '^$' -bench 'Benchmark(EncodeFrame1KB|WriteFrame1KB)$' -benchmem -count=5
```

Results:
- `BenchmarkEncodeFrame1KB`: `131.6-163.9 ns/op`, `1152 B/op`, `1 alloc/op`
- `BenchmarkWriteFrame1KB`: `62.0-72.7 ns/op`, `80 B/op`, `3 alloc/op`

Interpretation:
- direct frame writes roughly halved the local framing cost
- byte allocation dropped sharply because the full payload copy disappeared
- the remaining small allocations come from the `net.Buffers` write path

### End-To-End Readout

The live network harness remained noisy, but initial runs after this change reached:
- `14.7K-17.4K msg/s` on 5-second runs
- `14.9K msg/s` on one 10-second run

Later reruns varied lower, so the protocol microbench is the cleaner proof for this specific change.

## Decision

Keep both changes:
- per-message traffic logs default to `DEBUG`
- direct frame writes in the client/server hot path

## Why These Stay

They are:
- simple
- low-risk
- easy to reason about
- aligned with the queue-first use case
- supported by either direct end-to-end improvement, direct microbench improvement, or both
