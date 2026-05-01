# Optimization Loop 10: Server Publish-Ack Hot Path

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

Resume from the saved state and attack the next live server-path bottleneck after the earlier storage, logging, and direct delivery-write wins.

## First Check: Are Metrics The Bottleneck?

Before changing code, measure the runtime metrics path directly.

Benchmarks added:

```bash
go test ./internal/broker -run '^$' -bench 'Benchmark(RecordPublish|UpdateQueueMetrics)$' -benchmem -count=5
```

Results:
- `BenchmarkRecordPublish`: `63.8-65.0 ns/op`, `0 allocs/op`
- `BenchmarkUpdateQueueMetrics`: `75.9-77.6 ns/op`, `0 allocs/op`

Readout:
- metrics are not free
- but they are too cheap in isolation to justify being the next optimization target
- the next real bottleneck is elsewhere in the server/client wire path

## Finding

Even after introducing `WriteFrame` earlier, the server still built a full encoded response buffer for every publish ack via:
- `proto.Marshal(PublishResponse)`
- `EncodeFrame(...)`
- `conn.Write(...)`

That means every publish still paid for a second response-frame allocation/copy on the hot path.

## Change

Changed the publish handler so successful `PUBLISH_ACK` responses are written directly with `protocol.WriteFrame(...)` instead of returning a fully encoded frame buffer back to the generic connection loop.

Files changed:
- `internal/broker/server.go`

## Validation

```bash
go test ./...
```

Result:
- pass

## Benchmarks

Protocol framing benchmark remained favorable to direct writes:

```bash
go test ./internal/protocol -run '^$' -bench 'Benchmark(EncodeFrame1KB|WriteFrame1KB)$' -benchmem -count=3
```

Representative results:
- `BenchmarkEncodeFrame1KB`: `183.4-264.5 ns/op`, `1152 B/op`
- `BenchmarkWriteFrame1KB`: `65.5-68.1 ns/op`, `80 B/op`

Live server benchmark:

```bash
go run ./bench/cmd/bench/main.go --queue qwerq --scenario throughput --duration 10s --qwerq-addr 127.0.0.1:41876
```

Result after the publish-ack direct-write change:
- `148.7K published`
- `14.9K msg/s`

## Decision

Keep the change.

Why:
- it is simple
- it removes one more real copy on the hottest response path in the benchmarked client/server loop
- it preserved correctness and held up on a 10-second throughput run
