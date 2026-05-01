# Parallel Optimization Lab

**Date:** 2026-04-13
**Coordinator Branch:** `perf/parallel-lab`

## Problem

QWER-Q has richer broker semantics than narrowly optimized queue systems, but the queue-first hot path still carries avoidable storage, allocation, and acknowledgement overhead.

We want to push the single-node durable queue path much further without blindly committing to one architecture bet.

## Decision

Run two isolated experiments from the same `main` baseline:

- `perf/hotpath-lab` in `../qwer-q-hotpath`
- `perf/storage-lab` in `../qwer-q-storage`

## Shared Baseline

Use the same commands before and after each experiment:

```bash
go test -run '^$' -bench 'BenchmarkSaveMessage_(NoBatch|Batched)$' -benchmem -count=3 ./internal/storage
go run ./bench/cmd/bench/main.go --queue qwerq --scenario throughput --duration 5s --qwerq-addr 127.0.0.1:19876
go test ./...
```

## Baseline Results

Environment:
- machine: Apple M2 / macOS
- branch point: `d24b888`
- broker benchmark config: local broker on `127.0.0.1:19876`, 1KB messages, 5s duration

Results:
- `BenchmarkSaveMessage_NoBatch`: `5.19-5.49 us/op`, `4192-4724 B/op`, `45 allocs/op`
- `BenchmarkSaveMessage_Batched`: `704-711 us/op`, `6817-6899 B/op`, `30 allocs/op`
- end-to-end throughput (`bench/cmd/bench`, qwerq only): `77.6K published`, `15.5K msg/s`
- `go test ./...`: pass

Notes:
- The existing batched storage benchmark is latency-shaped because it waits for batch flush completion, so it is useful as a regression guard but not sufficient as the only throughput signal.
- The end-to-end throughput command is the primary comparator for experiment branches unless a better focused queue-path benchmark is added.

## Experiment A: Hot Path

Goal: improve the existing Go + Badger queue path with low-risk, high-leverage changes.

Initial targets:
- remove delete-by-scan on ACK
- reduce storage serialization overhead
- reduce allocation pressure in message ID generation and key building
- tighten publish/ack write paths without changing queue semantics

## Experiment B: Storage Path

Goal: test a more invasive queue-first storage shape while staying inside the current Go broker.

Initial targets:
- stronger direct-key persistence layout for queue messages
- lower write amplification on publish and delete
- cleaner persistence of lookup metadata needed by ACK/NACK/DLQ flows
- preserve current broker semantics and tests while changing storage internals

## Success Criteria

- measurable throughput improvement over baseline
- no regression in `go test ./...`
- no regression in durability semantics relative to the chosen configuration
- changes remain explainable and maintainable

## Experiment Comparison

| Branch | Main Idea | Throughput Result | Readout |
|---|---|---:|---|
| `perf/hotpath-lab` | O(1) delete via message-ID lookup index, ULID sharding, publish cleanup | `15.3K-16.3K msg/s` | Structural cleanup, but not a decisive win. Extra lookup writes mostly offset the ACK-side gain. |
| `perf/storage-lab` | Delete by `queue + id`, binary queue-message codec, legacy JSON read support | `16.7K-17.7K msg/s`, follow-up `16.9K msg/s` | Clear winner. Better storage-path economics without regressing correctness. |

## Outcome

Current winning direction: `perf/storage-lab` in `../qwer-q-storage`.

Why it won:
- removes delete-scan without paying for an extra write on publish
- lowers queue-message storage overhead with a compact binary codec
- preserves existing behavior well enough to keep `go test ./...` green

Recommended next step:
- continue from `perf/storage-lab`
- add a stronger queue-path benchmark that isolates publish+ack persistence cost more directly than the current broad broker benchmark
- keep stacking only changes that move measured throughput or allocation pressure in the queue-first path

Current branch validation after promoting the winning changes:
- `go test ./...`: pass
- `BenchmarkSaveMessage_NoBatch`: `4.51-5.20 us/op`, `41 allocs/op`
- `BenchmarkSaveMessage_Batched`: `704-707 us/op`, `26 allocs/op`
- initial end-to-end throughput reruns in current branch: `15.6K-16.6K msg/s`

## Follow-Up Loops On The Coordinator Branch

Additional kept improvements after the storage win:
- queue-message codec v2 removed redundant `ID` and `Queue` fields from stored queue payloads while keeping binary v1 and legacy JSON read compatibility
- per-message traffic logs (`publish`, `delivery`, `nack`) were demoted from `INFO` to `DEBUG`
- hot client/server wire paths now use direct frame writes instead of always building a second combined frame buffer
- the server publish handler now writes `PUBLISH_ACK` frames directly on the hot path instead of building a fully encoded response frame first
- exact wire-compatible fast paths now cover:
  - publish ack payload encode on the server
  - publish ack payload decode on the client with protobuf fallback
  - simple client publish request encode
  - simple client consume request encode
  - client ack request encode

Measured findings from later loops:
- focused broker round-trip benchmark exposed the publish/dequeue/ack persistence path directly, but remained noisy under Badger background sync
- queue backlog benchmark proved the current queue head-shift is O(n) under deep backlog; a rewrite achieved a big synthetic win but was reverted because the broader benchmark signal was not strong enough
- delete batching was explicitly tested and rejected because blocking-until-flush semantics made the queue round trip slower
- early decode-buffer release was explicitly tested and rejected because protobuf/input ownership was not safe enough
- runtime metrics were benchmarked directly and were too cheap in isolation to justify being the next main optimization target

Current readout:
- the kept storage-path changes remain the clearest durable win
- later runtime-path wins are now concentrated on less chatty logging plus cheaper client/server wire handling
- the live throughput harness is still noisy enough that protocol/storage microbenches are required alongside it

Current conservative benchmark picture on this branch:
- storage save no-batch: improved from baseline `5.19-5.49 us/op` to `4.51-5.20 us/op`
- protocol framing microbench:
  - `BenchmarkEncodeFrame1KB`: `183.4-264.5 ns/op`, `1152 B/op`
  - `BenchmarkWriteFrame1KB`: `65.5-68.1 ns/op`, `80 B/op`
- publish ack microbench:
  - fast encode: `15.7-25.3 ns/op`
  - protobuf marshal: `64.8-66.5 ns/op`
  - fast decode: `14.0-19.2 ns/op`
  - protobuf unmarshal: `75.6-76.0 ns/op`
- simple client request microbench:
  - fast publish encode: `153.9-245.8 ns/op`
  - protobuf publish marshal: `174.2-302.4 ns/op`
  - fast ack encode: `14.0-14.5 ns/op`
  - protobuf ack marshal: `48.6-50.3 ns/op`
- live 10-second throughput runs on the coordinator branch still vary materially, but repeated kept-code samples reached `14.9K`, `16.5K`, and `16.6K msg/s`

## Notes

The queue-first path is the priority. Stream mode and clustering stay out of scope unless a shared hot-path change materially benefits them without increasing risk.
