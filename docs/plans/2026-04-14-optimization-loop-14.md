# Optimization Loop 14: Typed USP Hardening And Operator Control

**Date:** 2026-04-14
**Branch:** `perf/parallel-lab`

## Goal

Use the new product-shaped scoreboards to make two concrete product improvements:
- improve the Go typed story
- improve operator control of the durability/performance tradeoff

## Change 1: Go Typed Client Layer

Added a minimal typed protobuf layer to the Go client:
- `PublishProto`
- `ConsumeProto`
- `SchemaRegisterMessage`

What this does:
- publish a protobuf message directly without hand-marshaling in user code
- consume and decode protobuf payloads directly
- register a schema from a generated protobuf message type by building a `FileDescriptorSet` from reflection

Files:
- `pkg/client/typed.go`
- `pkg/client/typed_test.go`

Validation:
- tests cover schema registration from a generated message type
- tests cover publish + consume of a typed protobuf payload against a real strict-mode broker path

## Change 2: Runtime Sync Interval Control

Exposed `--sync-interval` on `qwer-q serve`.

This closes a real operator/product gap:
- the storage layer already supported sync interval control
- the docs and decisions already assumed this should exist
- the runtime server surface did not expose it

Files:
- `cmd/qwer-q/serve.go`
- `README.md`
- `docs/plans/decisions-log.md`
- `docs/benchmarks/WEAKNESSES.md`

Quick validation runs:
- `--sync-interval=0` throughput sample: `4.4K msg/s`
- `--sync-interval=1s` throughput sample: `9.9K msg/s`

These are short runs, but they prove the flag is live and the tradeoff is real.

## Readout

This loop is important because it moves QWER-Q forward in exactly the areas the new benchmark charter says matter:
- better typed DX in the Go client
- better operator control of durability vs throughput

Neither of these is random optimization work. Both strengthen the intended product category.

## Decision

Keep both changes.

## Next Direction

With product-shaped benchmarks now in place and the typed/operator story improved, the next meaningful work should be chosen from:
- more USP hardening (typed workflow ergonomics, product scorecard, auth/admin polish)
- queue-engine redesign only where it improves queue-core against the must-win class
