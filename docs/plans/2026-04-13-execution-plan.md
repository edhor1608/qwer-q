# QWER-Q Staged Execution Plan

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

Turn the product thesis into an execution program that keeps QWER-Q aligned with its intended category:

**the best self-hosted typed durable queue broker that runs as a simple service**

## Phase 1: Product And Benchmark Charters

Status: complete

Outputs:
- `docs/plans/2026-04-13-product-charter.md`
- `docs/plans/2026-04-13-benchmark-charter.md`
- `DEC-032`
- `DEC-033`

Purpose:
- lock the category
- lock the must-win metrics
- stop roadmap and benchmark drift

## Phase 2: Product-Shaped Benchmark Layers

Status: materially complete for the first pass

Implemented benchmark layers:
- `queue-core` scenario in `bench/cmd/bench`
- `typed-queue` scenario in `bench/cmd/bench`
- `operator-core` scenario in `bench/cmd/bench`

Current meaning:
- `queue-core` measures throughput, latency, ordering, and redelivery behavior for the queue category
- `typed-queue` measures QWER-Q's schema-enforced contract path and invalid publish rejection
- `operator-core` measures backlog drain, crash durability, and restart recovery behavior

Representative results captured during this phase:
- QWER-Q `queue-core`: around `5.2K-7.1K msg/s`, `100% ordering`, redelivery visible
- QWER-Q `typed-queue`: around `4.6K-6.4K valid msg/s`, `100%` invalid rejection
- QWER-Q `operator-core`: `31.6K drain/sec`, `0.00%` loss in the current crash sample, ~`67ms` recovery

## Phase 3: Build Lanes Driven By The Charter

Status: started

### Lane A: USP Hardening

Current progress:
- typed-queue benchmark implemented
- Go typed client layer added (`PublishProto`, `ConsumeProto`, `SchemaRegisterMessage`)

Focus:
- schema and typed workflow UX
- API/dashboard/admin ergonomics
- auth and operator experience
- DLQ usability
- product scorecard work

Success signal:
- wins in the typed queue benchmark and product scorecard

### Lane B: Queue Engine

Current progress:
- queue-core and operator-core now exist as scoreboards
- queue-engine work is now constrained by those scoreboards instead of generic throughput alone

Focus:
- durability-path cost
- storage architecture
- publish/dequeue/ack persistence economics
- recovery behavior
- small-container efficiency

Success signal:
- wins in queue-core and operator-efficiency benchmarks

## Immediate Next Steps

1. Expand the product scorecard layer.
2. Add more same-class comparisons on the new scoreboards.
3. Keep hardening the typed/operator story where it clearly improves category fit.
4. Only take larger queue-engine redesign steps when they improve the product-shaped benchmarks against the must-win class.

## Rule Going Forward

A change is strategically important only if it improves:
- queue-core
- typed queue
- operator efficiency
- or the product scorecard

If it only improves a disconnected microbenchmark, it is not enough.
