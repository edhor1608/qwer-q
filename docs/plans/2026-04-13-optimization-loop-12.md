# Optimization Loop 12: Typed Queue Benchmark Layer

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

Implement the next product-shaped scoreboard after `queue-core`:

- a benchmark for QWER-Q's typed queue USP
- real schema registration
- real valid payloads
- real invalid publish rejection
- honest unsupported results for systems without broker-enforced schemas

## Changes

Added a new `typed-queue` scenario to the benchmark CLI.

Key pieces:
- optional `TypedQueueAdapter` capability for adapters that support broker-enforced schemas
- QWER-Q adapter implements schema registration through the Go client
- typed benchmark registers a real protobuf descriptor
- valid payload path measures schema-on throughput and latency
- invalid payload path measures broker-side rejection behavior
- non-schema systems are reported as unsupported instead of being forced through a fake comparison

Files:
- `bench/adapters/adapter.go`
- `bench/adapters/qwerq.go`
- `bench/scenarios/typed_queue.go`
- `bench/harness/harness.go`
- `bench/cmd/bench/main.go`

## Benchmark Design Note

The first implementation reused the generic latency target rate and immediately saturated the schema-enabled queue path, returning `queue is full` during the latency phase.

That was a benchmark-design error, not a product verdict.

Decision:
- cap the typed latency probe at a lower default rate (`2000 msg/s`) so it measures tail latency under a controlled typed workload instead of accidentally becoming a stress test

## Why This Matters

This benchmark is strategically important because it measures a QWER-Q differentiator that raw transport benchmarks cannot represent:
- broker-enforced contracts
- typed queue behavior
- invalid publish rejection at the broker boundary

It is the first benchmark layer where unsupported competitors should explicitly lose by category, not by throughput.
