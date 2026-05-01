# QWER-Q Benchmark Charter

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Purpose

Benchmarks should measure the product QWER-Q is trying to be.

The goal is not to win unrelated benchmarks against systems optimized for different categories.
The goal is to be number one in the benchmark that matches QWER-Q's actual product promise.

## Benchmark Philosophy

Benchmarking must be:
- product-shaped
- reproducible
- conservative
- explicit about guarantees
- explicit about competitor class

## Primary Benchmark Target

QWER-Q should aim to win the benchmark for:

**self-hosted typed durable queue brokers with strong queue semantics and simple operations**

That benchmark should reward:
- durability
- redelivery semantics
- DLQ-compatible queue behavior
- typed validation path
- simple operational footprint

Not just raw byte movement.

## Benchmark Layers

### 1. Queue-Core Benchmark

This is the first implemented product benchmark.
It measures the shared queue category baseline:
- durable-ish queue publish/consume/ack loop in the existing harness
- latency under queue semantics
- ordering behavior
- redelivery behavior

This suite is for identifying whether QWER-Q is strong at the actual queue problem, not just at publishing bytes.

### 2. Typed Queue Benchmark

This measures QWER-Q's differentiator:
- publish with schema validation enabled
- strict-mode queue behavior
- invalid-message rejection cost
- typed-client path where available

This is a must-win area because typing is part of the product identity.

### 3. Operator Efficiency Benchmark

This measures the deployment and ops promise:
- small-container memory footprint
- startup time
- backlog recovery time
- dashboard/API/metrics availability
- durability/recovery behavior after crash or restart

### 4. Product Scorecard

This captures built-in broker value not visible in pure throughput:
- schema registry
- auth
- API
- dashboard
- metrics
- DLQ workflows
- consumer groups
- ordering keys
- request/reply

## Competitor Classes

### Must-Win Class

Systems competing for the same buyer intent:
- QWER-Q
- `queued`
- RabbitMQ
- NATS/JetStream configured for comparable guarantees

### Reference Class

Useful context, but not the primary scoreboard:
- Kafka / Redpanda
- Pulsar
- SQS
- Redis list/pubsub setups

## Fairness Rules

1. State guarantees clearly for every system under test.
2. Distinguish durable vs non-durable configurations.
3. Run systems under comparable resource limits where possible.
4. Separate "same category" comparisons from "reference only" comparisons.
5. Never present an apples-to-oranges comparison as a product verdict.

## Metrics Priority

### Primary
- queue-core throughput under the intended guarantees
- queue-core latency
- ordering correctness
- redelivery behavior
- crash/restart durability

### Secondary
- raw max throughput
- peak fanout
- stream-oriented replay metrics
- distributed-scale claims

## Current Implementation Plan

### Phase 1
Lock the product and benchmark charters.

### Phase 2
Implement the first product-shaped benchmark suite in `bench/`:
- `queue-core`
- shared queue semantics scorecard
- reproducible output in the existing benchmark CLI

### Phase 3
Use that suite to drive two main implementation lanes:
- USP hardening
- queue engine / durability path optimization

## Acceptance Rule

Future optimization work should only be considered a strategic win if it improves:
- the queue-core benchmark
- the typed queue benchmark
- or the operator efficiency benchmark

If it only improves an unrelated benchmark, it is not a product win.
