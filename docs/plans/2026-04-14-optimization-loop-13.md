# Optimization Loop 13: Product-Shaped Benchmark Expansion

**Date:** 2026-04-14
**Branch:** `perf/parallel-lab`

## Goal

Move beyond raw throughput and turn the benchmark charter into executable scoreboards:
- typed queue benchmark
- operator-efficiency benchmark
- first same-class comparison on the new queue-core benchmark

## Typed Queue Benchmark

Added a `typed-queue` scenario to `bench/cmd/bench`.

What it measures:
- valid publish throughput with broker-enforced schema validation enabled
- p95 latency for the typed path under a controlled rate
- invalid publish rejection rate
- explicit `unsupported` results for systems without broker-enforced schemas

Key implementation detail:
- the typed latency probe needed its own capped rate because reusing the generic latency rate turned it into an accidental saturation test instead of a latency benchmark

Representative QWER-Q result:
- `4.6K valid msg/s`
- `100% invalid rejection`

## Operator Core Benchmark

Added an `operator-core` scenario.

What it measures:
- backlog drain speed from a prefilled queue
- crash durability under `SIGKILL`
- recovery time after restart

Important benchmark fix:
- the old depth benchmark understated drain rate by always dividing by the full timeout even if the queue drained early
- this was corrected so the drain result reflects actual recovery/drain speed

Representative QWER-Q result:
- `31.6K drain/sec`
- `0.00%` message loss in the current crash sample
- `66.94ms` recovery time

## First Same-Class Queue-Core Comparison

Ran `queue-core` for:
- QWER-Q
- NATS
- RabbitMQ

Representative result:
- QWER-Q: `5.2K msg/s`, `0.23ms p95`, `100% ordering`, redelivery present
- NATS: `766.6K msg/s`, `0.17ms p95`, no comparable queue redelivery story in this benchmark
- RabbitMQ: `5.3K msg/s`, `0.55ms p95`, `100% ordering`, redelivery present

## Readout

The important product conclusion is not that NATS is fast. That was already known.

The important conclusion is:
- QWER-Q is already in the same rough queue-core class as RabbitMQ on the first product-shaped benchmark sample
- QWER-Q is uniquely strong on the typed queue benchmark because the others are unsupported by category

That means the next highest-leverage work is not generic transport chasing.
The rational next lane is USP hardening plus selected operator improvements.

## Decision

Keep all of the new benchmark layers and the benchmark fixes.

Use them as the real scoreboards for future work.
