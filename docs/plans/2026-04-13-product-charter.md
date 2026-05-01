# QWER-Q Product Charter

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Mission

Build the best self-hosted typed durable queue broker that a small team can run as a simple service.

## Category

QWER-Q is a **queue-first broker**.

It is not primarily:
- a distributed stream platform
- a raw transport fabric
- a workflow engine
- a cloud-managed queue service

Its center of gravity is durable queue semantics with product-level ergonomics.

## Core User

Teams that want:
- self-hosted async infrastructure
- real queue semantics and durability
- typed message contracts
- simple deployment and operations
- more product surface than a bare messaging transport

## Core Use Case

A team wants to run one service and get:
- durable publish/consume/ack/nack
- visibility timeout and redelivery
- DLQ workflows
- consumer groups and ordering controls
- schema validation at the broker boundary
- metrics, API, dashboard, health, and auth

Without adopting Kafka-class operational complexity.

## Product Promise

QWER-Q should be the best choice when someone says:

> I want a real durable queue with contracts, DLQ, groups, ordering, admin surface, and simple self-hosting. I do not want Kafka, and I want more than a minimal transport.

## Must-Win Dimensions

1. **Simple ops**
   - one binary
   - one container
   - low setup friction
   - clear health/metrics/admin surface

2. **Real queue semantics**
   - at-least-once delivery
   - ack/nack
   - visibility timeout
   - redelivery
   - DLQ support

3. **Typed contracts**
   - broker-enforced schema validation
   - clear strict vs permissive modes
   - good typed client workflows

4. **Useful application semantics**
   - consumer groups
   - ordering keys
   - request/reply

5. **Strong single-node efficiency for this feature set**
   - durable queue path should be competitive for the guarantees QWER-Q provides
   - memory/disk/latency should stay reasonable in small-container deployments

## Stable Identity

QWER-Q's stable identity is:
- queue mode
- typed contracts
- auth
- metrics
- REST API
- dashboard
- DLQ and delivery controls
- simple self-hosting

## Preview Capabilities

Implemented but not product-defining today:
- stream mode
- clustering

These may mature later, but they should not pull QWER-Q away from its queue-first identity.

## Non-Goals

QWER-Q is not trying to be the best at:
- highest distributed scale
- replay-first event streaming
- routing-pattern breadth
- workflow orchestration / DAG scheduling
- managed cloud convenience

## Competitor Framing

### In-Category Competitors

These matter most because they overlap with the buying decision:
- `queued`
- RabbitMQ
- NATS/JetStream
- Redis-backed queue stacks in some workloads

### Reference Competitors

These matter for context, not direct product identity:
- Kafka / Redpanda
- Pulsar
- SQS

## Product Principles

1. Optimize for the intended category, not for generic messaging prestige.
2. Keep queue mode as the default mental model.
3. Treat typed validation and broker ergonomics as first-class features, not extras.
4. Prefer simple, local, operationally clear designs over platform sprawl.
5. Measure success against product-shaped benchmarks, not unrelated headline numbers.

## Implementation Rule

A change is aligned only if it makes QWER-Q better at being:

**the best self-hosted typed durable queue broker that runs as a simple service**.
