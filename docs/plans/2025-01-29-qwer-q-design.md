# QWER-Q: Typed, Docker-First Message Queue

> Design document — Last updated: 2025-01-29

## Vision

Build a **modern, docker-first, extremely easy-to-use message queue** that fills the gap between:
- **Kafka** — too heavy (ops complexity, multiple components)
- **NATS** — too minimal (missing advanced guarantees)

With **typing as a first-class feature** (not an afterthought).

### Target DX
```bash
docker run -p 9876:9876 qwer-q
# That's it. You have a production-viable message queue.
```

---

## The Problem

### A) Ops/Hosting Friction (Kafka pain)
- Non-trivial hosting and orchestration
- Multiple components and operational overhead
- Managed offerings cost money or have limiting free tiers

### B) End-to-End Typing Breaks with Message Queues
tRPC provides excellent DX for HTTP/RPC. But with MQ-based microservices:
- You lose "typed-by-default"
- You must manually build: shared types, validations, glue code, versioning

**Insight:** This should be one cohesive product, not a bunch of glue.

---

## Project Structure

Two separate projects:

### 1) Core Project: QWER-Q (this repo)
The message queue broker itself.

**Focus:**
- Simple deployment (single binary, single container)
- Lightweight and fast
- High developer experience
- Typed-by-default (schema registry built-in)

### 2) Companion Project: Gateway (separate repo, later)
tRPC-inspired "actions over MQ".

**Goal:**
- tRPC-like DX for microservice architectures
- "Call an action" feeling: frontend → gateway → services
- Adapter architecture for multiple brokers (NATS/Rabbit/Kafka/QWER-Q)

**Strategic value:**
- Useful early, even before QWER-Q is mature
- Validates the "tRPC-over-MQ" thesis with real users
- Becomes valuable independently

---

## Decisions Made

### 1. Primary Mental Model: Queue-First
| Aspect | Decision |
|--------|----------|
| **Model** | Queue (RabbitMQ-style), not Stream (Kafka-style) |
| **Message fate** | Consumed + ack'd = deleted |
| **Primary use case** | Task distribution, request/response, actions |
| **Stream features** | Optional, added later |

**Rationale:** The "typed actions" gateway maps cleanly to work distribution. Stream semantics (replay, event sourcing) can be layered on later.

### 2. Persistence: Embedded Durable Storage
| Aspect | Decision |
|--------|----------|
| **Model** | Embedded DB (BadgerDB or bbolt) |
| **Deployment** | Still single binary, single container |
| **Durability** | Survives restarts |

**Rationale:**
- Production-viable from day one
- Avoids "this is just a toy" perception
- Not much harder than in-memory in Go
- No external dependencies

### 3. Consumer Model: Simple Competing Consumers
| Aspect | Decision |
|--------|----------|
| **Model** | Multiple consumers on same queue = round-robin distribution |
| **Consumer groups** | Not in v1, can add later |

**Rationale:**
- Primary use case is "gateway calls service actions" — that's work distribution
- Consumer groups add complexity (membership tracking, rebalancing, heartbeats)
- Keeps v1 scope tight

### 4. Implementation Language: Go
| Aspect | Decision |
|--------|----------|
| **Language** | Go |
| **Why not Rust** | Faster iteration, simpler mental model, great for AI-native development |

**Rationale:**
- Faster to stable server
- Clean CLI story
- Excellent Docker story (static binaries)
- Rust can be introduced later for hot paths if needed

---

## Decisions Pending

### Delivery Semantics
- [ ] At-least-once vs exactly-once
- [ ] Ack model: per message vs per batch
- [ ] Retry/redelivery rules, timeouts, backoff

### Protocol/Transport
- [ ] Custom binary protocol vs gRPC vs simpler
- [ ] TLS strategy

### Schema Registry (core of "typed MQ")
- [ ] Schema format (Protobuf, JSON Schema, custom)
- [ ] Registration UX
- [ ] Compatibility rules (backward/forward/full)
- [ ] Where validation happens (producer, broker, consumer)

### Security
- [ ] Local/dev: open by default?
- [ ] Prod: TLS + auth strategy
- [ ] ACLs per queue/type

### Observability
- [ ] Metrics endpoint format
- [ ] Health endpoints
- [ ] Logging strategy

### Clustering (post-MVP)
- [ ] Single-node first, HA later
- [ ] Replication strategy
- [ ] Consensus (Raft-like)

---

## Non-Goals for v1

- Exactly-once delivery (at-least-once with idempotency helpers instead)
- Consumer groups
- Stream/log semantics
- Multi-node clustering
- Complex ACLs

---

## Success Criteria

1. **`docker run` and it works** — No config files required for basic use
2. **Type-safe by default** — Producers can only publish valid messages, consumers get typed data
3. **Sub-millisecond latency** — For in-memory hot path
4. **Durable** — Survives broker restarts
5. **Observable** — Prometheus metrics, health endpoints

---

## Open Questions to Explore

1. What schema format best balances DX and performance?
2. How should the CLI/admin interface work?
3. What's the wire protocol?
4. How do clients discover schema types?
