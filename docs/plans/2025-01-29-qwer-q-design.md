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

### 5. Delivery Semantics: Visibility Timeout
| Aspect | Decision |
|--------|----------|
| **Redelivery model** | Visibility timeout (SQS-style) |
| **Behavior** | Message invisible for N seconds after delivery, requeued if no ack |
| **Guarantees** | At-least-once |

**Rationale:**
- Simple to understand and implement
- Works even with dumb clients
- Consumer crash = automatic retry after timeout

### 6. Wire Protocol: Custom Binary over TCP
| Aspect | Decision |
|--------|----------|
| **Protocol** | Custom binary framed protocol |
| **Transport** | TCP (+TLS for prod) |
| **Why not gRPC** | Maximum control, optimize every byte |

**Rationale:**
- Full control over wire format
- Optimize for specific use cases
- Aligns with "build a real alternative" mindset

### 7. Schema Format: Protobuf
| Aspect | Decision |
|--------|----------|
| **Format** | Protocol Buffers |
| **Storage** | Compiled descriptors in schema registry |
| **Codegen** | Standard protoc/buf tooling |

**Rationale:**
- Battle-tested compatibility rules
- Binary efficiency
- Don't build a schema language when building a protocol

### 8. Security: Open by Default
| Aspect | Decision |
|--------|----------|
| **Default** | No auth required |
| **Production** | Opt-in via config/env vars |

**Rationale:** "docker run and it works" is the headline. Clear warnings when running without auth.

### 9. Observability: Prometheus Metrics
| Aspect | Decision |
|--------|----------|
| **Metrics** | Prometheus `/metrics` endpoint |
| **Web UI** | Deferred to post-v1 |
| **Logs** | Structured JSON |

### 10. Schema Registration: CLI + API
| Aspect | Decision |
|--------|----------|
| **Primary** | CLI tool (`qwer-q schema register`) |
| **Underlying** | Protocol API for programmatic use |

### 11. Validation: Broker-Side
| Aspect | Decision |
|--------|----------|
| **Authority** | Broker validates all messages |
| **Client libs** | Validate as convenience (fail-fast) |

### 12. Schema Evolution: Backward Compatible
| Aspect | Decision |
|--------|----------|
| **Default** | Backward compatible |
| **Allowed** | Add optional fields, deprecate |
| **Rejected** | Remove fields, change types |

### 13. Queue Creation: Mode-Dependent
| Aspect | Decision |
|--------|----------|
| **Permissive mode** | Queue auto-creates on first publish |
| **Strict mode** | Schema must be registered before publish |
| **Unknown queue (strict)** | Rejected (no schema = no queue) |

### 14. Failed Messages: Configurable, DLQ Default
| Aspect | Decision |
|--------|----------|
| **Default** | Move to `<queue>.dlq` after N retries |
| **Options** | `dlq` (default), `drop`, `infinite` |

### 15. Built-in Request/Reply
| Aspect | Decision |
|--------|----------|
| **Feature** | Native `CALL` command for RPC-style patterns |
| **Benefit** | "Type-safe RPC over durable queue" |

**Rationale:** Killer feature for standalone use. Gateway can use this for QWER-Q, implement manually for other broker adapters.

### 16. Message Ordering: Best-Effort FIFO
| Aspect | Decision |
|--------|----------|
| **v1** | Best-effort FIFO, no strict guarantees |
| **Future** | Ordering keys (same key → same consumer) |

### 17. Backpressure: Max Size + Reject
| Aspect | Decision |
|--------|----------|
| **Model** | Configurable max queue size |
| **On full** | Reject publish with clear error |
| **Default** | Generous limit (e.g., 1M messages or 1GB) |

### 18. Message ID: Broker Default, Client Override
| Aspect | Decision |
|--------|----------|
| **Default** | Broker generates ULID |
| **Override** | Client can provide own ID |
| **Format** | ULID (sortable, timestamp-based) |

### 19. Message Headers: Free-Form
| Aspect | Decision |
|--------|----------|
| **Format** | String→string map |
| **Broker behavior** | Pass-through, opaque |
| **Use cases** | Tracing, routing hints, context |

### 20. Idempotency: Opt-In Dedup Key
| Aspect | Decision |
|--------|----------|
| **Mechanism** | Client-provided `idempotency_key` |
| **Window** | Configurable TTL (default 5 min) |
| **No key** | No dedup overhead |

### 21. Client Libraries: Layered
| Aspect | Decision |
|--------|----------|
| **Core** | Thin wrapper — connection, protocol, basic pub/sub |
| **Batteries** | Optional — reconnect, pooling, typed wrappers, retry |
| **Why** | Gateway builds on core; power users get control |

---

## Decisions Pending

### Protocol Details (implementation phase)
- [ ] Frame format (length prefix, headers, payload)
- [ ] Command set (PUBLISH, CONSUME, ACK, etc.)
- [ ] Protocol versioning scheme

### Post-v1 Features
- [ ] Web UI dashboard
- [ ] Consumer groups
- [ ] Ordering keys (partition-like routing)
- [ ] Clustering / HA
- [ ] Stream mode (optional log semantics)

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

## Open Questions (implementation phase)

1. Frame format for custom protocol (length prefix, headers, payload structure)
2. Exact command set and opcodes
3. Client library design for Go and TypeScript
4. Embedded DB choice: BadgerDB vs bbolt vs other
