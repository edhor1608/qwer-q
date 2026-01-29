# QWER-Q

A typed, docker-first message queue.

```bash
docker run -p 9876:9876 ghcr.io/jonas/qwer-q
```

## Why?

- **Kafka is too heavy** — Complex ops, multiple components
- **NATS is too minimal** — Missing advanced guarantees
- **Typing breaks** — tRPC gives great DX for HTTP, but MQ loses type safety

QWER-Q fills the gap: simple deployment, real durability, types as a first-class feature.

## Status

Early development. See [docs/plans/](docs/plans/) for design documents.

## Goals

- Single binary, single container
- `docker run` and it works
- Typed-by-default (schema registry built-in)
- At-least-once delivery
- Sub-millisecond latency
- Durable (survives restarts)

## Non-Goals (v1)

- Exactly-once delivery
- Stream/log semantics (Kafka-style replay)
- Multi-node clustering
- Consumer groups

## License

Apache 2.0
