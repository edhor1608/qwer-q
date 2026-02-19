# Benchmark Claims Policy

Last updated: 2026-02-18

## Purpose

This policy defines what benchmark numbers are allowed in release-facing docs,
PRs, and announcements.

## Claim Levels

### Level A: Exploratory (internal only)

- Early runs, adapter debugging, partial suites
- Can be kept in dated benchmark notes
- Must not be used as external product claims

### Level B: Publishable (external-safe)

A number is publishable only if all conditions below are met:

1. Exact environment documented (CPU, memory, container limits, host class)
2. Queue durability mode documented for every system
3. Fresh-state run (`docker compose down -v` before run)
4. Scenario and command documented
5. At least 3 runs with median reported
6. Comparator adapter shows valid end-to-end behavior
   (published/consumed/errors sanity)

## Required Metadata for Any Publishable Table

- Date (UTC)
- Git commit SHA
- Scenario config (duration, payload size, producer/consumer counts)
- Resource limits
- Comparator versions
- Known caveats

## Comparator Set for Primary Tables

Primary comparison tables should use:

- QWER-Q
- NATS
- RabbitMQ
- Kafka

Additional systems (Redis, Pulsar, Redpanda, etc.) can be included in
appendices.

## Comparator Failure Handling

Comparator failures do **not** block release by default.

If a comparator run is invalid:

- Exclude it from the primary table
- Add it to an appendix with explicit failure reason
- Do not infer relative performance from invalid runs

## Claim Guardrails

Do not publish:

- Absolute “X is Yx faster” claims from exploratory runs
- Cross-system claims without durability/consistency context
- Numbers from runs with known adapter correctness issues

Prefer:

- Scenario-scoped statements
- Conservative ranges over single-point claims
- Explicit stable vs preview feature context
