# QWER-Q Current State and Forward Plan (v0.2.0)

> Last updated: 2026-02-18

This document is now a truth-synced status baseline, not a speculative pre-v2 roadmap.

## Problem

- Planning docs had drifted from what actually shipped on `main`.
- Type-safety messaging needed a precise runtime-vs-compile-time split.
- Product framing needed clear stable versus preview boundaries.

## What We Tried

- Reconciled roadmap claims with merged PR output and current behavior.
- Consolidated "next work" into reliability, docs discipline, and release hygiene.
- Removed speculative roadmap framing for already-completed milestones.

## Research Findings

- Most roadmap streams are complete; remaining risk is reliability confidence and docs credibility.
- Stream mode and clustering are feature-complete but still need hardening evidence.
- Permissive and strict schema modes must both be documented as first-class behavior.

## Design Decisions

- Treat this file as a baseline status document, not a forward-feature promise list.
- Keep type-safety position canonical: runtime enforcement in broker, compile-time flow separate.
- Prioritize hardening and release quality before net-new feature expansion.

## Lessons Learned

- Narrative drift can become a larger risk than missing features.
- Mature roadmap docs should distinguish implementation status from confidence level.
- Claims discipline improves release readiness as much as code changes.

## 1. What Is Already Shipped

All originally planned streams in the v1.1 → v2.0 roadmap have landed on `main`:

- Auth system (token-based)
- Write batching
- REST admin API
- Embedded dashboard SPA
- TypeScript client
- Documentation site
- Ordering keys
- Consumer groups
- Stream mode
- Raft-based clustering

## 2. Product Scope by Maturity

### Stable

- Queue mode: publish/consume/ack/nack with at-least-once delivery
- Schema registry + runtime schema validation
- Dual schema modes: `permissive` and `strict`
- Token auth
- REST API and dashboard
- Metrics and core operations

### Preview

- Stream mode
- Clustering (Raft)

Preview means implemented and tested, but still in hardening phase before broad production claims.

## 3. Type-Safety Position (Canonical)

Current state:
- Broker enforces runtime schema validity at publish-time when schema is present.
- `strict` mode requires schema registration before publish.
- `permissive` mode allows schema-less queues.

Not yet in this repo:
- End-to-end compile-time codegen workflow from registry schemas to client types.
- Companion gateway project for framework-level typed action orchestration.

## 4. What Is Still Open

### P0: Reliability Hardening

- Strengthen preview features with targeted failure tests and soak validation.
- Close known reliability debt from review feedback (snapshot/restore safety, test flake resistance, shutdown/error handling paths).

### P1: Docs and Claims Discipline

- Keep docs/README synchronized to shipped behavior only.
- Separate stable vs preview claims everywhere.
- Keep benchmark claims conservative and reproducible.

### P2: Security Completion

- Add mTLS on top of token auth.

### P3: Packaging and Release Hygiene

- Cut and tag release (`v0.2.0` baseline).
- Publish TypeScript package and container artifacts.

## 5. Non-Goals (Current)

- Exactly-once semantics
- Managed cloud offering
- Mixing app-framework gateway concerns into broker core

## 6. Working Rule Going Forward

Before new feature work:
1. Keep docs, decisions, and behavior aligned.
2. Harden preview features until they can be promoted.
3. Only then expand feature surface.
