# Release Readiness: v0.2.0

Last updated: 2026-02-18

## Scope of This Baseline

This baseline is for the current shipped feature set on `main`:
- queue core, schema modes, auth, REST/dashboard, TS client
- stream mode (preview)
- clustering (preview)

## Readiness Gates

## 1) Behavior/Docs Alignment
- [x] Shipped behavior documented as-is (no “planned” language for already merged features)
- [x] Type-safety position explicit: runtime enforcement in broker; compile-time gateway flow separate
- [x] Stable vs preview labels explicit

## 2) Reliability Baseline
- [x] Full `go test ./...` green
- [x] Cluster stream replication path covered by tests
- [x] FSM restore state-replacement behavior covered by tests
- [ ] Extended soak/chaos runs for clustering and stream preview

## 3) Benchmark Claim Hygiene
- [x] Claims policy defined (`docs/benchmarks/CLAIMS-POLICY.md`)
- [x] Comparator policy set (QWER-Q, NATS, RabbitMQ, Kafka for primary tables)
- [ ] Fresh publishable benchmark matrix regenerated under policy

## 4) Security and Ops
- [x] Token auth available
- [ ] mTLS implemented
- [ ] Production hardening guide for clustered preview mode

## 5) Packaging/Distribution
- [ ] Tag `v0.2.0`
- [ ] Publish container image(s)
- [ ] Publish TypeScript client package
- [ ] Release notes with stable vs preview framing

## Open Risks (Must Be Visible in Release Notes)

1. Stream mode is preview and not yet battle-tested in long-running production conditions.
2. Clustering is preview; operational guidance and failure-mode hardening are still in progress.
3. mTLS is not implemented; token auth + network controls are required.
4. Benchmark comparators may have adapter-specific limits; publish only policy-compliant numbers.

## Recommended Next Steps

1. Merge docs truth-sync and reliability hardening PRs.
2. Run policy-compliant benchmark pass and publish a dated baseline report.
3. Tag and ship `v0.2.0` with explicit preview labels and open-risk section.
