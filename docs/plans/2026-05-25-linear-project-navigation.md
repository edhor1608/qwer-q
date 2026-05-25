# QWER-Q Linear Project Navigation

**Date:** 2026-05-25
**Linear issue:** [EDH-365](https://linear.app/edhorsagents/issue/EDH-365/qwer-q-update-final-linear-project-navigation-after-prdsissues-are)
**Project:** [QWER-Q](https://linear.app/edhorsagents/project/qwer-q-4a59537aadbd)

## Source Documents

- [PRD: Durable Queue State Consistency](https://linear.app/edhorsagents/document/prd-durable-queue-state-consistency-1f4fc17c7a54)
- [PRD: Isolate Benchmark Dependencies From Broker Runtime](https://linear.app/edhorsagents/document/prd-isolate-benchmark-dependencies-from-broker-runtime-eb59442cf295)
- [PRD: Broker Flow Architecture Guide](https://linear.app/edhorsagents/document/prd-broker-flow-architecture-guide-93df8ee0360d)
- [PRD: Mental Project Plan for QWER-Q Work](https://linear.app/edhorsagents/document/prd-mental-project-plan-for-qwer-q-work-1f02b99ab836)

## Stack Order

1. EDH-353: Durable queue state contract
2. EDH-357: Pending and acked restart recovery
3. EDH-358: Transient failure restart recovery
4. EDH-359: Durable DLQ move, retry, and purge
5. EDH-360: Consumer group durable versus ephemeral behavior
6. EDH-354: Benchmark module boundary
7. EDH-361: Benchmark command ergonomics
8. EDH-355: Broker flow architecture guide
9. EDH-362: Architecture change paths
10. EDH-356: Contributor operating map
11. EDH-363: High-risk behavior testing strategy
12. EDH-364: Implementation issue map
13. EDH-365: Linear project navigation

This order keeps queue-mode correctness first, then benchmark dependency
hygiene, then contributor-facing documentation and follow-up planning.

## Repository Anchors

- Durable queue contract:
  `docs/plans/2026-05-25-durable-queue-state-contract.md`
- Broker architecture guide:
  `docs/architecture.md`
- Contributor operating map:
  `docs/contributor-operating-map.md`
- Follow-up issue map:
  `docs/plans/2026-05-25-implementation-issue-map.md`

## Current Review Boundary

The first stack is ready for code review after test verification and Graphite
submission.

The proposed follow-up issues in the implementation issue map are not yet
created in Linear. They need human review because several are product-contract
decisions, not only implementation tasks:

1. publish save-before-enqueue behavior,
2. ack behavior when durable deletion fails,
3. retry attempt durability across restart,
4. queue-mode consumer group semantics,
5. ack/nack protocol response ergonomics.

After those decisions are confirmed, use `to-issues` to publish the approved
follow-up slices in dependency order.
