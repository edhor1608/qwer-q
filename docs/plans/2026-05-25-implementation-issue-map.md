# QWER-Q Implementation Issue Map

**Date:** 2026-05-25
**Linear issue:** [EDH-364](https://linear.app/edhorsagents/issue/EDH-364/qwer-q-create-first-implementation-ready-issue-map-from-the-mental)
**Source PRD:** [PRD: Mental Project Plan for QWER-Q Work](https://linear.app/edhorsagents/document/prd-mental-project-plan-for-qwer-q-work-1f02b99ab836)

## Purpose

This is the first follow-on issue map after the initial QWER-Q PRD breakdown.
It prioritizes queue-mode durability, broker correctness, dependency hygiene,
and contributor documentation.

This map is a HITL draft. Do not create new Linear issues from the proposed
follow-ups until a human confirms the slices and ordering.

## Current Stack Slices

These slices are already represented by Linear issues and should stay in this
order when stacked:

1. **EDH-353: Durable queue state contract**
   - Type: HITL
   - Purpose: define the durable queue-mode contract and known gaps.

2. **EDH-357: Pending and acked restart recovery**
   - Type: AFK
   - Purpose: prove pending messages recover and acked messages do not recover.

3. **EDH-358: Transient failure restart recovery**
   - Type: AFK
   - Purpose: prove nacked requeue, in-flight, and visibility-timeout paths
     preserve at-least-once behavior over restart.

4. **EDH-359: Durable DLQ move, retry, and purge**
   - Type: AFK
   - Purpose: make DLQ move/retry/purge and queue purge survive restart.

5. **EDH-360: Consumer group durable versus ephemeral behavior**
   - Type: AFK
   - Purpose: clarify that queue-mode group membership and fan-out state are
     runtime delivery coordination, not durable subscriptions.

6. **EDH-354: Benchmark module boundary**
   - Type: AFK
   - Purpose: isolate benchmark-only queue client dependencies from the broker
     runtime module.

7. **EDH-361: Benchmark command ergonomics**
   - Type: AFK
   - Purpose: restore obvious benchmark command paths after module isolation.

8. **EDH-355: Broker flow architecture guide**
   - Type: AFK
   - Purpose: document runtime responsibilities and queue-mode message flow.

9. **EDH-362: Architecture change paths**
   - Type: AFK
   - Purpose: document where to start for protocol, broker, storage, API,
     client, and benchmark changes.

10. **EDH-356: Contributor operating map**
    - Type: AFK
    - Purpose: document reading order, core surfaces, preview areas, and scoped
      working rules.

11. **EDH-363: High-risk behavior testing strategy**
    - Type: AFK
    - Purpose: document which behavior needs integration-style coverage.

12. **EDH-364: Implementation issue map**
    - Type: HITL
    - Purpose: review this map and decide which follow-up issues to create.

13. **EDH-365: Linear project navigation**
    - Type: AFK
    - Purpose: update Linear project navigation after the stack and follow-up
      decisions settle.

## Proposed Follow-Up Issues

These are concrete executable slices found while documenting and hardening the
durable queue state contract. They should be created with `to-issues` after
human review.

1. **Make publish durable before runtime delivery**
   - Type: HITL, then AFK after decision
   - Why: publish currently mutates runtime queue state before storage save
     succeeds. The desired contract is clear, but the exact ordering trade-off
     affects latency and delivery behavior.
   - Shape: decide whether publish should save before enqueue or use another
     small atomicity pattern; then test storage failure behavior through the
     broker publish path.

2. **Surface queue metadata persistence failures**
   - Type: AFK
   - Why: queue config save errors are currently ignored when queues are
     created, which can make recovery depend on a best-effort side effect.
   - Shape: force queue metadata persistence failure in a narrow test and make
     the public operation fail or record a documented degraded state.

3. **Surface ack storage deletion failures**
   - Type: AFK
   - Why: ack can remove runtime in-flight state while storage delete errors are
     ignored, allowing completed work to reappear after restart.
   - Shape: test an ack path with failing storage deletion and define whether
     ack should fail, retry, or leave the message in a recoverable state.

4. **Make retry attempt state durable or explicitly reset-on-restart**
   - Type: HITL
   - Why: nacked requeue preserves at-least-once delivery, but attempt counts
     and visibility timestamps are not durably updated. This affects retry-limit
     semantics across restart.
   - Shape: decide the product contract first; then add tests for retry limit
     behavior across restart.

5. **Durably delete terminal non-DLQ nack outcomes**
   - Type: AFK
   - Why: drop-policy terminal outcomes can leave stored messages behind.
   - Shape: configure drop policy, force terminal nack, restart, and assert the
     message does not recover.

6. **Document or redesign queue-mode consumer group fan-out semantics**
   - Type: HITL
   - Why: queue-mode groups are runtime coordination, not durable independent
     subscriptions. That is now documented, but it may not match user
     expectations once groups are productized.
   - Shape: decide whether current group semantics are sufficient for the
     product promise or need a separate PRD for durable subscriptions.

7. **Add storage adapter tests for queue-wide deletion**
   - Type: AFK
   - Why: queue-wide deletion is now part of the storage interface because purge
     and DLQ purge rely on it.
   - Shape: add focused BadgerDB tests that delete one queue without deleting
     similarly prefixed or unrelated queues.

8. **Add protocol/client coverage for admin synchronization needs**
   - Type: AFK
   - Why: ack/nack have no response frame, so tests currently use admin
     roundtrips or connection close for synchronization.
   - Shape: decide whether this remains a test technique or whether the protocol
     should expose ack/nack responses in a future protocol PRD.

## Priority Recommendation

1. Publish durability before runtime delivery.
2. Ack storage deletion failure behavior.
3. Terminal non-DLQ nack durable deletion.
4. Queue metadata persistence failure behavior.
5. Retry attempt durability decision.
6. Queue-wide deletion storage adapter tests.
7. Consumer group product semantics decision.
8. Ack/nack response protocol consideration.

This order keeps the focus on queue-mode correctness before ergonomics or
larger semantic expansion.

## Human Review Questions

1. Should publish save-before-enqueue be the default durability contract even if
   it changes hot-path latency?
2. Should ack fail when storage delete fails, or should it keep runtime ack
   success and rely on later cleanup?
3. Are queue-mode consumer groups intentionally runtime-only for the next
   release, or do they need a durable subscription PRD?
4. Should ack/nack response frames be considered now, or deferred until a
   protocol compatibility PRD?
