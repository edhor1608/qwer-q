# Durable Queue State Contract

**Date:** 2026-05-25
**Linear issue:** [EDH-353](https://linear.app/edhorsagents/issue/EDH-353/qwer-q-document-the-durable-queue-state-contract)
**Source PRD:** [PRD: Durable Queue State Consistency](https://linear.app/edhorsagents/document/prd-durable-queue-state-consistency-1f4fc17c7a54)

## Context

QWER-Q is a queue-first broker. Queue mode is the product center of gravity:
messages are published to a named queue, delivered to consumers, and deleted
after ack. Stream mode and clustering exist as preview capabilities and do not
define this contract.

This contract makes the queue-mode durability rules explicit so later work can
test and harden them without re-litigating the product model.

Relevant existing decisions:

- [DEC-001: Queue-First Mental Model](decisions-log.md#dec-001-queue-first-mental-model)
- [DEC-002: Embedded Durable Storage](decisions-log.md#dec-002-embedded-durable-storage)
- [DEC-005: Visibility Timeout for Redelivery](decisions-log.md#dec-005-visibility-timeout-for-redelivery)
- [QWER-Q Product Charter](2026-04-13-product-charter.md)

## Durable State Model

Queue mode has two state layers:

- **Durable message state:** messages and queue metadata persisted by the
  storage backend.
- **Runtime delivery state:** connected consumers, consumer group members,
  in-flight maps, visibility timers, ordering-key assignments, and delivery
  channels held in memory.

Only durable message state is expected to survive broker restart. Runtime
delivery state is rebuilt from connected clients and recovered messages.

## Message States

### Pending

A pending message is accepted for a queue and not currently in-flight.

Contract:

- A successfully published message must be recoverable after broker restart.
- With durable storage enabled, publish saves the message before exposing it to
  runtime delivery.
- If the durable save fails, publish fails and the message is not visible to
  consumers.
- If runtime enqueue fails after a durable save, the durable write is rolled
  back and publish fails.
- After restart, recovered queue-mode messages are eligible for delivery.
- Queue mode does not provide replay after ack.

Current implementation notes:

- Queue-mode publish fails when auto-created queue metadata cannot be persisted.

### In-flight

An in-flight message has been delivered to a consumer but not acked or nacked.

Contract:

- In-flight delivery state is runtime state, not durable state.
- A broker restart must not lose an unacked message.
- After restart, an unacked message may be delivered again as pending work.
- This is at-least-once delivery; duplicates are allowed.

Current implementation notes:

- Consume moves the message to the in-memory in-flight map. The stored message is
  not deleted until ack, so restart recovery reintroduces it.
- Visibility extension is not persisted. On restart, the message becomes
  recoverable work instead of preserving the extended invisible-until timestamp.

### Acked

An acked message has been successfully processed by a consumer.

Contract:

- Ack removes the message from durable storage.
- Ack removes the message from runtime state only after durable deletion
  succeeds.
- If durable deletion fails, ack fails and the message remains in-flight for the
  current broker process.
- An acked message must not reappear after restart.

Current implementation notes:

- Ack returns storage deletion errors to the server boundary.

### Nacked With Requeue

A nacked message with requeue enabled represents transient failure.

Contract:

- The message remains part of durable queue state.
- The message is eligible for redelivery subject to retry policy.
- Restart must not lose the message.

Current implementation notes:

- Queue-mode nack requeue updates runtime delivery state and persists the
  updated attempt state for restart recovery.
- If retry-state persistence fails during nack requeue, nack fails and the
  message remains in-flight for the current broker process.
- Visibility-timeout retry attempt durability is tracked separately in EDH-380.
- Consumer-group retry attempt durability is tracked separately in EDH-381.

### Nacked Without Requeue

A nacked message without requeue represents explicit rejection.

Contract:

- With DLQ failure policy, the message moves durably to the DLQ.
- With drop failure policy, the message is durably removed.
- If durable deletion for a drop-policy terminal nack fails, nack fails and the
  message remains in-flight for the current broker process.
- With infinite retry policy, explicit reject semantics must be documented before
  further behavior changes.

Current implementation notes:

- DLQ movement deletes the original queue storage entry and saves the message
  under the DLQ name.
- Drop-policy terminal nack outcomes delete the original queue storage entry.
- Non-DLQ terminal outcomes other than drop-policy still need explicit product
  semantics before further behavior changes.

### Dead Letter Queue

A DLQ message is failed work retained for operator inspection or retry.

Contract:

- Moving a message to a DLQ must delete durable state from the original queue.
- Moving a message to a DLQ must save durable state under the DLQ queue name.
- DLQ messages must remain inspectable after restart until retried, purged, or
  otherwise removed.

Current implementation notes:

- Broker-level DLQ movement during nack persists the move.
- HTTP DLQ retry currently moves messages in memory and purges the in-memory DLQ
  only. It does not update storage today.
- HTTP DLQ purge currently purges in-memory state only. It does not delete stored
  DLQ messages today.

### Purged

A purged queue or DLQ has been administratively cleared.

Contract:

- Purge must remove pending and in-flight runtime state.
- Purge must remove recoverable durable message state.
- Purged messages must not reappear after restart.

Current implementation notes:

- Queue purge and DLQ purge currently clear in-memory queue state only.
- The storage interface has no queue-wide delete operation today, so purge cannot
  yet satisfy the durable contract.

## Visibility Timeout

Visibility timeout is the recovery mechanism for consumers that receive work but
do not ack it.

Contract:

- While the broker keeps running, expired in-flight messages are requeued for
  redelivery.
- On broker restart, in-flight state is lost but durable message state remains;
  unacked messages become eligible for delivery again.
- The contract is at-least-once, not exactly-once.

Current implementation notes:

- Runtime reaping periodically moves expired in-flight messages back to pending.
- Visibility timestamps are not persisted after delivery or extension.

## Consumer Groups

Consumer groups are runtime delivery coordination, not durable subscriptions.
This is an explicit product decision in DEC-035.

Contract:

- Connected group members are ephemeral.
- Group membership, heartbeats, member assignment, and per-member in-flight state
  do not survive broker restart.
- Queue-mode durable storage persists messages, not named group subscriptions.
- A restart may require consumers to reconnect and rejoin groups.
- Re-created groups do not receive historical group fan-out from before restart;
  the underlying queue message remains queue work unless it was acked.

Current implementation notes:

- Consumer group state lives in memory.
- Group delivery fan-out is not persisted as separate durable group work.
- Group ack currently deletes the underlying stored queue message, so queue-mode
  groups should not be treated as durable independent subscriptions.
- Durable independent subscriptions require a separate product PRD before
  implementation.

## Ordering Keys

Ordering-key assignment is runtime delivery state.

Contract:

- Ordering keys influence delivery while consumers are connected.
- Ordering-key to consumer assignment does not survive restart.
- A restart may assign a key to a different consumer after reconnect.

## Queue Recovery

Broker startup recovery loads queue metadata and messages from storage.

Contract:

- Queue-mode recovery reconstructs queues from stored queue configs.
- Stored queue-mode messages are loaded as pending work.
- Recovered messages must remain subject to normal queue delivery, ack, nack,
  DLQ, and purge behavior.

Current implementation notes:

- Recovery only loads messages for queues present in stored queue metadata.
- Queue-mode publish fails when auto-created queue metadata cannot be persisted.
- New queue metadata is persisted with the current default queue config.
- Stream publish fails when auto-created stream queue metadata cannot be
  persisted.

## Known Gaps For Follow-Up Issues

These gaps are intentionally captured here so the follow-up TDD issues can turn
them into failing behavior tests before implementation changes:

1. Visibility-timeout retry attempt durability is not decided.
2. Queue config zero-value compatibility is not decided.
3. Queue purge and DLQ purge do not delete durable message state.
4. DLQ retry does not durably move messages from DLQ storage back to the
   original queue.
5. Queue-mode consumer groups are runtime coordination only by decision
   (DEC-035); durable independent group subscriptions are not implemented.

## Test Contract

Follow-up implementation issues should test behavior through stable broker,
client, storage, or HTTP interfaces. Tests should not inspect private in-memory
maps as the primary proof.

Required behavior coverage:

- pending message survives restart
- acked message does not survive restart
- delivered but unacked message is recoverable after restart
- nacked requeue message is recoverable after restart
- DLQ move survives restart
- DLQ retry survives restart
- queue purge and DLQ purge survive restart
- consumer group durable/ephemeral behavior is explicit and tested or marked out
  of scope

## Out Of Scope

- Exactly-once delivery.
- Connected consumers as durable identities.
- Consumer group membership as durable state.
- Queue-mode group subscriptions as durable ledgers.
- Stream-mode replay and offsets.
- Raft clustering semantics.
- Replacing BadgerDB.
