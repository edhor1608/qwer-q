# Contributor Operating Map

This map is for contributors and agents starting QWER-Q work. It explains what
to read first, which surfaces are core, which surfaces are preview, and how to
keep changes scoped.

## Reading Order

Read in this order before non-trivial work:

1. [CLAUDE.md](../CLAUDE.md) for project context, commands, and working rules.
2. [Product charter](plans/2026-04-13-product-charter.md) for what QWER-Q is
   trying to be.
3. [Decision log](plans/decisions-log.md) for architectural decisions.
4. [Architecture guide](architecture.md) for runtime responsibilities and
   message flow.
5. [Durable queue state contract](plans/2026-05-25-durable-queue-state-contract.md)
   before queue durability, recovery, ack/nack, DLQ, purge, or group work.
6. Relevant planning or benchmark docs under `docs/plans/` and
   `docs/benchmarks/`.
7. The Linear issue or document that defines the current slice.

## Product Center

QWER-Q's center of gravity is durable queue mode:

- publish
- consume
- ack/nack
- visibility timeout and redelivery
- DLQ workflows
- typed schema validation
- auth
- metrics, REST API, and dashboard
- simple single-binary operation

Prefer changes that make QWER-Q better at being a self-hosted typed durable
queue broker. Do not optimize for generic messaging prestige.

## Preview Areas

Stream mode and clustering are implemented but preview. Respect them when
touching shared code, but do not let them drive product direction unless a
specific issue says so.

Preview work should be explicit about whether it changes queue-mode behavior.
Queue-mode correctness wins when there is tension.

## Core Surfaces

Most work touches one or more of these surfaces:

- CLI wiring and runtime configuration
- TCP server and protocol dispatch
- protocol frame and protobuf encoding
- schema registry and schema enforcement mode
- broker publish/consume/ack/nack handlers
- queue runtime state
- BadgerDB storage and recovery
- HTTP API and dashboard
- Go and TypeScript clients
- benchmark module and benchmark documentation

Start where the behavior is owned. Move outward only when the behavior crosses a
surface.

## Default Work Shape

Keep work small and vertical:

- one externally visible behavior per issue
- one branch per issue in the Graphite stack
- one test-first tracer bullet at a time for implementation work
- no broad renames or restructures unless the issue asks for them
- no speculative abstraction for future features

For implementation work, prefer TDD:

1. write one behavior test through a stable interface
2. watch it fail for the right reason
3. make the smallest change to pass
4. run the relevant package tests
5. repeat for the next behavior
6. run the broader verification gate before committing

## Branch Knowledge

Each feature branch should capture knowledge as it is learned:

- update the relevant docs when a durable decision is made
- add a planning note under `docs/plans/` for new investigations
- use Linear Documents for larger PRDs, RFCs, ADRs, or research artifacts
- use Linear Issues for executable follow-up work

Do not leave important discoveries only in chat or commit messages.

## When To Create More Work

Create a new Linear Issue when you find concrete executable work that should not
be folded into the current slice.

Create a new Linear Document or PRD when you find a larger product or
architecture area that needs design before implementation.

Use architecture review when a change would alter a durable contract, create a
new module seam, change public protocol behavior, or reopen an existing
decision.
