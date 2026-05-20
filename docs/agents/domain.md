# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Layout

This is a single-context repo.

Before exploring, read:

- `CLAUDE.md` for project context, terminology, current state, and development commands.
- `docs/plans/decisions-log.md` for architectural decisions and rationale.
- Relevant files under `docs/plans/` for planning history, research, and optimization loops.

If a future branch introduces `CONTEXT.md`, `CONTEXT-MAP.md`, or `docs/adr/`, prefer those files for their specific scope and keep this file updated.

## Use the project's vocabulary

When output names a domain concept in an issue title, refactor proposal, hypothesis, or test name, use the terms from `CLAUDE.md` and the decision log. Do not drift to synonyms without a reason.

If the concept you need is not documented yet, note the gap instead of inventing durable terminology.

## Flag decision conflicts

If output contradicts an existing decision in `docs/plans/decisions-log.md`, surface it explicitly rather than silently overriding it.
