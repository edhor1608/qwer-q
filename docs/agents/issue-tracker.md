# Issue tracker: Linear

Issues and implementation work for this repo live in Linear. Use Linear for all issue-tracker operations.

## Conventions

- **Initiatives**: Long-running outcomes or business directions.
- **Projects**: Work containers for features, product areas, or themed efforts.
- **Documents**: Long-lived knowledge such as PRDs, RFCs, ADRs, research, decision logs, and project planning notes. Attach Documents to the relevant Linear project unless the user asks for a different location.
- **Issues**: Executable work. Create issues only for work that someone or an agent can implement, verify, or decide.
- **Subissues**: Vertical slices or child work under a parent Linear issue. Use `parentId` when breaking an existing issue into slices.
- **Labels**: Use domain labels for product area and the mapped triage labels for workflow state. See `triage-labels.md`.

Keep Linear project descriptions short: goal, scope, and links to important Documents. Do not use the project description as a replacement for a PRD, RFC, ADR, or research Document.

## Common operations

- **Create or update a PRD, RFC, ADR, or research artifact**: create or update a Linear Document on the relevant project.
- **Read source material**: fetch the referenced Linear Document, Project, or Issue, including comments when the source is an issue.
- **Create implementation work**: create Linear Issues in the relevant project.
- **Create vertical slices from an existing issue**: create Linear subissues with `parentId`.
- **Represent dependencies**: use Linear issue relations and mention blockers in the issue body for readability.
- **Comment on work**: use Linear comments on issues. Do not put implementation discussion into Documents unless it changes the durable plan.

## When a skill says "publish to the issue tracker"

For PRDs, RFCs, ADRs, and research, create or update a Linear Document on the relevant project.

For executable work, create Linear Issues or Subissues in the relevant project.

## When a skill says "fetch the relevant ticket"

Fetch the Linear Issue if the reference points to executable work. Fetch the Linear Document or Project if the reference points to planning or durable knowledge.
