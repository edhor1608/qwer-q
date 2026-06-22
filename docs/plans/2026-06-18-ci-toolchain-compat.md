# CI Toolchain Compatibility

## Problem

`main` CI failed because `go.mod` declared Go 1.26.3 while CI, Docker, and the
downloaded golangci-lint binary still targeted Go 1.24.

## What Worked

Keep the Go 1.26 module target and update the build surfaces to use the same
secure patch version:

- CI `actions/setup-go` reads the Go version from `go.mod`.
- Docker uses `golang:1.26.4-alpine`.
- CI builds the pinned golangci-lint v1.64.8 from source under Go 1.26.4.
- CI runs the pinned govulncheck v1.4.0 analyzer.

This keeps the repository on the newer standard library while preserving the
v1 golangci-lint config format. The golangci-lint v1.64.8 plus Go 1.26.4
`goinstall` pairing is known-fragile because future Go patches can outpace the
frozen v1 linter's bundled analysis dependencies; migrating the config to
golangci-lint v2 is the long-term fix.

## What Did Not Work

Only lowering the `go` directive and dependency tags made CI/Docker compatible,
but `govulncheck` then reported reachable standard-library vulnerabilities under
Go 1.24.13. That made the downgrade the wrong fix.

## Decision

Align CI and Docker with `go 1.26.4`. This is the smallest safe fix because it
addresses the broken CI boundary without moving dependencies backward or
converting the linter config to golangci-lint v2.

Keep `govulncheck` in CI with the analyzer pinned to v1.4.0 so tool changes are
deliberate. The vulnerability database is still fetched live, so the step is
non-blocking to avoid failing unrelated PRs when new advisories are published.
