# AGENTS.md

## Cursor Cloud specific instructions

### Overview

QWER-Q is a single-binary Go message queue broker with embedded BadgerDB storage. No external databases or services are needed for development.

### Services

| Service | Port | Purpose |
|---------|------|---------|
| Broker (TCP) | 9876 | Binary protobuf wire protocol for publish/consume |
| HTTP (metrics/API/dashboard) | 9877 | REST API, Prometheus metrics, embedded web dashboard |

### Key commands

Standard build/test/lint commands are in the `Makefile`. Quick reference:

- **Build**: `make build` (outputs `bin/qwer-q`)
- **Test**: `make test` or `go test ./...` (CI uses `-race -coverprofile`)
- **Lint**: `make lint` (requires `golangci-lint` v1.x; the `.golangci.yml` config uses v1 format)
- **Run**: `./bin/qwer-q serve --data-dir ./data`

### Caveats

- **golangci-lint version**: The `.golangci.yml` config is written for golangci-lint v1.x. Do not install v2.x (it requires a different config format with a `version` field). Install v1.64.8 specifically: `curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sudo sh -s -- -b /usr/local/bin v1.64.8`.
- **No external dependencies**: The broker uses embedded BadgerDB for persistence. No Postgres, Redis, or Docker is required to build, test, or run locally.
- **Protobuf codegen is pre-committed**: The generated Go code (`internal/protocol/messages.pb.go`) is checked in. You do not need `protoc` unless modifying `proto/qwerq.proto`.
- **Data directory**: When running the broker locally, use `--data-dir ./data` to keep persistence data in the workspace. Clean with `rm -rf ./data`.
- **Docker/bench targets**: The `make docker-up`, `make bench`, and `make bench-all` targets require Docker, which is not needed for normal development.
