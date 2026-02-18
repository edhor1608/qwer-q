# QWER-Q Developer Experience Audit

**Date:** 2026-02-05
**Scope:** CLI (`cmd/qwer-q/`), Go client library (`pkg/client/`), error handling, output formatting, developer workflow

---

## Executive Summary

QWER-Q's CLI is functional but minimal. It covers the basics (serve, queue list, schema register/list) but is missing several commands that the protocol actually supports (publish, consume, ack, nack, extend-visibility, call). The Go client library works but lacks production-readiness features. Error messages are generic and unhelpful. There is no shell completion, no config file, no environment variable support, no `--json` output mode, and no publish/consume CLI commands — meaning the only way to interact with the queue is by writing Go code.

**Overall Grade: D+** — It works, but the developer experience is an afterthought.

---

## Grades by Area

| Area | Grade | Notes |
|------|-------|-------|
| CLI Structure | C | Logical hierarchy, but missing critical commands |
| Flag Naming | B | Consistent cobra-style, good short flags |
| Help Text | D | Minimal descriptions, zero examples |
| Error Messages | D | Generic, not actionable, no context |
| Output Formatting | C- | Tabwriter for lists, but no --json, --quiet, --verbose |
| Developer Workflow | F | No shell completion, config file, env vars, or .env |
| Go Client Library | D+ | Works but no context support, no reconnection, no timeouts |
| Discoverability | F | No publish/consume commands — the core use case is hidden |

---

## Specific Issues Found

### 1. Missing CLI Commands (P0 — Critical)

The protocol supports publish, consume, ack, nack, extend-visibility, and call operations (`internal/protocol/opcodes.go:10-19`), but the CLI only exposes:
- `serve` — start broker
- `queue list` — list queues
- `schema register` — register schema
- `schema list` — list schemas

**Missing commands:**
- `qwer-q publish <queue> <message>` — publish a message
- `qwer-q consume <queue>` — consume messages (stream to stdout)
- `qwer-q queue create <name>` — explicitly create a queue
- `qwer-q queue delete <name>` — delete a queue
- `qwer-q queue purge <name>` — purge all messages
- `qwer-q queue inspect <name>` — detailed queue stats

**Impact:** A developer cannot test the queue without writing Go code. This is a major barrier to adoption. Every competing tool (RabbitMQ, NATS, Redis) has publish/consume CLI commands.

### 2. No `--json` Output Mode (P0 — Critical)

**File:** All command files in `cmd/qwer-q/`

No command supports `--json` for machine-readable output. This blocks:
- Scripting and automation
- CI/CD integration
- Piping to `jq` for filtering
- Monitoring scripts

**Before (current):**
```
NAME      MESSAGES  IN-FLIGHT
orders    42        3
events    100       0
```

**After (with --json):**
```json
[
  {"name": "orders", "messages": 42, "in_flight": 3},
  {"name": "events", "messages": 100, "in_flight": 0}
]
```

### 3. Error Messages Are Not Actionable (P0 — Critical)

**File:** `cmd/qwer-q/queue.go:33`, `cmd/qwer-q/schema.go:55`

Errors just wrap the Go error with no user guidance.

**Before:**
```
Error: failed to connect: dial tcp localhost:9876: connection refused
```

**After:**
```
Error: could not connect to broker at localhost:9876 — connection refused

  Is the broker running? Start it with:
    qwer-q serve

  Or specify a different broker address:
    qwer-q queue list --broker <host:port>

  Set QWER_BROKER to avoid repeating the address:
    export QWER_BROKER=myhost:9876
```

**Before:**
```
Error: registration failed: schema validation failed: ...
```

**After:**
```
Error: schema registration failed

  The proto file could not be validated:
    <specific protoc error>

  Make sure:
    - protoc is installed (brew install protobuf)
    - The proto file is valid
    - The message type exists in the proto file
```

### 4. No Environment Variable Support (P1 — High)

**File:** `cmd/qwer-q/main.go:21`

The `--broker` flag defaults to `localhost:9876` with no env var override. Every command requires `--broker` if not using default.

**Expected:** `QWER_BROKER` environment variable as fallback.

Other useful env vars:
- `QWER_BROKER` — broker address
- `QWER_DATA_DIR` — data directory for serve
- `QWER_MAX_MESSAGE_SIZE` — max message size
- `QWER_LOG_FORMAT` — json/text log format

Cobra supports `viper` binding for env vars natively.

### 5. No Shell Completion (P1 — High)

No `qwer-q completion` command. Cobra provides this for free with `rootCmd.GenBashCompletion()`, `GenZshCompletion()`, `GenFishCompletion()`, and `GenPowerShellCompletion()`.

Every modern CLI (gh, fly, docker, kubectl) provides this. It takes ~10 lines of code.

### 6. No Config File Support (P1 — High)

No `~/.config/qwer-q/config.yaml` or `qwer-q.yaml`. Developers must specify `--broker` on every invocation.

**Expected config file:**
```yaml
broker: myhost:9876
default_queue: orders
output: json
```

### 7. Help Text Is Minimal (P1 — High)

**File:** `cmd/qwer-q/main.go:14-16`

```go
Short: "QWER-Q message queue",
Long:  "A lightweight protobuf-first message queue",
```

No examples. No getting-started guidance. Compare with `gh`:
```
Work seamlessly with GitHub from the command line.

USAGE
  gh <command> <subcommand> [flags]

CORE COMMANDS
  ...

EXAMPLES
  $ gh issue create
  $ gh repo clone cli/cli
  $ gh pr checkout 321
```

**Every subcommand should have an `Example` field.**

### 8. Banner Output on Serve (P2 — Medium)

**File:** `cmd/qwer-q/serve.go:91-98`

```go
func printBanner(brokerAddr, metricsAddr, ver string) {
    fmt.Printf(`
QWER-Q Message Queue v%s
Listening on %s (broker), %s (metrics)
Warning: Running without authentication - not for production
`, ver, brokerAddr, metricsAddr)
}
```

Issues:
- Leading blank line in output
- "Warning" line is always shown, even when auth is eventually added
- No structured log format option — banner is unstructured text while broker logs are JSON (`internal/broker/logging.go:11`)
- Should show data directory, memory limit, sync interval for debuggability

### 9. `--data-dir` Defaults to `/data` (P1 — High)

**File:** `cmd/qwer-q/serve.go:26`

```go
serveCmd.Flags().String("data-dir", "/data", "data directory for message persistence")
```

`/data` is a Docker convention. Running locally (`qwer-q serve`) will try to write to `/data` which requires root on macOS/Linux. Should default to `./data` or `~/.local/share/qwer-q/` for local development, with `/data` only as the Docker default (set in Dockerfile CMD or ENTRYPOINT).

### 10. Go Client Library Issues (P1 — High)

**File:** `pkg/client/client.go`

| Issue | Line | Description |
|-------|------|-------------|
| No `context.Context` support | All methods | No way to cancel or set timeouts on operations |
| No connection timeouts | `client.go:17` | `net.Dial` has no timeout — will hang indefinitely on unreachable host |
| No reconnection logic | — | Connection drops = client is dead, must create new one |
| No `DialWithContext` | `client.go:16` | Cannot cancel connection attempt |
| Swallowed marshal errors | `client.go:36,62,89,127` | `proto.Marshal` errors are silently ignored with `_` |
| No keepalive / heartbeat | — | Stale connections are not detected |
| No connection pooling | — | Single TCP connection, no concurrency safety |
| `BrokerError.Code` is opaque | `client.go:112-118` | Error codes (1-8) have no documentation or constants |
| No `Nack` method | — | Protocol supports it, client doesn't expose it |
| No `ExtendVisibility` method | — | Protocol supports it, client doesn't expose it |
| No `Call` method | — | Protocol supports it, client doesn't expose it |

### 11. `protoc` as Hidden Dependency (P2 — Medium)

**File:** `cmd/qwer-q/schema.go:107`

`schema register` silently requires `protoc` installed on the system. If missing, the error is:
```
Error: failed to compile proto: protoc failed: exec: "protoc": executable file not found in $PATH
```

Should check for `protoc` upfront and provide installation instructions.

### 12. Version Command Output (P2 — Low)

**File:** `cmd/qwer-q/main.go:20`

```go
rootCmd.SetVersionTemplate("qwer-q version {{.Version}}\n")
```

Missing build metadata. Best practice includes:
```
qwer-q version 0.1.0 (commit abc1234, built 2026-02-05)
```

Set via `-ldflags` at build time.

---

## Comparison vs Best-in-Class CLIs

| Feature | gh | fly | docker | kubectl | **qwer-q** |
|---------|-----|-----|--------|---------|-------------|
| Shell completion | Yes | Yes | Yes | Yes | **No** |
| `--json` output | Yes | Yes | Yes (--format) | Yes (-o json) | **No** |
| Config file | `~/.config/gh/` | `fly.toml` | `daemon.json` | `kubeconfig` | **No** |
| Env var support | `GH_TOKEN`, etc. | `FLY_API_TOKEN` | `DOCKER_HOST` | `KUBECONFIG` | **No** |
| Rich error messages | Yes | Yes | Yes | Yes | **No** |
| Examples in help | Yes | Yes | Yes | Yes | **No** |
| Interactive prompts | Yes | Yes | No | No | **No** |
| `--quiet` mode | Yes | Yes | Yes | No | **No** |
| `--verbose`/`--debug` | Yes | Yes | Yes | `-v=N` | **No** |
| Color output | Yes | Yes | Yes | Yes | **No** |
| Alias support | Yes | No | No | Yes (kubectl) | **No** |
| Version with build info | Yes | Yes | Yes | Yes | **Partial** |
| Publish from CLI | N/A | N/A | N/A | N/A | **No** |
| Consume from CLI | N/A | N/A | N/A | N/A | **No** |

---

## Prioritized Improvement List

### P0 — Must Fix (Blocks Adoption)

1. **Add `publish` and `consume` CLI commands** — This is the single most important improvement. Developers need to test the queue without writing code.
2. **Add `--json` output flag** — Required for scripting, CI/CD, and monitoring.
3. **Fix error messages** — Make every error actionable with guidance on how to fix it.
4. **Fix `--data-dir` default** — `/data` breaks local development.

### P1 — Should Fix (Improves DX Significantly)

5. **Add environment variable support** — `QWER_BROKER` at minimum.
6. **Add shell completion** — Free from Cobra, ~10 lines.
7. **Add config file support** — `~/.config/qwer-q/config.yaml` via Viper.
8. **Improve help text** — Add examples to every command.
9. **Fix Go client library** — Add context support, timeouts, missing methods.
10. **Expose missing client methods** — `Nack`, `ExtendVisibility`, `Call`.

### P2 — Nice to Have (Polish)

11. **Add `--quiet` and `--verbose` flags** — For scripting and debugging.
12. **Add color-coded output** — Errors in red, warnings in yellow.
13. **Check for `protoc` before schema register** — Friendly error.
14. **Add build metadata to version** — Commit hash, build date.
15. **Fix serve banner** — Remove leading newline, show config summary.
16. **Add `queue create`, `queue delete`, `queue purge` commands** — Complete admin story.

---

## Recommended CLI Structure

```
qwer-q
  serve                          # Start the broker
  publish <queue> <message>      # Publish a message (stdin or arg)
  consume <queue>                # Consume messages (stream to stdout)
  queue
    list                         # List all queues
    create <name>                # Create a queue
    delete <name>                # Delete a queue
    purge <name>                 # Purge all messages
    inspect <name>               # Detailed stats
  schema
    register -q <queue> -p <file> -m <type>
    list                         # List schemas
    get <queue>                  # Get schema for queue
  completion
    bash                         # Generate bash completion
    zsh                          # Generate zsh completion
    fish                         # Generate fish completion
  version                        # Version with build info
```

**Global flags:**
```
  -b, --broker string    broker address (default "localhost:9876", env: QWER_BROKER)
      --json             output as JSON
      --quiet            suppress non-essential output
      --verbose          enable debug output
      --config string    config file (default ~/.config/qwer-q/config.yaml)
```

---

## Error Message Improvement Examples

### Connection Refused

**Before:**
```
Error: failed to connect: dial tcp localhost:9876: connection refused
```

**After:**
```
Error: cannot connect to broker at localhost:9876

  The broker doesn't appear to be running. Try:

    qwer-q serve                     # start the broker locally
    qwer-q queue list -b host:port   # connect to a different broker
    export QWER_BROKER=host:port     # set default broker address
```

### Schema Registration Failed (protoc missing)

**Before:**
```
Error: failed to compile proto: protoc failed: exec: "protoc": executable file not found in $PATH
```

**After:**
```
Error: protoc is not installed

  Schema registration requires the Protocol Buffers compiler (protoc).

  Install it:
    brew install protobuf            # macOS
    apt install protobuf-compiler    # Debian/Ubuntu
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

### Memory Pressure

**Before (via Go client):**
```
memory pressure: server under load, try again later
```

**After:**
```
Error: broker rejected publish — memory pressure

  The broker is under memory pressure (approaching 400MB limit).
  Messages are being rejected to prevent OOM.

  Options:
    - Wait and retry (consumers may free memory)
    - Increase memory limit: qwer-q serve --memory-limit 800MB
    - Scale horizontally (add more broker instances)
```

### Queue Full

**Before:**
```
queue full: maximum 100000 messages
```

**After:**
```
Error: queue "orders" is full (100,000 messages)

  The queue has reached its message limit. Consumers are not keeping up.

  Options:
    - Ensure consumers are connected and processing
    - Increase queue limit: qwer-q serve --max-queue-size 500000
    - Add more consumers to increase throughput
```

---

## Go Client Library Recommendations

### Minimum Viable Improvements

```go
// 1. Add context to all methods
func (c *Client) Publish(ctx context.Context, queue string, payload []byte) (*protocol.PublishResponse, error)

// 2. Add dial options
func Dial(addr string, opts ...DialOption) (*Client, error)

type DialOption func(*dialConfig)

func WithTimeout(d time.Duration) DialOption
func WithTLS(cfg *tls.Config) DialOption

// 3. Don't swallow marshal errors
data, err := proto.Marshal(req)
if err != nil {
    return nil, fmt.Errorf("marshal request: %w", err)
}

// 4. Add missing methods
func (c *Client) Nack(messageID string, requeue bool) error
func (c *Client) ExtendVisibility(messageID string, extensionSeconds uint32) (time.Time, error)
func (c *Client) Call(queue string, payload []byte, timeoutMs uint32) (*protocol.CallResponse, error)

// 5. Export error code constants
const (
    ErrCodeUnknownOpcode     = 1
    ErrCodeInvalidRequest    = 2
    ErrCodePublishFailed     = 3
    ErrCodeMessageNotFound   = 4
    ErrCodeSchemaValidation  = 5
    ErrCodeSchemaRegFailed   = 6
    ErrCodeSchemaNotFound    = 7
    ErrCodeCallTimeout       = 8
)
```

---

## Quick Wins (< 1 hour each)

1. Add `completion` command — Cobra built-in, ~10 lines
2. Add `QWER_BROKER` env var — One `viper.BindEnv` call
3. Fix `--data-dir` default — Change `/data` to `./data`
4. Add examples to help text — `Example:` field on each cobra.Command
5. Add build metadata to version — `-ldflags` in Makefile/Dockerfile
