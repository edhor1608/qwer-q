---
title: API Reference
description: QWER-Q HTTP API endpoints and Go client library reference.
---

## REST API

The admin REST API is available now on the metrics port (default `9877`).

Base URL:

```text
http://localhost:9877
```

### Core Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness endpoint |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/api/v1/stats` | Broker runtime stats |
| `GET` | `/api/v1/consumers` | Active consumer counts by queue |
| `WS` | `/api/v1/ws` | WebSocket stream for dashboard updates |

### Queue Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/queues` | List queues with depth/in-flight/consumer counts |
| `GET` | `/api/v1/queues/{name}` | Queue details (max size, retries, failure policy, schema presence) |
| `DELETE` | `/api/v1/queues/{name}` | Purge queue messages |
| `GET` | `/api/v1/queues/{name}/messages?limit=10` | Peek queue messages |
| `GET` | `/api/v1/queues/{name}/dlq?limit=100` | List DLQ messages |
| `POST` | `/api/v1/queues/{name}/dlq/retry` | Retry DLQ messages back to source queue |
| `DELETE` | `/api/v1/queues/{name}/dlq` | Purge DLQ |

### Schema Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/schemas` | List registered schemas |
| `GET` | `/api/v1/schemas/{queue}` | Get schema details for a queue |

### Example: list queues

```bash
curl http://localhost:9877/api/v1/queues
```

Example response:

```json
[
  {
    "name": "orders",
    "depth": 42,
    "in_flight": 3,
    "consumer_count": 2
  }
]
```

### Example: queue detail

```bash
curl http://localhost:9877/api/v1/queues/orders
```

Example response:

```json
{
  "name": "orders",
  "depth": 42,
  "in_flight": 3,
  "consumer_count": 2,
  "max_size": 10000,
  "max_retries": 5,
  "failure_policy": "dlq",
  "has_schema": true
}
```

---

## Go Client Library

The Go client library is at `github.com/jonas/qwer-q/pkg/client`.

### Connecting

```go
import "github.com/jonas/qwer-q/pkg/client"

c, err := client.Dial("localhost:9876")
if err != nil {
    log.Fatal(err)
}
defer c.Close()
```

### Publishing

```go
resp, err := c.Publish("orders", payloadBytes)
if err != nil {
    log.Fatal(err)
}
fmt.Println("Published:", resp.MessageId)
```

### Consuming

```go
err := c.Consume("orders", 1, func(msg *protocol.Message) error {
    // Process message...
    return c.Ack(msg.MessageId)
})
```

### Schema Management

```go
resp, err := c.SchemaRegister("orders", descriptorBytes, "myapp.Order")
fmt.Printf("Registered version: %d\n", resp.Version)

list, err := c.SchemaList()
for _, s := range list.Schemas {
    fmt.Printf("%s: %s (v%d)\n", s.Queue, s.MessageType, s.Version)
}
```
