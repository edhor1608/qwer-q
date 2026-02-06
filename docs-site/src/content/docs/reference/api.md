---
title: API Reference
description: QWER-Q REST API endpoints and Go client library reference.
---

## REST API (Planned — v1.1)

A REST API for admin operations is planned for v1.1. It will run on the metrics port (default 9877) and expose:

- Queue listing and inspection
- DLQ management (view, replay, purge)
- Schema management
- Metrics and health

Until then, use the [CLI](/reference/cli/) or the Go client library for all operations.

## Current HTTP Endpoints

These endpoints are available on the metrics port (default 9877):

### `GET /health`

Health check endpoint.

**Response:**

```json
{
  "status": "ok",
  "time": "2026-02-06T12:00:00Z"
}
```

### `GET /metrics`

Prometheus-compatible metrics endpoint. Returns all broker metrics in Prometheus exposition format.

```bash
curl http://localhost:9877/metrics
```

Example output (excerpt):

```
# HELP qwerq_messages_published_total Total number of messages published
# TYPE qwerq_messages_published_total counter
qwerq_messages_published_total{queue="orders"} 1523

# HELP qwerq_queue_depth Current number of messages in queue (not in-flight)
# TYPE qwerq_queue_depth gauge
qwerq_queue_depth{queue="orders"} 42

# HELP qwerq_publish_latency_seconds Latency of publish operations
# TYPE qwerq_publish_latency_seconds histogram
qwerq_publish_latency_seconds_bucket{queue="orders",le="0.001"} 1200
```

See [Concepts — Observability](/concepts/#observability) for the full list of metrics.

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
    // Handle error (queue full, schema validation, memory pressure, etc.)
    log.Fatal(err)
}
fmt.Println("Published:", resp.MessageId)
```

**Error types from Publish:**
- Schema validation failed (code 5)
- Queue full (code 3)
- Memory pressure (code 3)
- Duplicate message (code 3)

### Consuming

`Consume` is a blocking call that reads messages until an error occurs or the connection is closed.

```go
err := c.Consume("orders", 1, func(msg *protocol.Message) error {
    fmt.Printf("Received: %s (attempt %d)\n", msg.MessageId, msg.Attempt)

    // Process the message...

    // Acknowledge success
    return c.Ack(msg.MessageId)
})
```

**Parameters:**
- `queue` — Queue name to consume from
- `prefetch` — Maximum unacknowledged messages (use `1` for sequential processing)
- `handler` — Function called for each message; return an error to stop consuming

### Acknowledging

```go
// Positive acknowledgment — message is permanently deleted
err := c.Ack(msg.MessageId)
```

ACK is fire-and-forget on the wire (no response frame).

### Schema Management

```go
// Register a schema
resp, err := c.SchemaRegister("orders", descriptorBytes, "myapp.Order")
fmt.Printf("Registered version: %d\n", resp.Version)

// List all schemas
list, err := c.SchemaList()
for _, s := range list.Schemas {
    fmt.Printf("%s: %s (v%d)\n", s.Queue, s.MessageType, s.Version)
}
```

### Queue Management

```go
// List all queues
list, err := c.QueueList()
for _, q := range list.Queues {
    fmt.Printf("%s: %d messages, %d in-flight\n",
        q.Name, q.MessageCount, q.InFlightCount)
}
```

### Error Handling

Broker errors are returned as `*client.BrokerError`:

```go
resp, err := c.Publish("orders", payload)
if err != nil {
    if brokerErr, ok := err.(*client.BrokerError); ok {
        fmt.Printf("Broker error (code %d): %s\n", brokerErr.Code, brokerErr.Message)
    }
}
```
