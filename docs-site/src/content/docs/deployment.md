---
title: Deployment Guide
description: Deploy QWER-Q with Docker, docker-compose, and production configuration.
---

## Docker (Quickstart)

The simplest deployment — a single `docker run` command:

```bash
docker run -d --name qwer-q \
  -p 9876:9876 \
  -p 9877:9877 \
  -v qwer-q-data:/data \
  qwer-q
```

| Port | Purpose |
|------|---------|
| 9876 | Broker protocol (TCP) — clients connect here |
| 9877 | Metrics (Prometheus) and health check (HTTP) |

The `-v qwer-q-data:/data` mounts a Docker volume so data persists across container restarts.

## Docker Compose

For a more structured setup with Prometheus monitoring:

```yaml
# docker-compose.yml
services:
  qwer-q:
    image: qwer-q
    ports:
      - "9876:9876"
      - "9877:9877"
    volumes:
      - qwer-q-data:/data
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "1.0"
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    depends_on:
      - qwer-q

volumes:
  qwer-q-data:
```

With this Prometheus config:

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'qwer-q'
    static_configs:
      - targets: ['qwer-q:9877']
```

## Building from Source

```bash
# Clone the repository
git clone https://github.com/jonas/qwer-q.git
cd qwer-q

# Build the binary
go build -o bin/qwer-q ./cmd/qwer-q

# Run locally
./bin/qwer-q serve --data-dir ./data

# Or build the Docker image
docker build -t qwer-q .
```

The Dockerfile uses a multi-stage build:

```dockerfile
# Multi-stage build for small image
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o qwer-q ./cmd/qwer-q

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/qwer-q /usr/local/bin/
EXPOSE 9876 9877
ENTRYPOINT ["qwer-q"]
CMD ["serve"]
```

The final image is based on Alpine and contains only the static binary plus CA certificates.

## Production Checklist

### Resource Limits

Always set memory limits in production. The broker is designed for 512 MB containers:

```bash
docker run -d --name qwer-q \
  --memory=512m \
  --cpus=1 \
  -p 9876:9876 \
  -p 9877:9877 \
  -v qwer-q-data:/data \
  qwer-q
```

The broker's internal memory limit (400 MB) leaves ~100 MB headroom for BadgerDB caches and Go runtime overhead. For larger containers, the memory limit can be adjusted in the source.

### Data Persistence

**Always mount a volume for `/data`** in production. Without it, all messages are lost when the container restarts.

```bash
# Named volume (recommended)
-v qwer-q-data:/data

# Bind mount (useful for backups)
-v /opt/qwer-q/data:/data
```

### Health Checks

Add a Docker health check:

```yaml
services:
  qwer-q:
    image: qwer-q
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:9877/health"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 5s
```

Or with `docker run`:

```bash
docker run -d --name qwer-q \
  --health-cmd="wget -q --spider http://localhost:9877/health" \
  --health-interval=10s \
  -p 9876:9876 \
  -p 9877:9877 \
  -v qwer-q-data:/data \
  qwer-q
```

### Monitoring

Scrape the Prometheus metrics endpoint at `http://<host>:9877/metrics`.

**Key metrics to alert on:**

| Metric | Alert Condition | Meaning |
|--------|----------------|---------|
| `qwerq_queue_depth` | Consistently growing | Consumers can't keep up |
| `qwerq_messages_dlq_total` | Increasing | Messages failing repeatedly |
| `qwerq_queue_full_errors_total` | Any occurrence | Queue at capacity — producers blocked |
| `qwerq_publish_latency_seconds` | p99 > 100ms | Storage or memory pressure |

### Logging

The broker outputs structured JSON logs to stdout:

```json
{"time":"2026-02-06T12:00:00Z","level":"INFO","msg":"client connected","addr":"172.17.0.1:54321"}
{"time":"2026-02-06T12:00:00Z","level":"INFO","msg":"message published","queue":"orders","message_id":"01HXY...","client":"172.17.0.1:54321"}
```

Logs are written using Go's `slog` package with a JSON handler. Collect them with any standard log aggregation tool (Loki, Datadog, CloudWatch, etc.).

### Security

Token auth is available and recommended for production:

```bash
qwer-q serve --auth-token "$QWERQ_AUTH_TOKEN"
```

Or via environment variable:

```bash
QWERQ_AUTH_TOKEN=your-shared-secret qwer-q serve
```

If auth is not configured, the broker warns on startup:

```
Warning: Running without authentication - not for production
```

Network-level controls are still required:
- Run in a private network or behind a firewall
- Use Docker networks to limit which containers can reach the broker
- Don't expose port 9876 to the public internet

mTLS is not built in yet.

### Backup & Recovery

BadgerDB data lives in the configured data directory (`/data` by default). To back up:

```bash
# Stop the broker (ensures clean state)
docker stop qwer-q

# Copy the data directory
cp -r /opt/qwer-q/data /opt/qwer-q/backup-$(date +%Y%m%d)

# Restart
docker start qwer-q
```

Alternatively, use Docker volume backups.

## Kubernetes

A basic Kubernetes deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: qwer-q
spec:
  replicas: 1  # Recommended default; cluster mode is preview and configured separately
  selector:
    matchLabels:
      app: qwer-q
  template:
    metadata:
      labels:
        app: qwer-q
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9877"
    spec:
      containers:
        - name: qwer-q
          image: qwer-q:latest
          ports:
            - containerPort: 9876
              name: broker
            - containerPort: 9877
              name: metrics
          resources:
            limits:
              memory: 512Mi
              cpu: "1"
            requests:
              memory: 256Mi
              cpu: "250m"
          volumeMounts:
            - name: data
              mountPath: /data
          livenessProbe:
            httpGet:
              path: /health
              port: metrics
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health
              port: metrics
            initialDelaySeconds: 3
            periodSeconds: 5
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: qwer-q-data
---
apiVersion: v1
kind: Service
metadata:
  name: qwer-q
spec:
  selector:
    app: qwer-q
  ports:
    - name: broker
      port: 9876
      targetPort: 9876
    - name: metrics
      port: 9877
      targetPort: 9877
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: qwer-q-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

**Important:** default deployments should run `replicas: 1` (single-node mode). Clustering exists behind explicit cluster flags and is currently preview.
