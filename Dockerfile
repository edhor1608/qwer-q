# Multi-stage build for small image
FROM golang:1.26.4-alpine AS builder
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
