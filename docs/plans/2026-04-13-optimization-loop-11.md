# Optimization Loop 11: Exact Protobuf Fast Paths For Publish/Ack

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

Push further into the exact producer hot path used by the benchmark:
- publish request encode on the Go client
- publish ack encode on the server
- publish ack decode on the Go client
- ack request encode on the Go client

## Motivation

The generic protobuf path is flexible, but the benchmarked client uses very simple shapes repeatedly:
- `Publish(queue, payload)` with no headers or optional fields
- `Ack(messageID)` with one string field
- `PublishResponse` with one string field

These are good candidates for exact fast encoders/decoders that preserve wire compatibility.

## Changes

Added exact protobuf payload helpers in `internal/protocol`:
- `EncodePublishResponsePayload`
- `DecodePublishResponsePayload`
- `EncodeSimplePublishRequestPayload`
- `EncodeSimpleConsumeRequestPayload`
- `EncodeAckRequestPayload`

Applied them to the hot paths:
- server publish ack response writing
- client publish request write
- client publish ack decode with fallback to generic protobuf decode if the fast path does not match
- client consume request write
- client ack request write

Files changed:
- `internal/protocol/publish_response.go`
- `internal/protocol/publish_response_test.go`
- `internal/protocol/client_codec.go`
- `internal/protocol/client_codec_test.go`
- `pkg/client/client.go`
- `internal/broker/server.go`

## Validation

```bash
go test ./...
```

Result:
- pass

## Microbench Results

Publish response fast path:

```bash
go test ./internal/protocol -run '^$' -bench 'Benchmark(EncodePublishResponsePayload|ProtoMarshalPublishResponse|DecodePublishResponsePayload|ProtoUnmarshalPublishResponse)$' -benchmem -count=5
```

Results:
- `EncodePublishResponsePayload`: `15.7-25.3 ns/op`, `48 B/op`, `1 alloc/op`
- `ProtoMarshalPublishResponse`: `64.8-66.5 ns/op`, `32 B/op`, `1 alloc/op`
- `DecodePublishResponsePayload`: `14.0-19.2 ns/op`, `32 B/op`, `1 alloc/op`
- `ProtoUnmarshalPublishResponse`: `75.6-76.0 ns/op`, `96 B/op`, `2 allocs/op`

Simple client request encoders:

```bash
go test ./internal/protocol -run '^$' -bench 'Benchmark(EncodeSimplePublishRequestPayload|ProtoMarshalSimplePublishRequest|EncodeAckRequestPayload|ProtoMarshalAckRequest)$' -benchmem -count=5
```

Results:
- `EncodeSimplePublishRequestPayload`: `153.9-245.8 ns/op`, `1152 B/op`, `1 alloc/op`
- `ProtoMarshalSimplePublishRequest`: `174.2-302.4 ns/op`, `1152 B/op`, `1 alloc/op`
- `EncodeAckRequestPayload`: `14.0-14.5 ns/op`, `48 B/op`, `1 alloc/op`
- `ProtoMarshalAckRequest`: `48.6-50.3 ns/op`, `32 B/op`, `1 alloc/op`

Readout:
- the fast-path publish response codec is a clear win
- the fast-path ack encoder is a clear win
- the simple publish request encoder is usually faster, though its gain is smaller than the publish-response win

## End-To-End Readout

The live 10-second throughput harness remained noisy, but repeated runs after these changes reached:
- `13.7K msg/s`
- `16.5K msg/s`
- `12.7K msg/s`
- `16.6K msg/s`

Interpretation:
- the harness still swings enough that single samples are not trustworthy
- there is no clear evidence of regression
- the protocol microbenches provide strong proof that the hot publish/ack encode-decode path got cheaper

## Decision

Keep the exact fast paths.

Why:
- wire-compatible with existing protobuf messages
- scoped only to the simple client/server hot path
- measurable improvement in targeted protocol microbenches
- client decode still falls back to generic protobuf decode if payload shape changes
