# Optimization Loop 9: Decode Buffer Release Experiment

**Date:** 2026-04-13
**Branch:** `perf/parallel-lab`

## Goal

See whether the protocol buffer pools in `DecodeFrame` can actually be used in production code by returning decode buffers after request/response processing.

## Why This Was Investigated

`DecodeFrame` already allocates from size-tiered pools, but there was no production caller returning those buffers.
That suggested a possible allocation reduction on both the client and server sides of the socket path.

## Change Tried

Added a `Frame.Release()` path and attempted to call it in:
- server connection handling after request dispatch
- client response/message decode paths after protobuf unmarshal

## What Happened

The experiment caused severe throughput regressions during the live benchmark:
- one run dropped to `4.6K msg/s`
- another dropped to `2.4K msg/s`

## Root Cause

The decode buffer cannot be safely returned early in the current architecture because protobuf decode input ownership is not isolated enough.

Two concrete hazards showed up:
- publish requests carry `payload []byte`, and the broker stores that payload in live message state
- client-side decoded responses/messages may still reference input-backed data after unmarshal

So the optimization was not just noisy; it was unsafe.

## Decision

Revert the decode-buffer release experiment completely.

Keep:
- the `WriteFrame` wire-write optimization from Loop 8

Do not keep:
- `Frame.Release()` in production flow
- any attempt to return decode buffers without first proving ownership/copy semantics end-to-end

## Revisit Trigger

Only revisit if request/response decode paths are redesigned so that:
- escaping fields are copied explicitly
- frame lifetime is owned centrally and provably ends before release
