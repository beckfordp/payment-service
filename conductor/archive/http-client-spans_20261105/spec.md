# Spec: PERF-5.10 — purerest 0.14.0 (Server-kind HTTP spans) (gluon PERF-5.10)

## Overview
A checkout trace held only order-service spans: service-to-service HTTP calls
started new traces, and HTTP spans were kind INTERNAL. purerest 0.14.0's
`ClientTracing` opens a Client-kind span and injects its `traceparent`;
`ServerTracing` spans are Server-kind.

## Functional Requirements
1. purerestlib/purekafka 0.13.0 → 0.14.0.
2. No code change: HTTP server spans become Server-kind with HTTP attributes, and continue the caller's `traceparent`.

## Acceptance Criteria
- `sbt scalafmtCheck test` green.
- Live: an inbound service-to-service call's SERVER span is a child of the caller's CLIENT span.

## Out of Scope
- Route-template span names; cart/catalog (no service-to-service callers).
