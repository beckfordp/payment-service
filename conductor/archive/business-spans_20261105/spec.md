# Spec: business spans (gluon PERF-5.9)

## Overview
Traces show the HTTP and `<topic> process` spans but nothing inside them. Name
the business steps so a trace reads as the business flow.

## Functional Requirements
1. `PaymentStoreTracing` decorator: spans `payment.record_authorization|record_decline|record_capture|record_capture_failure|record_void|expire` with `order.id`; `PaymentGatewayClient` decorator: `payment.gateway.authorize|capture` with `outcome`.
2. Span `outbox.write` (attrs `outbox.event_type`, `outbox.aggregate_id`) inside `insertOutbox`; the `traceparent` stored in the row is read inside it, so consumers are its children.

## Acceptance Criteria
- Tests (purerest `Tracing.test`): each traced step finishes one span with the
  expected name and attributes, parented on the caller's span; a raised error
  sets span status Error.
- `sbt scalafmtCheck test` green.
- Live: the spans appear in Tempo under the request/consumer span.

## Out of Scope
- skunk per-query spans; reads, reconciliation; HTTP client propagation (PERF-5.10).
