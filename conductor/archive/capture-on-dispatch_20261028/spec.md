# Specification: US-19.1 — capture on DispatchConfirmed

## Overview

payment-service consumes `fulfilment-events: DispatchConfirmed` and captures the
held authorization for that order: `authorized -> captured`, publishing
`payment-events: PaymentCaptured {orderId, amountCents, timestamp}`. A capture
decline records the payment `failed` and publishes `PaymentFailed {orderId,
paymentId, amountCents, reason: "capture_failed", timestamp}`. Idempotent on
`orderId` — a redelivered `DispatchConfirmed` never captures twice.

## Functional Requirements

- **FR-1 — Store transitions.** `PaymentStore` gains `recordCapture(orderId)` and
  `recordCaptureFailure(orderId, reason)`: a conditional `authorized ->
  captured|failed` update plus its outbox insert in one transaction; `None` when
  no authorized row exists, nothing written. Both interpreters. No migration —
  the CHECK constraint already allows `captured`/`failed`.
- **FR-2 — Simulated capture.** `PaymentGatewayClient.capture(authId, cardToken)
  -> F[Either[DeclineReason, Unit]]`. `SimulatedGatewayConfig` gains
  `captureOutcomes` keyed by `tok_*` plus `defaultCaptureOutcome`; a decline is
  a value, not an error, so it never reaches the DLQ.
- **FR-3 — Consumer.** `FulfilmentEventConsumer` handles `DispatchConfirmed`:
  unknown payment -> log/skip; already `captured` -> log/skip (duplicate); any
  other non-`authorized` status -> log/skip. Authorized -> resolve the payment
  method's card token, `gateway.capture`; `Right` -> `recordCapture`, `Left` ->
  `recordCaptureFailure` with the contract's fixed `capture_failed` reason.
  Missing `authId`, `paymentMethodId` or method row -> raise through the DLQ
  (data integrity). Existing `DispatchFailed` void path unchanged.
- **FR-4 — AsyncAPI.** Subscribe `DispatchConfirmed` on `fulfilment-events`
  (the `PaymentCaptured`/`PaymentFailed` publish entries already exist); schema
  matches the pinned contract.
- **FR-5 — Idempotency.** A duplicate `DispatchConfirmed` calls capture once and
  leaves exactly one `PaymentCaptured` outbox row.
- **FR-6 — Live proof.** `scripts/verify-capture.sh` proves the success and
  decline paths end-to-end against the local cluster, including duplicate
  injection.

## Non-Functional Requirements

- No new dependencies; no card-token logging beyond existing simulated usage;
  `sbt scalafmtCheck test` and `schemaRegistryCheck` green; >80% coverage of new
  code.

## Acceptance Criteria

- Capture called exactly once per authorized payment, status `captured`,
  `PaymentCaptured` published with the contract payload; a declined capture
  leaves `failed` and publishes `PaymentFailed {reason: capture_failed}` without
  DLQ retry; verify script passes both paths.

## Out of Scope

- Order-service US-19.2/19.3, notification-service US-19.4, gshop US-19.5,
  Journey 5's expiry sweep, a real payment provider, a `captured_at` column.
