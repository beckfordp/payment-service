# Specification: US-20.1 + US-9.3 — expiry sweep and cancellation void

## Overview

Journey 5 needs a producer for `PaymentAuthorizationExpired`, and US-9's
shared cancellation path needs payment-service to void an authorized hold when
`order-events: OrderCancelled` arrives.

The sweep is an in-process fs2 fixed-delay loop (decided 2026-11-02), not a
CronJob: the terminal state write and the outbox row must be one transaction,
and the loop reuses the existing store/outbox/telemetry.

## Functional Requirements

- FR-1: `PaymentStore.expireDueAuthorizations(limit)` atomically selects due
  `authorized` rows (`expires_at <= now()`, `FOR UPDATE SKIP LOCKED`), moves
  them to `voided` with `decline_reason = authorization_expired`, and writes
  one `PaymentAuthorizationExpired` outbox row per payment in the same
  transaction. Both interpreters.
- FR-2: a partial index `payment(status, expires_at) WHERE status =
  'authorized'` supports the scan.
- FR-3: `ExpirySweep` runs on a configurable interval (default 30s, env
  `PAYMENT_EXPIRY_SWEEP_INTERVAL`, batch size 100); one tick's failure is
  logged, the loop survives.
- FR-4: a new `order-events: OrderCancelled` consumer validates the payment's
  order ID, finds the payment, and:
  - `Authorized` + authId -> `gateway.void` then `recordVoid`;
  - `Voided`/`Captured`/`Failed`/unknown -> logged no-op.
  Its own consumer group (`payment-service-order-events`) and DLQ.
- FR-5: a capture arriving after the void already no-ops through the
  conditional `authorized -> captured` update.
- FR-6: AsyncAPI registers the `OrderCancelled` subscription.

## Non-Functional Requirements

- DLQ/retry/supervision identical to the existing `fulfilment-events` consumer.
- `sbt scalafmtCheck test` and `schemaRegistryCheck` green.

## Acceptance Criteria

- A seeded past `expires_at` is swept once: payment `voided`, one
  `PaymentAuthorizationExpired` outbox row, no duplicate on the next tick.
- A live authorization is not swept.
- `OrderCancelled` voids an authorized payment; duplicate/`captured` no-ops.
- `verify-auth-expiry.sh` proves the end-to-end expiry path.

## Out of Scope

Order-service cancellation/reason, inventory/fulfilment fan-out,
notification copy, reconciliation flags, capture-vs-expiry race (US-9.6/21).
