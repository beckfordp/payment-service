# Specification: US-18.2 — void authorization on DispatchFailed

## Overview

payment-service consumes `fulfilment-events: DispatchFailed` and voids an
authorized payment's gateway hold, recording the payment as `Voided`. Nothing
is published.

## Functional Requirements

- **FR-1 — Status.** `PaymentStatus` gains `Voided`; migration `V9` widens the
  CHECK constraint.
- **FR-2 — Consumer.** New `FulfilmentEventConsumer` on `fulfilment-events`
  (group `payment-service-fulfilment-events`): null-safe, envelope decode,
  handles only `DispatchFailed`, malformed payloads logged, processing
  DLQ-guarded.
- **FR-3 — Void flow.** Look up by `orderId`: missing or not `authorized` →
  log and skip. Authorized → `PaymentGatewayClient.void(authId)`, then on
  success mark `voided` (conditional authorized→voided). A gateway failure goes
  through the DLQ retry path and leaves the payment authorized.
- **FR-4 — Idempotency.** A duplicate finds the payment already `voided` and
  no-ops; the void is never called twice.
- **FR-5 — AsyncAPI.** Subscribe `DispatchFailed` on `fulfilment-events`;
  schemaRegistryCheck passes.
- **FR-6 — Live proof.** Authorized order → fulfilment forced failure → payment
  row `voided`; a duplicate is harmless.

## Non-Functional Requirements

- No new dependencies; no event published; no PAN/token logging; `sbt
  scalafmtCheck test` green.

## Acceptance Criteria

- Void called exactly once per authorized payment; status `voided`;
  non-authorized/missing logged no-ops; live check passes.

## Out of Scope

- capture (US-19), notifications, order-side changes, auth expiry (US-20).
