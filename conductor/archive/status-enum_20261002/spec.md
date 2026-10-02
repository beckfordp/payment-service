# Specification: Harden Payment.status to PaymentStatus ADT

## Overview
Harden payment-service's `status` field from a raw `String` to a closed
`PaymentStatus` ADT (`Pending`/`Settled`/`Failed`), mirroring order-service's
`OrderStatus` hardening exactly. Codegen v1 has no enum type, so the
field-spec used `String` as a stand-in; this track closes that gap at both
the Scala and Postgres levels.

## Functional Requirements
- New `paymentservice.PaymentStatus` sealed trait with case objects
  `Pending`/`Settled`/`Failed`; `asString`/`fromString(raw): Either[String,
  PaymentStatus]` methods, same shape as `OrderStatus`.
- `Payment.status: PaymentStatus` (was `String`).
- `PaymentStore` Skunk codec: `text.eimap(PaymentStatus.fromString)(_.asString)`
  — Postgres column stays `TEXT`.
- New Flyway migration `V2__add_payment_status_check.sql`:
  `ALTER TABLE "payment" ADD CONSTRAINT payment_status_check CHECK (status IN ('pending','settled','failed'))`.
- `PaymentError` ADT gains `InvalidStatus(raw: String)` alongside existing
  `PaymentNotFound`.
- `PaymentRoutes` PATCH/PUT: parse `UpdatePaymentRequest.status` via
  `PaymentStatus.fromString` *before* calling the store; invalid value →
  `Left(InvalidStatus(raw))` → `400` `{"error": "Invalid payment status:
  '<raw>'"}` via a new `oneOfVariant`, sharing the error output with the
  existing 404 `notFoundVariant`.
- Wire format unchanged: `PaymentResponse.status` stays a plain lowercase
  `String` (`.asString` at the boundary) — no JSON shape change.
- Create-path default stays `PaymentStatus.Pending` (serialized `"pending"`).

## Non-Functional Requirements
- Scalafmt-clean; tagless-final / ADT-error style per
  `development-guidelines.md`.
- Existing test suites (in-memory + Postgres store, routes) updated for the
  new type, same coverage shape.

## Acceptance Criteria
- PATCH/PUT with `status: "settled"` or `"failed"` → 200, persists
  correctly.
- PATCH/PUT with an invalid status (e.g. `"shipped"`) → 400, store never
  touched.
- A direct SQL write with an out-of-set status is rejected by the Postgres
  CHECK constraint.
- `sbt scalafmtCheck test` passes.
- POST/GET/DELETE behavior and wire format unchanged.

## Out of Scope
- US-6.1 (Kafka consume/publish), US-6.2 (Redis idempotency),
  payment-provider ADR — separate backlog items.
- Any status-transition/state-machine rules (e.g. disallowing
  settled→pending) — this track only closes the representation gap.
