# Product Guide — payment-service

## Context
Part of the **Gluon** platform (v1) — a production-grade microservices
e-commerce platform built pure-FP-first in Scala 3 (Cats Effect / http4s).
See the cross-repo [Gluon Product Vision](../../../docs/product.md) (in the
`gluon/` monorepo root) for the full platform vision, naming scheme, and
goals. This document scopes that vision down to payment-service's own slice.

## What this service does
payment-service owns the **Payment** domain entity — the charge record
against an order — for the walking-skeleton checkout flow in Gluon's user
stories. It is the only service permitted to read or write the `payment`
Postgres database (one-db-per-service).

Generated via `pure-service-generator` (giter8 template over `purerest`),
field-spec applied from `gluon/specs/payment.yaml`, hand-extended per
`gluon/backlogs/payment-service.md`: the `status` field hardened to a closed
ADT (done); US-6.1/US-6.2 below remain backlog items, not yet implemented.

## Domain model
- **Payment** — `orderId` (create-only, logical reference to order-service's
  `Order` — different service, different database, no real FK), `amountCents`
  (create-only), `status` (server-defaulted `PaymentStatus.Pending`,
  serialized as `"pending"`; hardened to a closed `PaymentStatus` ADT —
  `Pending`/`Settled`/`Failed` — at both the Scala and DB level, mirroring
  order-service's own `OrderStatus`: a `text.eimap`-based Skunk codec plus a
  Postgres `CHECK` constraint (`payment_status_check`). PATCH/PUT reject any
  other value with `400`), `createdAt`/`updatedAt`.
- No other fields generated — the full CRUD surface (`POST`/`GET`/`PATCH`/
  `PUT`/`DELETE /payments{/id}`) matches the field-spec exactly; nothing
  codegen couldn't represent beyond the enum gap above.

## User stories in scope (gluon/docs/user-stories.md)
- US-6.1 — consume `order.created`, charge, publish `payment.settled` /
  `payment.failed`
- US-6.2 — Redis idempotency keys to avoid double-charging on
  retry/redelivery

## Sequencing (gluon/PLAN.md)
- **Phase 4** (after Phases 1–3 prove the sync-reserve + async-outcome
  patterns) — US-6.1, US-6.2. Stub/fan-out: drive the consumer with
  synthetic `order.created` events published to a test Kafka — no live
  order-service needed; the "charge" step itself is stubbed/simulated (no
  real payment provider decided yet, see Open design questions below);
  idempotency is proven by replaying the same synthetic event twice.

## Events
- Consumes: `order.created` — ⚠ **no producer defined yet** in
  order-service (see `gluon/docs/system-design.md`'s "Open design
  questions" — the `order.created` producer gap must be resolved in
  order-service before US-6.1 can be implemented against a real payload,
  though the stubbed/synthetic-event test path is unaffected).
- Publishes: `payment.settled`, `payment.failed` — no payload contract
  documented yet in `gluon/docs/system-design.md`; add one there (not just
  in this repo) when US-6.1 defines it.

## Out of scope for this service
- Auth/identity (bare `customerId`/`orderId` for now — no user-service yet)
- Order and inventory domain data (order-service's/inventory-service's job;
  `orderId` is a logical reference only, never a real FK)
- Choice of real payment provider (Stripe etc.) vs. simulated charge — open
  ADR, see `gluon/backlogs/payment-service.md`
