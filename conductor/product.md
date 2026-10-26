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
ADT (done); US-6.1 done (consumes `order.reserved`, charges (simulated),
publishes `payment.settled`/`payment.failed`); US-6.2 remains a backlog
item, not yet implemented.

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
- **Known gotcha fixed in `OrderReservedConsumer`:** a null Kafka message key
  (e.g. a producer that never sets one) used to crash the whole background
  consumer stream silently; both key and value deserializers are now
  `Option[String]` (fs2-kafka's `Deserializer.option`) to guard against
  this. The same latent issue likely exists in order-service's
  `StockEventConsumer` (same `ConsumerSettings[F, String, String]` pattern)
  — not fixed there, out of scope for this repo.

## User stories in scope (gluon/docs/user-stories.md)
- US-6.1 — consume `order.reserved`, charge, publish `payment.settled` /
  `payment.failed` (done)
- US-6.2 — Redis idempotency keys to avoid double-charging on
  retry/redelivery

## Sequencing (gluon/PLAN.md)
- **Phase 4** (after Phases 1–3 prove the sync-reserve + async-outcome
  patterns) — US-6.1, US-6.2. Stub/fan-out: drive the consumer with
  synthetic `order.reserved` events published to a test Kafka — no live
  order-service needed; the "charge" step itself is stubbed/simulated (no
  real payment provider decided yet, see Open design questions below);
  idempotency is proven by replaying the same synthetic event twice.

## Events
One topic per domain with a shared envelope (ADR 0009, migrated
2026-10-11, `kafka-envelope_20261011`), plus the 2026-10-26 sync-payment-auth
decision: authorization is a synchronous REST call from order-service, so this
service consumes no Kafka topics today (the old `OrderReserved` consumer was
retired by Rework 7).
- Publishes: `payment-events` — four contracts pinned in
  `gluon/docs/system-design.md`: `PaymentAuthorized` (authorize-success async
  side effect, Journey 1), `PaymentCaptured` (capture on dispatch, Journey 4),
  `PaymentAuthorizationExpired` (expiry sweep, Journey 5), and `PaymentFailed`
  (capture-decline only, with `reason: capture_failed`; authorize-decline stays
  synchronous and publishes nothing). All via the `outbox_events` table +
  Debezium CDC, keyed and `correlationId`'d by `orderId`. The producers land
  with those journeys — no code writes these rows yet.

## Out of scope for this service
- Auth/identity (bare `customerId`/`orderId` for now — no user-service yet)
- Order and inventory domain data (order-service's/inventory-service's job;
  `orderId` is a logical reference only, never a real FK)
- Choice of real payment provider (Stripe etc.) vs. simulated charge — open
  ADR, see `gluon/backlogs/payment-service.md`
