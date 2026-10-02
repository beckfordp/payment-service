# Specification: US-6.1 consume order.reserved, charge, publish payment.settled/payment.failed

## Overview
payment-service consumes `order.reserved` from Kafka, creates a `Payment`
record directly via `PaymentStore` (no HTTP round-trip), simulates a charge
(always succeeds in this track — no real provider decided yet, separate ADR
backlog item), and publishes `payment.settled`/`payment.failed` to Kafka.
Mirrors inventory-service's `StockEventPublisher` (producer) and
order-service's `StockEventConsumer` (consumer) patterns exactly: fs2-kafka
3.6.0, plain String key/value, hand-rolled circe JSON (no circe-kafka
serializer layer), Testcontainers Kafka for tests, per-service local
event-shape mirrors (no shared schema registry).

## Functional Requirements
- New `OrderReservedEvent(orderId: String, customerId: String, totalCents:
  Int, timestamp: Instant)` — local mirror of the pinned `order.reserved`
  payload (`gluon/docs/system-design.md`), circe `Codec` via `deriveCodec`.
- New `OrderReservedConsumer.run[F]`: one `fs2.Stream` via
  `KafkaConsumer.stream(consumerSettings).subscribeTo("order.reserved").records`,
  consumer group `payment-service-order-reserved`, `AutoOffsetReset.Earliest`.
  Per record: decode JSON; on decode failure, log error and commit offset
  (skip, matching `StockEventConsumer`'s precedent — no retry on the
  consume/processing side); on success, call `PaymentStore.create(orderId,
  totalCents)` directly, then "charge" (always succeeds this track) via
  `PaymentStore.update(id, PaymentStatus.Settled)`, then publish
  `payment.settled`, then commit offset regardless of outcome (same
  at-least-once, commit-after-process risk profile as the existing
  precedent).
- New `PaymentEventPublisher` (mirrors `StockEventPublisher`):
  `publishSettled`/`publishFailed`, `ProducerSettings[F, String, String]` +
  `KafkaProducer.resource`, keyed by `orderId`. Publish wrapped in a
  hand-rolled cats-retry bounded-retry (cats-retry is already transitive via
  purerestlib) — NOT `purerest.resilience` (that's `Client[F]`-only,
  confirmed by reading its source, doesn't apply to a Kafka producer call) —
  log loudly and drop on exhaustion, matching the documented reliability
  stance.
- New payload contract (not yet pinned anywhere — this track defines it and
  documents it back to `gluon/docs/system-design.md`, not just locally):
  `payment.settled`/`payment.failed` — `{orderId: String, paymentId: String,
  amountCents: Int, timestamp: Instant}` (same shape, two case classes
  `PaymentSettledEvent`/`PaymentFailedEvent`, mirroring inventory-service's
  `StockReservedEvent`/`StockReservationFailedEvent` pattern). `orderId` is
  what order-service's `order.status-changed` consumer actually needs to
  correlate.
- `Main.scala`: consumer runs as a backgrounded fiber
  (`.compile.drain.background.use { ... }`), same wiring shape as
  `order-service/Main.scala`.
- `PaymentServiceConfig` gains a `KafkaConfig(bootstrapServers: String)`,
  mirroring `OrderServiceConfig`/`InventoryServiceConfig`.

## Non-Functional Requirements
- `build.sbt`: add `fs2-kafka` 3.6.0, `testcontainers-scala-kafka` 0.43.6
  (Test), matching the exact versions already used by
  inventory-service/order-service.
- Scalafmt-clean; tagless-final / ADT-error style per
  `development-guidelines.md`.
- No duplicate-delivery guard in this track — a redelivered `order.reserved`
  creates a second `Payment` row for the same `orderId`; that's explicitly
  US-6.2's job (Redis idempotency keys), not this track's.

## Acceptance Criteria
- A synthetic `order.reserved` event published to a test Kafka
  (Testcontainers) results in a `Payment` created with status `settled`, and
  a `payment.settled` event published with the correct
  `orderId`/`paymentId`/`amountCents`.
- A malformed `order.reserved` payload is logged as a decode failure and does
  not crash the consumer stream; the offset is still committed.
- `PaymentEventPublisher.publishFailed` is covered by a direct unit test
  (forcing the failure path), since the charge-simulation itself always
  succeeds in this track — `payment.failed` is never produced by the real
  consumer flow yet.
- A publish failure that exhausts the bounded retry is logged loudly and
  does not crash the consumer (matches the no-outbox/log-and-drop stance);
  covered by a test against a producer that always fails.
- `sbt scalafmtCheck test` passes.
- `gluon/docs/system-design.md`'s `payment.settled`/`payment.failed` payload
  contract section is filled in with the shape this track defines
  (cross-repo doc update, not just this repo's own docs).

## Out of Scope
- US-6.2 (Redis idempotency keys / duplicate-delivery guard) — separate
  backlog item.
- Real payment provider integration — separate ADR backlog item; charge is
  simulated (always succeeds) in this track.
- order-service's own `order.reserved` producer and `OrderStatus`
  `Confirmed`/`PaymentFailed` cases — a different repo/track; this track
  only needs order-service's documented payload contract, not its
  implementation.
- Any status-transition/compensation logic beyond creating+settling the
  Payment record.
