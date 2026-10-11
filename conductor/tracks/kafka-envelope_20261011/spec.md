# Spec: Migrate to order-events/payment-events topics with envelope

## Overview
Migrate payment-service's Kafka consume/publish from four per-event-type
topics (consumes `order.reserved`; publishes `payment.settled`,
`payment.failed`) to two domain topics (`order-events`, `payment-events`),
every event wrapped in a shared envelope carrying `eventType` so consumers
dispatch on that instead of topic subscription. Part of a coordinated
cross-repo cutover ([ADR 0009](../../../../docs/adr/0009-one-kafka-topic-per-domain.md),
Accepted 2026-10-11; `gluon/PLAN.md`'s Rework 2) — order-service's
`OrderEventPublisher`/`PaymentEventConsumer` change in lockstep in its own
repo/track.

## Functional Requirements
- New `EventEnvelope` case class (same shape as every other repo's copy,
  no shared library — matching this codebase's existing pattern):
  `eventId`, `eventType`, `timestamp`, `source`, `correlationId`,
  `payload: Json`.
- `OrderReservedConsumer` subscribes to `order-events` (was
  `order.reserved`). Decodes the envelope first; an envelope whose
  `eventType` isn't `"OrderReserved"` (e.g. a future `OrderStatusChanged`
  on the same topic) is silently skipped, not logged as malformed — only
  an envelope that fails to decode at all, or whose `eventType` is
  `"OrderReserved"` but whose `payload` fails to decode as
  `OrderReservedEvent`, is a decode-error log line. Existing
  `Option[String]`/`Option[String]` null-safe key/value deserializer
  (TD-2.1's fix) is preserved.
- `PaymentEventPublisher.publishSettled`/`publishFailed` both produce to a
  single `payment-events` topic (replacing `settledTopic`/`failedTopic`),
  wrapping the existing payload with `eventType = "PaymentSettled"` /
  `"PaymentFailed"`. `source = "payment-service"`, `correlationId =
  event.orderId` (payment-service's events are already keyed by `orderId`
  today — this is the same id, just promoted to the envelope field too).

## Non-Functional Requirements
- No new dependency.
- Existing retry/timeout behavior (`PaymentEventPublisher`'s bounded retry
  against an unreachable broker) unchanged.

## Acceptance Criteria
- [x] `PaymentEventPublisherSuite` confirms both event types land on
  `payment-events`, decode as a valid `EventEnvelope` with the right
  `eventType`/`source`/`correlationId`, payload round-trips.
- [x] `OrderReservedConsumerSuite` confirms: a synthetic `OrderReserved`
  envelope on `order-events` still creates a settled `Payment` and
  publishes `PaymentSettled`; a differently-typed envelope on the same
  topic is ignored, not treated as malformed; a malformed envelope and a
  null-keyed record still don't block a later good event (existing
  TD-2.1-adjacent resilience behavior preserved).
- [x] Full `sbt test` suite stays green (73/73).

## Out of Scope
- `purekafka`-based consumer resilience (Rework 3) — separate track.
- Any change to the synchronous charge-simulation logic in
  `OrderReservedConsumer.handleEvent`.
