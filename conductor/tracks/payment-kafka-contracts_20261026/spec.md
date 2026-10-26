# Spec: Rework 7 payment-service — sync-payment-auth Kafka contracts

## Overview

Payment auth moved to a synchronous `POST /payments/authorize` (2026-10-26). This track brings
payment-service's Kafka layer in line before the business logic lands (Checkout Journey 1):
retire the old async-auth events and register the new authorize/capture/expiry event contracts
exactly as pinned in `gluon/docs/system-design.md`. No authorize endpoint or gateway logic here —
contract/wiring only.

## Functional Requirements

1. **Retire `PaymentSettled`** — delete `PaymentSettledEvent` (case class + Codec/Schema + AsyncAPI
   channel + tests). `PaymentStatus.Settled` itself stays unchanged until Journey 1 redefines it
   as capture.
2. **Stop publishing on Settled** — remove the `PaymentSettled` outbox write branch in
   `PaymentStore.update`; status `Settled` writes no outbox row (payment-service publishes
   nothing until Journey 1/4 add real producers). Outbox table/migrations remain.
3. **Register four `payment-events` publish channels** (`ChannelDirection.Publish(PublisherKind.Cdc)`),
   each with `Codec` + `Schema`:
   - `PaymentAuthorized { orderId, authId, expiresAt, timestamp }` (producer: Journey 1)
   - `PaymentCaptured { orderId, amountCents, timestamp }` (producer: Journey 4)
   - `PaymentAuthorizationExpired { orderId, timestamp }` (producer: Journey 5)
   - `PaymentFailed { orderId, paymentId, amountCents, reason, timestamp }` (producer: Journey 4
     capture-decline only)
4. **Retire the `OrderReserved` consumer** — delete `OrderReservedConsumer`, its suite, the
   `OrderReserved` AsyncAPI subscribe channel, and payment-service's entire Kafka consumer wiring
   (`KafkaConsumers` resource/DLQ/supervision) from `Main`/routes/config; payment-service has no
   consumers until later journeys.
5. **`AsyncApiChannels`** ends up with exactly those four publish channels for `payment-events`; no
   `order-events` subscriptions.
6. **`schemaRegistryCheck`** runs against the updated registrations (shared `gluon` group): the four
   new artifacts register; `PaymentSettled`/`OrderReserved` are no longer checked. No `BACKWARD`
   conflict expected.
7. **Reconcile the cross-repo contract docs**: update `gluon/docs/system-design.md`'s
   `PaymentFailed` JSON shape to include `reason` (capture-decline only) and remove the
   contradictory “authorize-decline also publishes” wording.
8. **Update payment-service docs/scripts** that name retired events/classes: README event list,
   `conductor/product.md`/`tech-stack.md` stale `PaymentEventPublisher`/`OrderReserved` references,
   `verify-kafka-asyncapi-docs.sh`, `verify-schema-registry-check.sh`; delete or rewrite
   `verify-order-reserved-consumer.sh` and the stale `verify-kafka-publisher.sh`.

## Non-Functional Requirements

- TDD; full existing suite stays green (minus deleted consumer tests), coverage not reduced
  materially.
- No new dependencies; no producer/consumer runtime behavior beyond removals.
- New schemas must match the pinned JSON field names/types exactly (`amountCents`, ISO-8601
  instants).
- Preserve the outbox/CDC mechanism and `payment-events` Debezium routing unchanged.

## Acceptance Criteria

- [ ] `AsyncApiChannels.channels` is exactly the four `payment-events` publish channels;
  `AsyncApiChannelsSuite` asserts topic/eventType/direction/payload shapes.
- [ ] Generated `/docs/asyncapi.json` contains the four new event types and neither
  `PaymentSettled` nor `OrderReserved`.
- [ ] No `OrderReservedConsumer`/`KafkaConsumers` consumer wiring remains in payment-service main
  code; full suite green.
- [ ] `PaymentStore` writes no outbox row on a transition to `Settled` (test proves it); outbox
  tests for the retired event removed/updated.
- [ ] `sbt schemaRegistryCheck` passes against the local Apicurio registry; four new artifacts
  exist under group `gluon`.
- [ ] `docs/system-design.md` `PaymentFailed` shape includes `reason`; no
  authorize-decline-publishes wording remains.
- [ ] README/conductor docs/verification scripts contain no references to `PaymentSettled`
  producers or the `OrderReserved` consumer.

## Out of Scope

- `POST /payments/authorize`, `PaymentGatewayClient`, `PaymentStatus.Authorized`, and actually
  producing the new events (Checkout Journeys 1/4/5).
- Changing `PaymentStatus.Settled`'s meaning (Journey 1).
- order-service changes (separate Rework 7 track).
- Removing the outbox table, Debezium connector, or `payment-events` topic.
