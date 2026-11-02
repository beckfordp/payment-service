# Spec — TD-3.4: Transactional outbox for payment-service (ADR 0008)

## Overview
payment-service's `OrderReservedConsumer` calls `store.create(...)` then
`store.update(payment.id, Settled)` (Postgres) and then, as a separate
step, calls `PaymentEventPublisher.publishSettled(...)` to produce
directly to Kafka (TD-3). This track closes that gap by writing an
`outbox_events` row atomically with the `update` call that transitions a
payment to `Settled` — the state change `PaymentSettled` actually
announces. (`publishFailed` has no call site today — the simulated charge
always succeeds, per US-6.1 — so there's no failure path to migrate yet;
it moves to the outbox mechanically whenever a real decline path is added
later.)

## Functional Requirements
1. New Flyway migration (`V3__create_outbox_events.sql`): `outbox_events`
   table — `id` (uuid, pk), `aggregate_id` (text, not null — holds
   `orderId`, matching `publishSettled`'s existing Kafka key), `payload`
   (jsonb, not null — the complete `EventEnvelope` JSON), `trace_parent`
   (text, nullable), `correlation_id` (text, not null — also `orderId`,
   unchanged), `created_at` (timestamptz, not null, default `now()`). No
   `sent`/`processed` column.
2. `PaymentStore.update` is changed to write the matching outbox row
   **inside the same transaction** as its status-change UPDATE, when
   transitioning to `Settled` — true atomicity for the one real call site
   today.
3. `PaymentEventPublisher` (the trait, its Kafka-producer `Resource`,
   `noOp`, including the as-yet-unused `publishFailed`) is deleted
   outright, along with `PaymentEventPublisherSuite.scala`.
4. `Main.scala` wiring updated: the `PaymentEventPublisher.resource(...).use
   { ... }` block and its threading through to `OrderReservedConsumer`
   removed.
5. Test coverage (Testcontainers-Postgres): asserts the outbox row
   (`aggregate_id`/`correlation_id` = `orderId`, decodable `payload`) is
   written atomically with the `Settled` transition; an atomicity test
   proves a no-op update (e.g. updating a non-existent payment id,
   returning `None`) writes no outbox row either.

## Non-Functional Requirements
- No live Kafka/Kafka Connect/Debezium needed for this track's tests.
- No change to any consumer-facing behavior — `OrderReservedConsumer`
  still processes `OrderReserved` and reaches the same end state.

## Acceptance Criteria
- `outbox_events` table exists per the shape above.
- `OrderReservedConsumer`'s settle step writes to the outbox instead of
  calling the publisher.
- `PaymentEventPublisher` and its test suite are gone.
- Atomicity test passes.
- Full existing test suite still passes.

## Out of Scope
- TD-3.1 (Kafka Connect + Debezium connector, gluon-root) — separate
  track.
- The Outbox Event Router SMT's own config — lives with TD-3.1.
- Outbox-row cleanup CronJob — also TD-3.1/gluon-root.
- order-service's and inventory-service's own outbox migrations
  (TD-3.2/TD-3.3, both done) — separate tracks, already complete.
- Building an actual decline/failure path for the simulated charge
  (that's ADR 0011/US-14's job, not this track's).
