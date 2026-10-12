# Plan — TD-3.4: Transactional outbox for payment-service (ADR 0008)

## Phase 1 — Outbox table + atomic settle write
- [ ] Task: Write failing `MigrationsSuite` test asserting `outbox_events` exists with columns `id`/`aggregate_id`/`payload`/`trace_parent`/`correlation_id`/`created_at` (Red)
- [ ] Task: Add `V3__create_outbox_events.sql` migration to make it pass (Green)
- [ ] Task: Write failing `PaymentStoreSuite` test: `update(id, Settled)` writes a matching outbox row (`aggregate_id`/`correlation_id` = `orderId`) in the same transaction as the status-change UPDATE (Red)
- [ ] Task: Extend `PaymentStore.update` (needs `orderId` + envelope-building inputs threaded in) to write the outbox row inside its existing transaction when transitioning to `Settled` (Green)
- [ ] Task: Write failing atomicity test: `update` on a non-existent payment id (returns `None`) writes no outbox row (Red); confirm the existing `None`-short-circuit path already satisfies this (Green)
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Outbox table + atomic settle write' (Protocol in workflow.md)

## Phase 2 — Wire OrderReservedConsumer to the outbox, not the publisher
- [ ] Task: `OrderReservedConsumer`'s `handleEvent`: replace the `publisher.publishSettled(...)` call with passing the envelope inputs into `store.update` (Red: existing consumer test asserts outbox row rather than publisher invocation; Green: wire it)
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Wire OrderReservedConsumer to the outbox' (Protocol in workflow.md)

## Phase 3 — Delete PaymentEventPublisher
- [ ] Task: Delete `PaymentEventPublisher.scala` and `PaymentEventPublisherSuite.scala`
- [ ] Task: Remove `PaymentEventPublisher.resource(...).use { ... }` wiring and publisher parameter threading from `Main.scala` and `OrderReservedConsumer`
- [ ] Task: Run full test suite, confirm no regressions, confirm nothing still references `PaymentEventPublisher`
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Delete PaymentEventPublisher' (Protocol in workflow.md)
