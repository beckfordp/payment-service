# Plan — TD-3.4: Transactional outbox for payment-service (ADR 0008)

## Phase 1 — Outbox table + atomic settle write
- [x] Task: Write failing `MigrationsSuite` test asserting `outbox_events` exists with columns `id`/`aggregate_id`/`payload`/`trace_parent`/`correlation_id`/`created_at` (Red) `893057b`
- [x] Task: Add `V3__create_outbox_events.sql` migration to make it pass (Green) `893057b`
- [x] Task: Write failing `PaymentStoreSuite` test: `update(id, Settled)` writes a matching outbox row (`aggregate_id`/`correlation_id` = `orderId`) in the same transaction as the status-change UPDATE (Red) `ab88aa9`
- [x] Task: Extend `PaymentStore.update` (needs `orderId` + envelope-building inputs threaded in) to write the outbox row inside its existing transaction when transitioning to `Settled` (Green) `ab88aa9` — deviation: no new parameters were actually needed, `orderId`/`amountCents`/`updatedAt` already come back from the existing `UPDATE...RETURNING`
- [x] Task: Write failing atomicity test: `update` on a non-existent payment id (returns `None`) writes no outbox row (Red); confirm the existing `None`-short-circuit path already satisfies this (Green) `ab88aa9`
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Outbox table + atomic settle write' (Protocol in workflow.md)

## Phase 2 — Wire OrderReservedConsumer to the outbox, not the publisher
- [ ] Task: `OrderReservedConsumer`'s `handleEvent`: replace the `publisher.publishSettled(...)` call with passing the envelope inputs into `store.update` (Red: existing consumer test asserts outbox row rather than publisher invocation; Green: wire it)
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Wire OrderReservedConsumer to the outbox' (Protocol in workflow.md)

## Phase 3 — Delete PaymentEventPublisher
- [ ] Task: Delete `PaymentEventPublisher.scala` and `PaymentEventPublisherSuite.scala`
- [ ] Task: Remove `PaymentEventPublisher.resource(...).use { ... }` wiring and publisher parameter threading from `Main.scala` and `OrderReservedConsumer`
- [ ] Task: Run full test suite, confirm no regressions, confirm nothing still references `PaymentEventPublisher`
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Delete PaymentEventPublisher' (Protocol in workflow.md)
