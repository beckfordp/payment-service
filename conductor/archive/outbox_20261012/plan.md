# Plan — TD-3.4: Transactional outbox for payment-service (ADR 0008)

## Phase 1 — Outbox table + atomic settle write [checkpoint: 4391b80]
- [x] Task: Write failing `MigrationsSuite` test asserting `outbox_events` exists with columns `id`/`aggregate_id`/`payload`/`trace_parent`/`correlation_id`/`created_at` (Red) `893057b`
- [x] Task: Add `V3__create_outbox_events.sql` migration to make it pass (Green) `893057b`
- [x] Task: Write failing `PaymentStoreSuite` test: `update(id, Settled)` writes a matching outbox row (`aggregate_id`/`correlation_id` = `orderId`) in the same transaction as the status-change UPDATE (Red) `ab88aa9`
- [x] Task: Extend `PaymentStore.update` (needs `orderId` + envelope-building inputs threaded in) to write the outbox row inside its existing transaction when transitioning to `Settled` (Green) `ab88aa9` — deviation: no new parameters were actually needed, `orderId`/`amountCents`/`updatedAt` already come back from the existing `UPDATE...RETURNING`
- [x] Task: Write failing atomicity test: `update` on a non-existent payment id (returns `None`) writes no outbox row (Red); confirm the existing `None`-short-circuit path already satisfies this (Green) `ab88aa9`
- [x] Task: Conductor - User Manual Verification 'Phase 1: Outbox table + atomic settle write' (Protocol in workflow.md) `4391b80` — reduced-prompting mode: automated-test verification only (77 passed), no interactive walkthrough; see git note on `4391b80`

## Phase 2 — Wire OrderReservedConsumer to the outbox, not the publisher [checkpoint: dbbadd8]
- [x] Task: `OrderReservedConsumer`'s `handleEvent`: replace the `publisher.publishSettled(...)` call with passing the envelope inputs into `store.update` (Red: existing consumer test asserts outbox row rather than publisher invocation; Green: wire it) `e5cf8e2` — deviation: no envelope inputs needed passing in (Phase 1's `store.update` already builds the full envelope internally); `OrderReservedConsumerSuite`'s 4 tests redesigned to observe settlement via a `Ref`-backed store wrapper instead of racing against a live `payment-events` Kafka message (the app no longer produces one), plus a recording-publisher double in the happy-path test proving `publishSettled` is called zero times — see git note on `e5cf8e2` for full detail
- [x] Task: Conductor - User Manual Verification 'Phase 2: Wire OrderReservedConsumer to the outbox' (Protocol in workflow.md) `dbbadd8` — reduced-prompting mode: automated-test verification only (77 passed), no interactive walkthrough; see git note on `dbbadd8`

## Phase 3 — Delete PaymentEventPublisher [checkpoint: aba3838]
- [x] Task: Delete `PaymentEventPublisher.scala` and `PaymentEventPublisherSuite.scala` `85d64c2`
- [x] Task: Remove `PaymentEventPublisher.resource(...).use { ... }` wiring and publisher parameter threading from `Main.scala` and `OrderReservedConsumer` `85d64c2` — done in the same commit as the deletion above, since neither compiles without the other; also dropped the Phase-2-era `recordingPublisher` test double from `OrderReservedConsumerSuite` (no type left to construct it against) — see git note on `85d64c2` for full detail
- [x] Task: Run full test suite, confirm no regressions, confirm nothing still references `PaymentEventPublisher` `85d64c2` — `sbt test`: 74 passed, 0 failed (down from 77, the 3 deleted `PaymentEventPublisherSuite` tests); `grep -rl PaymentEventPublisher src/` returns nothing
- [x] Task: Conductor - User Manual Verification 'Phase 3: Delete PaymentEventPublisher' (Protocol in workflow.md) `aba3838` — reduced-prompting mode: automated-test verification only (74 passed), no interactive walkthrough; see git note on `aba3838`
