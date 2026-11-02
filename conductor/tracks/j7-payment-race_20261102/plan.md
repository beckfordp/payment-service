# Plan: Checkout Journey 7 — payment race reconciliation

## Phase 1: Reconciliation store

- [ ] Task: Failing store tests — flag/reason first-wins, idempotent duplicate
  flags (Red).
- [ ] Task: `V10__create_payment_reconciliation.sql`; add
  `flagReconciliation`/`reconciliationReason` to both interpreters (Green).
- [ ] Task: Migration suite asserts the table.
- [ ] Task: `sbt scalafmtCheck test`.

## Phase 2: Race classification in handlers

- [ ] Task: Failing tests — pre-check contradictions, expired capture,
  post-`None` classification, duplicates (Red). A transient void gateway
  failure stays a DLQ retry and writes no flag.
- [ ] Task: Pure `PaymentRace` classifier; wire `OrderCancelledConsumer` and
  `FulfilmentEventConsumer` (Green).
- [ ] Task: Store-level `IO.both(recordVoid, recordCapture)` race test.
- [ ] Task: Barrier-based concurrent handler test with a latched gateway:
  both observe `Authorized`, exactly one terminal outcome/outbox, loser flag,
  no DLQ.
- [ ] Task: `sbt scalafmtCheck test`.
- [ ] Task: Conductor - User Manual Verification 'Payment race reconciliation'
  (Protocol in workflow.md)

## Review

- Filled after `/conductor:review`.
