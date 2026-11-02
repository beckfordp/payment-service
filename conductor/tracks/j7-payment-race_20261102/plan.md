# Plan: Checkout Journey 7 — payment race reconciliation

## Phase 1: Reconciliation store

- [x] Task: Failing store tests — flag/reason first-wins, idempotent duplicate
  flags (Red).
- [x] Task: `V10__create_payment_reconciliation.sql`; add
  `flagReconciliation`/`reconciliationReason` to both interpreters (Green). (4f1c9d4)
- [x] Task: Migration suite asserts the table.
- [x] Task: `sbt scalafmtCheck test`.

## Phase 2: Race classification in handlers

- [x] Task: Failing tests — pre-check contradictions, expired capture,
  post-`None` classification, duplicates (Red). A transient void gateway
  failure stays a DLQ retry and writes no flag. (4f1c9d4)
- [x] Task: Pure `PaymentRace` classifier; wire `OrderCancelledConsumer` and
  `FulfilmentEventConsumer` (Green). (4f1c9d4)
- [x] Task: Store-level `IO.both(recordVoid, recordCapture)` race test.
- [x] Task: Barrier-based concurrent handler test with a latched gateway:
  both observe `Authorized`, exactly one terminal outcome/outbox, loser flag,
  no DLQ. (4f1c9d4)
- [x] Task: `sbt scalafmtCheck test`.
- [x] Task: Conductor - User Manual Verification 'Payment race reconciliation'
  (Protocol in workflow.md)

## Review

- Reviewed 2026-11-02: plan compliance yes; style pass; tests cover the pure
  classifier, store-level void/capture race, pre-check contradictions,
  expired-capture flag and duplicates. Full suite 233 passed. Transient void
  gateway errors intentionally stay on the DLQ path (no false reconciliation
  row). No blocking findings.
