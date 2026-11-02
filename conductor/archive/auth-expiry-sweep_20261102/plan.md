# Plan: US-20.1 + US-9.3 — expiry sweep and cancellation void

## Phase 1: Store expiry operation

- [x] Task: Failing tests — due authorized payment expires + outbox event, live/captured/voided untouched, duplicate tick no-op (Red)
- [x] Task: `expireDueAuthorizations` in both interpreters, V10 partial index (Green)
- [x] Task: Refactor, `sbt scalafmtCheck test` green
- [x] Task: Conductor - User Manual Verification 'Store expiry operation' (Protocol in workflow.md)

## Phase 2: Sweep loop + config

- [x] Task: Failing tests — `sweepOnce` returns/logs the count, per-tick error is swallowed (Red)
- [x] Task: `ExpirySweep`, config keys, Main wiring (Green)
- [x] Task: Conductor - User Manual Verification 'Sweep loop + config' (Protocol in workflow.md)

## Phase 3: OrderCancelled consumer

- [x] Task: Failing handler tests — void authorized, duplicate no-op, captured no-op, malformed/DLQ (Red)
- [x] Task: Consumer + DLQ/supervisor wiring + AsyncAPI channel (Green)
- [x] Task: Conductor - User Manual Verification 'OrderCancelled consumer' (Protocol in workflow.md)

## Risks

- Capture gateway call outside the transaction: a capture that succeeds just
  as the sweep voids it leaves a DLQ'd outcome; US-9.6/21 owns the fix.

## Review (2026-11-02)

- `sbt scalafmtCheck test` green (223 tests); `sbt schemaRegistryCheck` green.
- `scripts/verify-auth-expiry.sh` passed live: payment voided
  (`authorization_expired`), one `PaymentAuthorizationExpired` outbox row,
  no duplicate on a second tick; the OrderCancelled void path also ran.
- No blocking findings. Capture-vs-expiry gateway race remains US-9.6/21.
