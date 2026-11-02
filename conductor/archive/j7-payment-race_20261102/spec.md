# Specification: Checkout Journey 7 — payment race reconciliation

## Overview

`OrderCancelled`/`DispatchFailed` (void) and `DispatchConfirmed` (capture)
both target an `authorized` payment. The conditional update already makes
exactly one win (US-21.2), but a loser currently raises `UnrecordedTransition`
and dead-letters: it retries a call that can never succeed and records no
manual-reconciliation flag. This track classifies losers after the conditional
update, records `payment_reconciliation`, and treats capture-after-void / void-
after-capture as committed no-ops. US-9.6 is satisfied by these **terminal**
loser flags; transient gateway errors stay on the DLQ retry path and are not
flagged (a retryable failure would otherwise leave a false reconciliation row).

## Functional Requirements

- `payment_reconciliation(order_id text primary key references payment(order_id) on delete cascade, reason text not null, created_at timestamptz)`; store `flagReconciliation` / `reconciliationReason`, first-wins.
- Pure `PaymentRace.loserReason(target, current)` returns the flag reason for contradictory terminal states; duplicate same-target and benign failed cases return no flag.
- Handlers:
  - pre-check `Captured` on a void path → flag `void_after_capture`, log, commit;
  - pre-check `Voided` on capture → flag `capture_after_void`, log, commit;
  - pre-check `Failed` on capture → flag `capture_after_failed`, log, commit;
  - expired authorization on capture → flag `dispatch_after_authorization_expired`, log, commit (no dead-end DLQ);
  - a failed `gateway.void` call is retried via the existing DLQ path and is
    **not** flagged (transient; US-9.6 applies to terminal losers only);
  - `recordVoid`/`recordCapture`/`recordCaptureFailure` returning `None` → re-read the payment and classify: duplicate/benign → log; contradictory → flag + commit; unknown/live-but-unrecorded → keep `UnrecordedTransition`/DLQ.
- Void/capture gateway calls stay outside the transaction; the conditional update remains the arbiter.
- No Kafka contract changes.

## Acceptance Criteria

- Store-level `IO.both(recordVoid, recordCapture)` yields exactly one winner, one outbox row, one reconciliation row.
- Barrier-based concurrent handlers (both observe `Authorized`) prove one terminal outcome and a flagged loser, with no DLQ.
- Pre-check contradiction, duplicate and expired-authorization paths are covered.
- A failing void gateway still retries via the DLQ (no reconciliation row).
- Postgres migration/store suite and `sbt scalafmtCheck test` green.

## Out of Scope

Order-service reconciliation (done), fulfilment/inventory flags (their
tracks), real gateway idempotency (recorded as TD).
