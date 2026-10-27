# Plan: TD-10.1 — idempotent authorize per order

## Phase 1: Store + schema

- [ ] Task: Add failing store tests (in-memory + Postgres): `findByOrderId` finds the stored row; a second `recordAuthorization`/`recordDecline` for the same order returns `None` and leaves exactly one row and one outbox event (Red)
- [ ] Task: Add migration `V8__unique_payment_order_id.sql` (`UNIQUE (order_id)`) and update `MigrationsSuite` expectations (Green)
- [ ] Task: Add `PaymentStore.findByOrderId`; change `recordAuthorization`/`recordDecline` to `F[Option[Payment]]` using `INSERT ... ON CONFLICT (order_id) DO NOTHING` in Postgres and an existing-order check in memory (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Store + schema' (Protocol in workflow.md)

## Phase 2: Route replay + conflict handling

- [ ] Task: Add failing `AuthorizeRoutes` tests: identical second call returns the same `authId` with the gateway called once and one outbox row; replay after decline returns the same 402 reason; different amount → 409; different method → 409; insert-conflict path voids the new hold and replays the winner; missing row after conflict → 500 (Red)
- [ ] Task: Implement pre-check/replay in `AuthorizeRoutes` plus the insert-conflict compensation path; add the tapir `409` error variant (Green)
- [ ] Task: Refactor, full suite green
- [ ] Task: Conductor - User Manual Verification 'Route replay + conflict handling' (Protocol in workflow.md)

## Phase 3: Contract + live verification

- [ ] Task: Update `gluon/docs/system-design.md`'s `POST /payments/authorize` contract with the `409` and replay semantics
- [ ] Task: Rebuild/redeploy payment-service; add `scripts/verify-authorize-idempotency.sh` — same orderId twice → same `authId`, one payment row, one `PaymentAuthorized` outbox row; mismatch → 409; decline replay → same 402
- [ ] Task: Conductor - User Manual Verification 'Contract + live verification' (Protocol in workflow.md)

## Phase 4: Companion order-service change (TD-10.2)

- [ ] Task: Restore `payment-client` retries (`max-retries = 3`) and its config tests; small separate track/commit in order-service

## Risks / notes

- Full `UNIQUE(order_id)` is safe locally (14 rows, 14 distinct order ids) and for fresh/CI DBs; if any environment has legacy duplicates the migration will fail loudly rather than delete history.
- `Captured`/`Pending` rows for an orderId replay as `409` — capture idempotency is US-19.1, not this track.
- A replay publishes no second `PaymentAuthorized`, proven by the outbox row count.
- The gateway `void` on a losing concurrent hold is best-effort: a failed void is logged and the replay continues (that hold then lapses via the expiry sweep).
