# Plan: TD-10.1 — idempotent authorize per order

## Phase 1: Store + schema

- [x] Task: Add failing store tests (in-memory + Postgres): `findByOrderId` finds the stored row; a second `recordAuthorization`/`recordDecline` for the same order returns `None` and leaves exactly one row and one outbox event (Red) `55615f1`
- [x] Task: Add migration `V8__unique_payment_order_id.sql` (`UNIQUE (order_id)`) and update `MigrationsSuite` expectations (Green) `55615f1` (suite also gained a duplicate-order rejection test; per-test order ids made unique)
- [x] Task: Add `PaymentStore.findByOrderId`; change `recordAuthorization`/`recordDecline` to `F[Option[Payment]]` using `INSERT ... ON CONFLICT (order_id) DO NOTHING` in Postgres and an existing-order check in memory (Green) `55615f1` (the generated CRUD `create` keeps its own behavior; only authorize outcomes are conflict-aware)
- [x] Task: Refactor, `sbt scalafmtCheck test` green `55615f1` (149/149 tests)
- [x] Task: Conductor - User Manual Verification 'Store + schema' (Protocol in workflow.md) — full suite green; unique constraint and replay proven in Postgres tests

## Phase 2: Route replay + conflict handling

- [x] Task: Add failing `AuthorizeRoutes` tests: identical second call returns the same `authId` with the gateway called once and one outbox row; replay after decline returns the same 402 reason; different amount → 409; different method → 409; insert-conflict path voids the new hold and replays the winner; missing row after conflict → 500 (Red) `55615f1`
- [x] Task: Implement pre-check/replay in `AuthorizeRoutes` plus the insert-conflict compensation path; add the tapir `409` and `500` error variants (Green) `55615f1`
- [x] Task: Refactor, full suite green `55615f1`
- [x] Task: Conductor - User Manual Verification 'Route replay + conflict handling' (Protocol in workflow.md) — 14 route tests green, including both conflict paths

## Phase 3: Contract + live verification

- [x] Task: Update `gluon/docs/system-design.md`'s `POST /payments/authorize` contract with the `409` and replay semantics (in the gluon repo's working tree; committed with the checkoff)
- [x] Task: Rebuild/redeploy payment-service; add `scripts/verify-authorize-idempotency.sh` — same orderId twice → same `authId`, one payment row, one `PaymentAuthorized` outbox row; mismatch → 409; decline replay → same 402 `55615f1` (live run passed)
- [x] Task: Conductor - User Manual Verification 'Contract + live verification' (Protocol in workflow.md) — live script green against the redeployed cluster

## Phase 4: Companion order-service change (TD-10.2)

- [x] Task: Restore `payment-client` retries (`max-retries = 3`) and its config tests; small separate track/commit in order-service — `79daa50` (`payment-retries_20261027`)

## Risks / notes

- Full `UNIQUE(order_id)` is safe locally (14 rows, 14 distinct order ids) and for fresh/CI DBs; if any environment has legacy duplicates the migration will fail loudly rather than delete history.
- `Captured`/`Pending` rows for an orderId replay as `409` — capture idempotency is US-19.1, not this track.
- A replay publishes no second `PaymentAuthorized`, proven by the outbox row count.
- The gateway `void` on a losing concurrent hold is best-effort: a failed void is logged and the replay continues (that hold then lapses via the expiry sweep).
