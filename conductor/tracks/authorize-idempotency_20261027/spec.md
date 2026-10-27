# Specification: TD-10.1 — idempotent authorize per order

## Overview

`POST /payments/authorize` inserts a fresh payment row on every call. A
retried call whose first attempt landed (response lost, timeout, 5xx after
commit) places a second gateway hold. This track makes authorization
idempotent per `orderId`: replay the stored outcome, reject conflicting
details, and back it with a DB unique constraint. It supersedes US-6.2's
Redis-key mechanism, which was written for the retired async charge path.

## Functional Requirements

- **FR-1 — One payment row per order.** Migration `V8` adds
  `UNIQUE (order_id)` to `payment` (local data verified duplicate-free).
- **FR-2 — Lookup + conflict-aware writes.** `PaymentStore` gains
  `findByOrderId`; `recordAuthorization`/`recordDecline` return
  `Option[Payment]`, `None` when a row for that order already exists (unique
  conflict, nothing written, no outbox row). In-memory and Postgres
  interpreters behave identically.
- **FR-3 — Replay by orderId.** Before calling the gateway, lookup by
  `orderId`; if a row exists and amount + payment method match:
  - `Authorized` → `200` with the stored `authId`/`expiresAt`;
  - `Failed` → `402` with the stored decline reason;
  - any other status (e.g. `Captured`) → `409`.
  A different amount or payment method is always `409`
  (`{"error": "..."}`), never a second hold.
- **FR-4 — Concurrent retries.** On insert conflict (a concurrent call
  won), void this call's fresh gateway hold, re-read the row, and apply the
  FR-3 replay logic. A missing row after a conflict is a `500`.
- **FR-5 — Contract.** `gluon/docs/system-design.md` documents the
  authorize `409` and replay semantics. AsyncAPI/events unchanged; a replay
  publishes no second `PaymentAuthorized` event.
- **FR-6 — Tests.** Route tests (replay authorize, replay decline, amount
  mismatch, method mismatch, gateway called once, one outbox row), store
  tests for `findByOrderId`/conflict, Postgres test proving the unique
  constraint and replayed row, migrations suite updated.

## Non-Functional Requirements

- No new dependencies.
- No DB lock held across the gateway call.
- Existing authorize/decline behavior unchanged for first attempts.
- `sbt scalafmtCheck test` green; live check against the local cluster.

## Acceptance Criteria

- Two identical authorize calls return the same `authId`, create one
  payment row, and publish one `PaymentAuthorized` event.
- Different amount or method on the same `orderId` returns `409` with no new
  hold/row/event.
- A replayed decline returns the same `402` reason.
- Full suite and migrations green; contract doc matches the code.

## Out of Scope

- US-6.2's Redis idempotency keys (superseded).
- Capture idempotency (US-19.1, Journey 4).
- order-service retry restore (TD-10.2, separate companion track in
  order-service).
