# Spec: payment-service review fixes

## Overview
Address all 11 findings from the 2026-10-29 full review of payment-service (4 High / 3 Medium / 4 Low). Baseline: 176 tests green at `fa171a6`.

## Functional Requirements

### High
- **FR-1 Pure lifecycle.** Add `PaymentStatus.transition(from, to): Either[IllegalTransition, PaymentStatus]` in `domain/` (legal: pending→authorized|failed; authorized→captured|voided|failed; terminal: captured, voided, failed). All store status writes conditional (`WHERE status = $expected`); zero rows → typed error. Remove `PATCH`/`PUT /payments/{id}` (status was the only mutable field) — status is server-owned.
- **FR-2 Capture/void atomicity.** Simulated gateway `capture`/`void` idempotent by `authId` (repeat returns first outcome). `recordCapture`/void write returning `None` or failing raises → DLQ retry, never commit-with-warn.
- **FR-3 DLQ on undecodable events.** Undecodable / unknown fulfilment-events go raw to `fulfilment-events-dlq` instead of log-and-commit.
- **FR-4 Authorize cancellation.** `guaranteeCase` around gateway-authorize→persist: on `Canceled`/`Errored` after successful authorize, void the hold.

### Medium
- **FR-5** Replace catch-all over sealed `PaymentStatus` in `AuthorizeRoutes` with exhaustive match.
- **FR-6** Authorize replay of expired authorization is not 200-success (re-authorize or explicit expired response); capture SQL guards `expires_at > now()`.
- **FR-7** `POST /payments`: duplicate `orderId` → 409 (ON CONFLICT + in-memory uniqueness); `amountCents <= 0` → 400.

### Low
- **FR-8** Typed `OrderId` for idempotency key/outbox/Kafka key; `AuthorizePaymentRequest.paymentMethodId` uses typed id (wire format unchanged).
- **FR-9** Fix stale docs: `PaymentEvents.scala:12-13`, `build.sbt:104`, `conductor/product.md` (add Voided, PATCH/PUT removal), README, verify script.
- **FR-10** Tests for: post-void DispatchConfirmed; recordCapture None; replay captured/voided; duplicate POST; MigrationsSuite inserts `voided`; illegal transitions (ScalaCheck over transition table).
- **FR-11** Narrow effect bounds: `DbTimer` → `Sync`; envelope builders → `Clock`+`UUIDGen`.

## Non-Functional
- TDD per workflow; >80% coverage; scalafmtCheck green; no wire-contract change to Kafka payloads/AsyncAPI.

## Acceptance Criteria
- Each finding has a failing-first test now green.
- Full suite green; registry/AsyncAPI checks green; live verify scripts green on local cluster.
- PATCH/PUT /payments/{id} return 404/405.

## Out of Scope
- Real payment provider (backlog ADR); Journey 5 expiry sweep; pending-intent rows; Redis idempotency (US-6.2 closed).
