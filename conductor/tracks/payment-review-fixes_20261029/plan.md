# Payment review fixes — Implementation Plan

## Phase 1 — Pure lifecycle and server-owned status (FR-1, FR-5)

- [ ] Write failing tests: `PaymentStatusTransitionSuite` (table + ScalaCheck: every illegal pair rejected, terminals absorbing); store conditional-update returns typed error on status mismatch (in-memory + Postgres); PATCH/PUT /payments/{id} → 404/405; confirm failure.
- [ ] Implement `domain/PaymentStatus.transition` + `IllegalTransition`; `PaymentStore` status writes take `expected` (`WHERE status = $expected`); remove `UpdatePaymentRequest`, PATCH/PUT endpoints, `store.update`; void path uses transition.
- [ ] Refactor: exhaustive match over `PaymentStatus` in `AuthorizeRoutes` (no catch-all); update fixtures.
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Pure lifecycle' (Protocol in workflow.md)

## Phase 2 — Gateway atomicity and cancellation (FR-2, FR-4)

- [ ] Write failing tests: simulated gateway capture/void idempotent by authId; recordCapture `None` / write failure raises (handler fails → DLQ path) for capture and void; repeat delivery after DB failure captures once; authorize cancelled after gateway success voids hold (`TestControl`/`Deferred` cancel); confirm failure.
- [ ] Implement idempotent simulated gateway (`Ref[Map[authId, outcome]]`); raise typed error on `None`/failure in `FulfilmentEventConsumer`; `guaranteeCase` void in `AuthorizeRoutes`.
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Gateway atomicity' (Protocol in workflow.md)

## Phase 3 — Event DLQ, expiry, create validation (FR-3, FR-6, FR-7)

- [ ] Write failing tests: undecodable + unknown fulfilment-event → raw to DLQ, offset committed after DLQ; expired authorization replay not 200; capture after `expires_at` rejected by SQL guard; duplicate POST /payments → 409 (Postgres + in-memory); amountCents <= 0 → 400; confirm failure.
- [ ] Implement DLQ-on-decode-failure; replay expiry check + `expires_at > now()` guard; `ON CONFLICT` + 409 errorOut + amount validation + in-memory uniqueness.
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — DLQ, expiry, validation' (Protocol in workflow.md)

## Phase 4 — Low-severity cleanup (FR-8, FR-9, FR-10, FR-11)

- [ ] Write failing tests: `OrderId` parse/roundtrip; remaining gaps — post-void DispatchConfirmed, replay captured/voided, MigrationsSuite inserts `voided`; confirm failure.
- [ ] Implement typed `OrderId` (idempotency key, outbox, Kafka key) and typed `paymentMethodId` in `AuthorizePaymentRequest` (wire unchanged); narrow `DbTimer` to `Sync`, envelope builders to `Clock`+`UUIDGen`.
- [ ] Docs: `PaymentEvents.scala`, `build.sbt:104`, `conductor/product.md` (Voided, PATCH/PUT removed), README, `verify-domain-foundations.sh`.
- [ ] Task: Conductor - User Manual Verification 'Phase 4 — Cleanup' (Protocol in workflow.md)

## Phase 5 — Verification

- [ ] Full suite + coverage (>80%) + scalafmtCheck; `schemaRegistryCheck`; AsyncAPI check.
- [ ] Live verify scripts on local cluster (re-create Debezium register Job first if OrbStack restarted).
- [ ] Task: Conductor - User Manual Verification 'Phase 5 — Verification' (Protocol in workflow.md)

**Risks:** removing PATCH/PUT breaks `verify-domain-foundations.sh` (updated Phase 4); conditional SQL changes rows-affected semantics in outbox transactions; cold-build AsyncAPI plugin race (rerun once).
