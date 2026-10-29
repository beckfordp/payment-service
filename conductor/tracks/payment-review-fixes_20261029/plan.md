# Payment review fixes — Implementation Plan

## Phase 1 — Pure lifecycle and server-owned status (FR-1, FR-5) [checkpoint: 036e1fb]

- [x] Write failing tests: `PaymentStatusTransitionSuite` (table + ScalaCheck: every illegal pair rejected, terminals absorbing); store conditional-update returns typed error on status mismatch (in-memory + Postgres); PATCH/PUT /payments/{id} → 404/405; confirm failure. — 036e1fb
- [x] Implement `PaymentStatus.transition` + `IllegalTransition` (kept in `PaymentStatus.scala`, not `domain/`, beside the ADT); `PaymentStore` status writes take `expected` (`WHERE status = $expected`); remove `UpdatePaymentRequest`, PATCH/PUT endpoints, `store.update`; void path uses conditional `recordVoid`. — 036e1fb
- [x] Refactor: exhaustive match over `PaymentStatus` in `AuthorizeRoutes` (no catch-all); update fixtures. — 036e1fb
- [x] Task: Conductor - User Manual Verification 'Phase 1 — Pure lifecycle' (Protocol in workflow.md) — covered by suite (168 green, incl. Postgres testcontainers)

## Phase 2 — Gateway atomicity and cancellation (FR-2, FR-4) [checkpoint: b9aad38]

- [x] Write failing tests: simulated gateway capture/void idempotent by authId; recordCapture `None` / write failure raises (handler fails → DLQ path) for capture and void; repeat delivery after DB failure captures once; authorize cancelled after gateway success voids hold (`TestControl`/`Deferred` cancel); confirm failure. — b9aad38 (`FulfilmentEventHandlerSuite`, broker-free via extracted `handle`)
- [x] Implement idempotent simulated gateway (deviation: simulator outcome is already a pure function of card token, so no `Ref` ledger - contract documented + tested); raise `UnrecordedTransition` on `None` in `FulfilmentEventConsumer`; `guaranteeCase` void in `AuthorizeRoutes`. — b9aad38
- [x] Task: Conductor - User Manual Verification 'Phase 2 — Gateway atomicity' (Protocol in workflow.md) — covered by suite (176 green)

## Phase 3 — Event DLQ, expiry, create validation (FR-3, FR-6, FR-7) [checkpoint: f129927]

- [x] Write failing tests: undecodable + unknown fulfilment-event → raw to DLQ, offset committed after DLQ; expired authorization replay not 200; capture after `expires_at` rejected by SQL guard; duplicate POST /payments → 409 (Postgres + in-memory); amountCents <= 0 → 400; confirm failure. — f129927 (13 red before impl)
- [x] Implement DLQ-on-decode-failure; replay expiry check + `expires_at > now()` guard (plus consumer pre-check: expired → DLQ, no gateway call); `ON CONFLICT` + 409 errorOut + amount validation + in-memory uniqueness. Fixture expiries moved to 2099. — f129927
- [x] Task: Conductor - User Manual Verification 'Phase 3 — DLQ, expiry, validation' (Protocol in workflow.md) — covered by suite (188 green)

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
