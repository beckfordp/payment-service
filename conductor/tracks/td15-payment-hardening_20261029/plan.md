# TD-15.3 Implementation Plan

## Phase 1 — Typed ids

- [ ] Write failing tests: `PaymentIdSuite`/route malformed-id → 400, store tests using typed ids; confirm failure.
- [ ] Implement `domain/PaymentId`, `domain/PaymentMethodId` (opaque UUID, `Either.catchOnly`, tapir codec); update `Payment`, `PaymentMethod`, both stores, routes, authorize flow.
- [ ] Refactor: remove all raw `UUID.fromString`/`Try(...).toOption`; update fixtures/suites.
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Typed ids' (Protocol in workflow.md)

## Phase 2 — Observability and config

- [ ] Write failing tests: unknown-event logging + raw-log redaction; config loads `asyncApiHtmlDir`; confirm failure.
- [ ] Implement logging changes and `asyncApiHtmlDir` config/`AppRoutes`/`Main`/`application.conf`.
- [ ] Refactor: remove `sys.env` and `Map("raw" -> raw)`.
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Observability & config' (Protocol in workflow.md)

## Phase 3 — ScalaCheck and narrower constraints

- [ ] Write failing tests: `munit-scalacheck` + id/amount properties; confirm failure.
- [ ] Implement properties; narrow `Cors` to `Monad`, `HealthRoutes` health to `Applicative`, ready/routes to `Monad`.
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — ScalaCheck & purity' (Protocol in workflow.md)

## Phase 4 — Verification

- [ ] Update `verify-kafka-asyncapi-docs.sh`/`verify-schema-registry-check.sh` if needed; run full suite + coverage + scalafmt.
- [ ] Run `schemaRegistryCheck` and the live verification scripts.
- [ ] Task: Conductor - User Manual Verification 'Phase 4 — Verification' (Protocol in workflow.md)
