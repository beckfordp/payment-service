# TD-15.3 Implementation Plan

## Phase 1 — Typed ids

- [x] Write failing tests: `PaymentIdSuite`/route malformed-id → 400, store tests using typed ids; confirm failure. — 1fb193d
- [x] Implement `domain/PaymentId`, `domain/PaymentMethodId` (opaque UUID, `Either.catchOnly`, tapir codec); update `Payment`, `PaymentMethod`, both stores, routes, authorize flow. — 1fb193d
- [x] Refactor: remove all raw `UUID.fromString`/`Try(...).toOption`; update fixtures/suites. — 1fb193d
- [x] Task: Conductor - User Manual Verification 'Phase 1 — Typed ids' (Protocol in workflow.md) — covered by the full suite

## Phase 2 — Observability and config

- [x] Write failing tests: unknown-event logging + raw-log redaction; config loads `asyncApiHtmlDir`; confirm failure. — fa171a6
- [x] Implement logging changes and `asyncApiHtmlDir` config/`AppRoutes`/`Main`/`application.conf`. — fa171a6
- [x] Refactor: remove `sys.env` and `Map("raw" -> raw)`. — fa171a6
- [x] Task: Conductor - User Manual Verification 'Phase 2 — Observability & config' (Protocol in workflow.md) — covered by tests

## Phase 3 — ScalaCheck and narrower constraints

- [x] Write failing tests: `munit-scalacheck` + id/amount properties; confirm failure. — fa171a6
- [x] Implement properties; narrow `Cors` to `Monad`, `HealthRoutes` health to `Applicative`, ready/routes to `Monad`. — fa171a6
- [x] Task: Conductor - User Manual Verification 'Phase 3 — ScalaCheck & purity' (Protocol in workflow.md) — covered by tests

## Phase 4 — Verification

- [x] Update `verify-kafka-asyncapi-docs.sh`/`verify-schema-registry-check.sh` if needed; run full suite + coverage + scalafmt. — 176 tests green, 87.5%/85.7% coverage; b4cddc8
- [x] Run `schemaRegistryCheck` and the live verification scripts. — registry green; `verify-kafka-asyncapi-docs.sh` passed
- [x] Task: Conductor - User Manual Verification 'Phase 4 — Verification' (Protocol in workflow.md) — covered by the live script
