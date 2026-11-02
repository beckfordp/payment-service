# Plan: ADM-4 — reconciliation workflow

## Phase 1: Persistence and domain

- [x] Task: `MigrationsSuite` assertions + `V12` migration (Red → Green).
- [x] Task: `ReconciliationStatus`/`Reconciliation` pure rule + tests.
- [x] Task: Conductor - User Manual Verification 'Persistence'
  (Protocol in workflow.md) — prompting off: committed suites.

## Phase 2: Store

- [x] Task: Failing in-memory/Postgres list + resolve tests (winner, repeat,
  unknown); implement both interpreters and the insert-only counter.
- [x] Task: Conductor - User Manual Verification 'Store'
  (Protocol in workflow.md) — prompting off: committed suites.

## Phase 3: Routes and config

- [x] Task: `OperatorAuth` + `operator-auth` config + startup warning.
- [x] Task: Failing routes suite (GET filters, PATCH 200/400/401/404,
  idempotency); add `ReconciliationRoutes`, register in `AppRoutes`, add
  `Authorization` to CORS.
- [x] Task: Update stubs, docs suite, config suite and verify-rest-docs map;
  run `sbt scalafmtCheck test`.
- [x] Task: Conductor - User Manual Verification 'Routes'
  (Protocol in workflow.md) — prompting off: full suite green (238 passed).

## Review

- Reviewed 2026-11-02 (conductor:review): plan compliance yes; style pass;
  first-resolver-wins conditional update, insert-only counter, dev-token gate
  and CORS fixed. Full suite 238 passed. No blocking findings.
