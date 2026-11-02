# Plan: ADM-5 — authorization expiry trigger

## Phase 1: Store

- [x] Task: Failing Postgres test (live auth not due → pulled → swept once;
  repeat/unknown `None`); implement `expireAuthorizationNow` in both
  interpreters; update test stubs.
- [x] Task: Conductor - User Manual Verification 'Store'
  (Protocol in workflow.md) — prompting off: committed suites.

## Phase 2: Route

- [x] Task: Failing `ExpiryTriggerRoutesSuite` (200 voided + one expired
  event, repeat 409, captured 409, unknown 404, 401 missing/wrong/disabled);
  add `ExpiryTriggerRoutes`, register in `AppRoutes`/`Main`.
- [x] Task: Docs suite, `verify-rest-docs.sh`, README; `sbt scalafmtCheckAll
  test`.
- [x] Task: Conductor - User Manual Verification 'Route'
  (Protocol in workflow.md) — prompting off: full suite green.

## Review

- Reviewed 2026-11-02 (conductor:review): plan compliance yes; style pass;
  trigger reuses `ExpirySweep.sweepOnce` (no parallel void path), conditional
  update is the idempotency guard, token-gated and off without a token. Full
  suite 244 passed. No blocking findings.
