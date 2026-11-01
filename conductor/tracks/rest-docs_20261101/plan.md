# Plan — payment-service: REST docs convention (gluon TD-20.4)

## Phase 1: purerestlib 0.7.3
- [x] Task: bump `purerestlibVersion` 0.4.0 → 0.7.3 (+ aligned pins); fix API drift until suite green; sync `conductor/tech-stack.md` — no drift, 207/207 75a53e8
- [x] Task: Conductor - User Manual Verification 'Phase 1: purerestlib 0.7.3' (Protocol in workflow.md) — covered by suite

## Phase 2: Coverage test and per-endpoint error outputs
- [~] Task: `PaymentDocsSuite` — `Docs.undocumented` over the 7 endpoints passed to `Docs.routes` (red first, left uncommitted until the Appendix is approved) + assert each endpoint's error statuses (FR2); TD-16.1 already narrowed them to one `oneOf` per endpoint, so expect no route change — red: 64 gaps; error-status assertions green (400/409, 404, 404, 400, 400/402/404/409/500, none, 503), no route change needed
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Coverage test and per-endpoint error outputs' (Protocol in workflow.md)

## Phase 3: Descriptions and examples (after Appendix approval)
- [ ] Task: coverage test first (`Docs.undocumented == Nil`, red) → apply the approved summaries/descriptions/tags, `id` parameter description+example, DTO/Address/error annotations + examples per spec Appendix; green
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Descriptions and examples' (Protocol in workflow.md) — covered by Phase 4's verify script

## Phase 4: Smoke test
- [ ] Task: `scripts/verify-rest-docs.sh` adapted from order-service — run the service, fetch `/docs/docs.yaml`, assert summaries/descriptions/examples + narrowed errors; scalafmtCheck; CI green
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Smoke test' (Protocol in workflow.md) — `scripts/verify-rest-docs.sh` PASS
