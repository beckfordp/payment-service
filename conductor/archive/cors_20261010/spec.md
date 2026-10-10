# Spec: Backport CORS support from pure-service-generator (TD-1.3)

## Overview
payment-service wasn't independently browser-confirmed to lack CORS, but it's scaffolded from the same `pure-service-generator` template as the other already-fixed services. Same proven pattern — a direct, no-new-design port. This track's own end-to-end verification doubles as the "confirm affected" step.

## Functional Requirements
1. Add `Cors.scala` (package `paymentservice`) — identical to the proven `Cors.middleware[F[_]: Async]`: allow-all origins, GET/POST/PATCH/PUT/DELETE, `Content-Type` header, credentials disallowed.
2. Wire `Cors.middleware` into `Main.scala`, wrapping the fully-composed routes before `.withHttpApp(routes.orNotFound)`.
3. Add `CorsSuite.scala` — same 3 assertions, against payment-service's own `/payments` route (only bare `POST /payments` exists here — GET/PATCH/PUT/DELETE all require a path id — so the dummy test routes mirror that: bare GET and POST, same shape as the other services' suites, for consistency with the proven test).

## Non-Functional Requirements
- No new dependency.
- No change to payment-service's domain routes/logic.

## Acceptance Criteria
- [ ] `CorsSuite` passes (3 tests), 100% coverage on `Cors.scala`.
- [ ] Full `sbt test` suite green, no regressions.
- [ ] Real verification via `scripts/verify-cors.sh`: `POST /payments` (the only bare route that exists) with `Origin` shows ACAO; OPTIONS preflight shows correct Allow-Methods/-Headers — this also confirms the gap was real.

## Out of Scope
- The CORS policy itself (already decided at the generator level).
- notification-service — final remaining TD-1.3 item, separate track.
