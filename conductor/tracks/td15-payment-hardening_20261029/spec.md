# TD-15.3 — Cross-repo consistency hardening (payment-service)

## Overview

Payment-service slice of gluon TD-15: typed ids at the boundary, unknown-event
logging, raw-payload redaction, `asyncApiHtmlDir` config, ScalaCheck, and
narrower effect bounds. No Kafka contract change (payment-service already uses
per-outcome events).

## Functional Requirements

1. **Typed ids** — opaque `PaymentId` and `PaymentMethodId` parsed once at the
   HTTP boundary with `Either.catchOnly`; routes `path[PaymentId]`; stores take
   the parsed types; malformed ids → 400; remove all
   `Try(UUID.fromString).toOption` and bare `UUID.fromString`.
2. **Observability** — unknown Kafka `eventType` logged with topic/type; raw
   payloads redacted (topic/byte length/error class).
3. **Config** — `asyncApiHtmlDir` field with `${?ASYNCAPI_HTML_DIR}`; no
   `sys.env` in `AppRoutes`.
4. **Tests** — `munit-scalacheck` + properties (id parse round-trip, amount
   arithmetic).
5. **Effect constraints** — `Cors` → `Monad`; `HealthRoutes` health →
   `Applicative`, ready/routes → `Monad`; route endpoints narrowed where
   possible.

## Acceptance Criteria

- Malformed payment/payment-method id → 400; no raw UUID parsing in stores.
- Unknown event types logged; raw payloads absent from logs (tests assert).
- Full suite + scalafmt green; `schemaRegistryCheck` unchanged/green.

## Out of Scope

- No event-contract changes; other services (TD-15.4–15.8).
