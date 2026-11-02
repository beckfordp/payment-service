# Specification: ADM-5 — authorization expiry trigger

## Overview

The admin console must demonstrate the US-20.1 expiry sweep without SQL or a
7-day wait. Add a dev/demo operator trigger that expires one order's
authorization through the real sweep path.

## Functional Requirements

- `PaymentStore.expireAuthorizationNow(orderId)`: conditional `UPDATE ... SET
  expires_at = now() WHERE status = 'authorized'`; `None` otherwise, nothing
  written. In-memory and Postgres.
- `POST /payments/authorizations/{orderId}/expire` (`ExpiryTriggerRoutes`):
  requires the dev operator token (ADR 0024; 401 absent/invalid, and always
  401 when no token is configured); pulls expiry forward then runs one
  `ExpirySweep.sweepOnce` tick, so the void and `PaymentAuthorizationExpired`
  outbox row are the normal sweep path. 200 with the payment, 404 no payment,
  409 not `authorized`.
- Register in `AppRoutes` (sweep batch size from config); docs suite,
  `verify-rest-docs.sh`, README.

## Non-Functional
- TDD; `sbt scalafmtCheckAll test` green.
- No Kafka/registry changes (reuses `PaymentAuthorizationExpired`).

## Out of Scope
- gshop button (ADM-7); live demo (ADM-9); ADR 0022 fault-injection API.
