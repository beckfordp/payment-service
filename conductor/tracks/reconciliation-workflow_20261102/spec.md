# Specification: ADM-4 — reconciliation workflow

## Overview

payment-service records first-wins manual-reconciliation flags with no API or
resolve action. This track adds payment-service's open/resolved queue and
operator resolve, per ADR 0026 and the wire contract in
`gluon/docs/system-design.md`.

## Functional Requirements

- `V12__extend_payment_reconciliation.sql`: add `status text not null default
  'open'`, `operator_id`, `note`, `resolved_at`; named status + resolution
  consistency checks; index `(status, created_at desc)`.
- `domain/Reconciliation.scala`: status ADT/parse, row, pure resolve rule.
- `PaymentStore`: `listReconciliations(status, orderId)` newest first;
  `resolveReconciliation` — conditional `UPDATE ... WHERE status='open'` in
  one transaction plus a same-session read → `Resolved | AlreadyResolved |
  NotFound`; first resolver wins, repeats are idempotent and never overwrite.
  Increment OTel counter `reconciliation.flags.recorded` (attribute `reason`)
  only on real inserts, after commit; metric failures never fail a consumer.
- `ReconciliationRoutes.scala`: `GET /reconciliations?status=&orderId=`
  (blank = absent, unknown status 400, unknown orderId empty); `PATCH
  /reconciliations/{orderId}` requiring the dev operator token (401 absent/
  invalid), body only `resolved` (400 otherwise), 404 unknown, 200 row.
- Copy `OperatorAuth` with `requireOperator`; config `operator-auth`
  (`OPERATOR_TOKEN`/`OPERATOR_ID`, defaults `dev-operator-token`/
  `dev-operator`); startup warning. Add `Authorization` to CORS allowed
  headers. Register routes in `AppRoutes`.
- Update every anonymous `PaymentStore` test stub, `PaymentDocsSuite`
  statuses, `MigrationsSuite`, config suite, `scripts/verify-rest-docs.sh`.

## Non-Functional
- TDD; `sbt scalafmtCheck test` green; coverage >80% on new code.
- No Kafka/registry changes.

## Out of Scope
- gshop UI (ADM-7); live demo (ADM-9); reopening resolved flags.
