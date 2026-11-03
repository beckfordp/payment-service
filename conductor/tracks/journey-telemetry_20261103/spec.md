# Spec: Journey telemetry (gluon PERF-4.1–4.3, 4.5, 4.11)

## Overview
gluon PLAN.md Performance 4 for payment-service. Adopts purerestlib/purekafka 0.8.0 and
adds payment business metrics for the Christmas Rush dashboard (Q10, Q15).

## Functional Requirements
1. Bump purerestlib + purekafka to 0.8.0; `Main` passes `runtime.metrics` (PERF-4.1).
2. `PoolMetrics` on both pools: `payment`, `payment-method` (PERF-4.3).
3. `EventAge` in both consumers (fulfilment-events, order-events) (PERF-4.2).
4. `PaymentJourneyMetrics` store decorator (PERF-4.5): `payments.authorize.outcome
   {authorized|declined}`, `payments.capture.outcome{captured|failed}`, `payments.voided`,
   `payments.authorizations.expired` (sweep + operator expiry); `Milestones` lines
   `payment_authorized`, `payment_declined`, `payment_captured`, `payment_capture_failed`,
   `payment_expired` (PERF-4.11).
5. Uncaptured-authorization gauges (Q15): new store query `uncapturedAuthorizations`
   (count + oldest `updated_at` of `authorized` rows) → observable gauges
   `payments.uncaptured.count` and `payments.uncaptured.oldest_age` (s).

## Non-Functional Requirements
- No order ids on labels. Coverage > 80% on new code. scalafmt clean.

## Acceptance Criteria
- Unit tests (decorator, in-memory + Postgres `uncapturedAuthorizations`, gauges).
- Live: metrics port shows the new series after a checkout.

## Out of Scope
- Outbox gauge (PERF-4.9): measured from Debezium's `MilliSecondsBehindSource` instead
  (outbox rows are never marked published). Consumer spans (Perf 5).
