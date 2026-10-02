# Implementation Plan: Harden Payment.status to PaymentStatus ADT

## Phase 1: Harden Payment.status to PaymentStatus ADT

- [x] Task: Add `PaymentStatus` sealed trait (Pending/Settled/Failed) with `fromString`/`asString` + unit tests `abf1c52`
- [ ] Task: Add `InvalidStatus(raw)` case to `PaymentError`; wire a new errorOut mapping in `PaymentRoutes` (alongside the existing `notFoundOutput`) — combined with the two tasks below into one commit (see note at bottom): can't compile/test independently once PaymentStore's signature changes
- [x] Task: Add Flyway migration `V2__add_payment_status_check.sql` (CHECK constraint on status); verify it applies cleanly on top of V1 `631fde5`
- [ ] Task: Add Skunk `paymentStatus` eimap codec; update `Payment`, `PaymentStore` (in-memory + postgres) to use `PaymentStatus` instead of raw String
- [ ] Task: Update `PaymentRoutes` PATCH/PUT to parse the request's status via `PaymentStatus.fromString`, short-circuiting invalid values to `InvalidStatus`; `PaymentResponse` keeps serializing status as a String via `asString`
- [ ] Task: Add/extend tests — invalid-status PATCH/PUT returns 400 with nothing persisted; valid-status PATCH/PUT behaves exactly as before; `PaymentStatus.fromString`/`asString` round-trips
- [ ] Task: Conductor - User Manual Verification 'Harden Payment.status to PaymentStatus ADT' (Protocol in workflow.md)

Each task follows the standard TDD lifecycle from workflow.md (failing test
→ implement → refactor → commit → git note) during `/conductor:implement`.
Kept as a single phase since this is one cohesive unit of work.
