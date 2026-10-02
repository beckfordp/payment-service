# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Generate payment-service + apply field-spec (infra) — ⚠ already done (this
  service was generated and `specs/payment.yaml` applied before this
  `/gluon:add` run); user should prune this line
- Harden `status` from raw String to a real enum {pending, settled, failed} — codegen v1 only supports String/Int/Boolean/Instant, no enum type, so the field-spec used String as a stand-in (infra)
- US-6.1: consume order.created, charge, publish payment.settled / payment.failed
- US-6.2: Redis idempotency keys to avoid double-charging on retry/redelivery
- ADR: payment provider — real integration (Stripe etc.) vs. simulated

---
