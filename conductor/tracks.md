# Project Tracks

This file tracks all major tracks for the project.

- [ ] **Track: US-6.1: consume order.reserved, charge, publish payment.settled / payment.failed**
  *Link: [./tracks/order-reserved-consumer_20261002/](./tracks/order-reserved-consumer_20261002/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- US-6.2: Redis idempotency keys to avoid double-charging on retry/redelivery
- ADR: payment provider — real integration (Stripe etc.) vs. simulated

---
