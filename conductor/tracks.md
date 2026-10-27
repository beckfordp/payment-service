# Project Tracks

This file tracks all major tracks for the project.

- [x] **Track: Transactional outbox for payment-service (TD-3.4, ADR 0008)**
  *Link: [./tracks/outbox_20261012/](./tracks/outbox_20261012/)*

- [ ] **Track: Checkout Journey 1 payment-service — gateway algebra, Authorized status, payment methods + billing address, POST /payments/authorize (US-12.1, US-13.1, US-13.2, US-14.1, ADR 0011)**
  *Link: [./tracks/checkout-authorize_20261027/](./tracks/checkout-authorize_20261027/)*
---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- US-6.2: Redis idempotency keys to avoid double-charging on retry/redelivery
- ADR: payment provider — real integration (Stripe etc.) vs. simulated

---
