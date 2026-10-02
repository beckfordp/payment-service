# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- US-6.1: consume order.created, charge, publish payment.settled / payment.failed — unblocked 2026-10-02: the `order.created` gap (no producer, no pinned payload) is resolved, see `gluon/docs/system-design.md`'s "Design proposal to fill gaps" and Kafka payload contracts. order-service publishes it once an order's stock is fully `Reserved`, not at raw checkout — payload is `{orderId, customerId, totalCents, timestamp}`. order-service's own publishing side isn't built yet either, so this can proceed in parallel against synthetic events per the usual stub/fan-out pattern, same as US-5's split
- US-6.2: Redis idempotency keys to avoid double-charging on retry/redelivery
- ADR: payment provider — real integration (Stripe etc.) vs. simulated

---
