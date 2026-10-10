# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- No CORS support (no `Access-Control-Allow-Origin` header) — not
  independently browser-confirmed for this service (unlike catalog-
  /cart-/order-service), but scaffolded from the same
  `pure-service-generator` template, so almost certainly the same gap.
  Fix at the generator first
  (`src/main/g8/src/main/scala/$package$/Main.scala`, wrap `routes`
  with http4s's `org.http4s.server.middleware.CORS` before
  `.orNotFound`), then port here. See `gluon/TECHNICAL_DEBT.md` TD-1 and
  `gluon/backlogs/payment-service.md`.
- US-6.2: Redis idempotency keys to avoid double-charging on retry/redelivery
- ADR: payment provider — real integration (Stripe etc.) vs. simulated

---
