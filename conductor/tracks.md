# Project Tracks

This file tracks all major tracks for the project.

- [x] **Track: Transactional outbox for payment-service (TD-3.4, ADR 0008)**
  *Link: [./tracks/outbox_20261012/](./tracks/outbox_20261012/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- US-6.2: Redis idempotency keys to avoid double-charging on retry/redelivery
- ADR: payment provider — real integration (Stripe etc.) vs. simulated
- TD-2.4 (ADR 0010): `OrderReservedConsumer` adopts `purekafka`'s stream
  supervision (restart-with-backoff on any stream error, unbounded
  attempts — TD-2.1 only fixed its narrower null-key/value crash, not
  general stream resilience) and dead-letter routing (a message that
  still fails after retries goes to `<topic>-dlq` instead of crashing or
  blocking). Not separately named in TD-2.2/TD-2.3's own bullets, but
  covered by TD-2.4's "all four Kafka-consuming services" DLQ scope.
  Requires `io.github.beckfordp:purekafka` published (`sbt publishLocal`
  in the sibling `purerest` checkout) before this can compile against it.

---
