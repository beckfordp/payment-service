# Implementation Plan: US-6.1 consume order.reserved, charge, publish payment.settled/payment.failed

## Phase 1: Tech stack & infra [checkpoint: a5efb25]

- [x] Task: Update `tech-stack.md` documenting fs2-kafka 3.6.0 + testcontainers-scala-kafka 0.43.6 as new dependencies (workflow.md requires this before implementation) `9174865`
- [x] Task: Add `fs2-kafka`, `testcontainers-scala-kafka` to `build.sbt` `6d6d677`
- [x] Task: Add a `kafka` service to `docker-compose.yml` (single-node KRaft broker, `apache/kafka:3.8.0`, mirroring inventory-service's block exactly) `8814c05`
- [x] Task: Add `kafka { bootstrap-servers = "localhost:9092" }` to `application.conf` + matching `KafkaConfig`/`PaymentServiceConfig` field, mirroring inventory-service's shape `8814c05`
- [x] Task: `sbt compile` confirms the new dependencies resolve cleanly `6d6d677`
- [x] Task: Conductor - User Manual Verification 'Tech stack & infra' (Protocol in workflow.md) — `docker compose up -d` brings up postgres+kafka together, both healthy `a5efb25`

## Phase 2: Event payloads + publisher [checkpoint: 56238c0]

- [x] Task: Write a failing integration test (Testcontainers Kafka) asserting a publish call produces exactly one JSON message with the correct fields on the right topic `271d554`
- [x] Task: Define `PaymentSettledEvent(orderId, paymentId, amountCents, timestamp)` / `PaymentFailedEvent` (same shape) case classes + circe codecs; pin this shape in `gluon/docs/system-design.md`'s payload-contracts section (cross-repo doc, not just this repo) `271d554` (gluon docs: `774df58`, bundled into a concurrent session's commit — content verified intact)
- [x] Task: Implement `PaymentEventPublisher` (fs2-kafka `KafkaProducer`-backed) with `publishSettled`/`publishFailed`, keyed by `orderId`, each publish wrapped in a hand-rolled cats-retry bounded retry (NOT `purerest.resilience` — confirmed `Client[F]`-only) that logs loudly and drops on exhaustion; add a `noOp` instance for tests that don't care about Kafka `271d554`. **Deviation discovered during implementation:** also had to set the Kafka producer's own `max.block.ms`/`request.timeout.ms`/`delivery.timeout.ms` to `publishTimeout` — the client's 60s defaults block far longer than a cats-effect `.timeout()` can actually interrupt, so without this the bounded-retry test against an unreachable broker took 30s+ per attempt instead of ~2s
- [x] Task: Run tests, confirm green — 66 passed, 0 failed `271d554`
- [x] Task: Conductor - User Manual Verification 'Event payloads + publisher' (Protocol in workflow.md) `56238c0`

## Phase 3: order.reserved consumer (the actual US-6.1 behavior) [checkpoint: 364b11d]

- [x] Task: Add local `OrderReservedEvent(orderId, customerId, totalCents, timestamp)` case class mirroring order-service's pinned `order.reserved` payload, no shared library `42b5543`
- [x] Task: Write a failing Testcontainers-Kafka test: publish a synthetic `order.reserved` event, assert a `Payment` is created and settled, and `payment.settled` is published with the correct `orderId`/`paymentId`/`amountCents` `42b5543`
- [x] Task: Implement `OrderReservedConsumer.run[F]`: subscribes to `order.reserved` (group `payment-service-order-reserved`, `AutoOffsetReset.Earliest`); decode failure → log + commit (skip, no retry on consume side, matching `StockEventConsumer` precedent); success → `PaymentStore.create`, simulate charge (always succeeds this track) via `PaymentStore.update(_, Settled)`, publish `payment.settled`, commit offset regardless of outcome `42b5543`
- [x] Task: Wire the consumer as a backgrounded fiber in `Main.scala` (mirroring order-service's `Main.scala` wiring), passing the real `PaymentEventPublisher` `42b5543`
- [x] Task: Add tests — malformed payload is logged and skipped without crashing the stream; `PaymentEventPublisher.publishFailed` covered directly (forcing the failure path, since the real flow never produces it this track); a publish failure that exhausts the bounded retry is logged loudly and doesn't crash the consumer (tested against a producer that always fails) `42b5543` (the latter two already covered by Phase 2's `PaymentEventPublisherSuite`)
- [x] Task: Run tests, confirm green — 68 passed, 0 failed `42b5543`
- [x] Task: Verify coverage (`sbt coverage test coverageReport`, target >80% on new code) — 88.03% statement / 88.68% branch overall
- [x] Task: Conductor - User Manual Verification 'order.reserved consumer' (final, Protocol in workflow.md) `364b11d`
- [x] Task (unplanned, found during manual verification): fix a null-Kafka-key crash in `OrderReservedConsumer` — a bare `kafka-console-producer.sh` call (no key set) killed the whole background consumer stream silently; fixed via fs2-kafka's null-safe `Deserializer.option` for both key and value, with a new test covering it `b715893`

Three phases, each independently testable before the next builds on it —
Phase 1 is pure infra addition, Phase 2 is the publish side in isolation
(testable without a real consumer), Phase 3 is the consumer wiring
everything together.
