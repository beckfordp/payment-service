# Implementation Plan: US-6.1 consume order.created, charge, publish payment.settled/payment.failed

## Phase 1: Tech stack & infra

- [ ] Task: Update `tech-stack.md` documenting fs2-kafka 3.6.0 + testcontainers-scala-kafka 0.43.6 as new dependencies (workflow.md requires this before implementation)
- [ ] Task: Add `fs2-kafka`, `testcontainers-scala-kafka` to `build.sbt`
- [ ] Task: Add a `kafka` service to `docker-compose.yml` (single-node KRaft broker, `apache/kafka:3.8.0`, mirroring inventory-service's block exactly)
- [ ] Task: Add `kafka { bootstrap-servers = "localhost:9092" }` to `application.conf` + matching `KafkaConfig`/`PaymentServiceConfig` field, mirroring inventory-service's shape
- [ ] Task: `sbt compile` confirms the new dependencies resolve cleanly
- [ ] Task: Conductor - User Manual Verification 'Tech stack & infra' (Protocol in workflow.md) — `docker compose up -d` brings up postgres+kafka together, both healthy

## Phase 2: Event payloads + publisher

- [ ] Task: Write a failing integration test (Testcontainers Kafka) asserting a publish call produces exactly one JSON message with the correct fields on the right topic
- [ ] Task: Define `PaymentSettledEvent(orderId, paymentId, amountCents, timestamp)` / `PaymentFailedEvent` (same shape) case classes + circe codecs; pin this shape in `gluon/docs/system-design.md`'s payload-contracts section (cross-repo doc, not just this repo)
- [ ] Task: Implement `PaymentEventPublisher` (fs2-kafka `KafkaProducer`-backed) with `publishSettled`/`publishFailed`, keyed by `orderId`, each publish wrapped in a hand-rolled cats-retry bounded retry (NOT `purerest.resilience` — confirmed `Client[F]`-only) that logs loudly and drops on exhaustion; add a `noOp` instance for tests that don't care about Kafka
- [ ] Task: Run tests, confirm green
- [ ] Task: Conductor - User Manual Verification 'Event payloads + publisher' (Protocol in workflow.md)

## Phase 3: order.created consumer (the actual US-6.1 behavior)

- [ ] Task: Add local `OrderCreatedEvent(orderId, customerId, totalCents, timestamp)` case class mirroring order-service's pinned `order.created` payload, no shared library
- [ ] Task: Write a failing Testcontainers-Kafka test: publish a synthetic `order.created` event, assert a `Payment` is created and settled, and `payment.settled` is published with the correct `orderId`/`paymentId`/`amountCents`
- [ ] Task: Implement `OrderCreatedConsumer.run[F]`: subscribes to `order.created` (group `payment-service-order-created`, `AutoOffsetReset.Earliest`); decode failure → log + commit (skip, no retry on consume side, matching `StockEventConsumer` precedent); success → `PaymentStore.create`, simulate charge (always succeeds this track) via `PaymentStore.update(_, Settled)`, publish `payment.settled`, commit offset regardless of outcome
- [ ] Task: Wire the consumer as a backgrounded fiber in `Main.scala` (mirroring order-service's `Main.scala` wiring), passing the real `PaymentEventPublisher`
- [ ] Task: Add tests — malformed payload is logged and skipped without crashing the stream; `PaymentEventPublisher.publishFailed` covered directly (forcing the failure path, since the real flow never produces it this track); a publish failure that exhausts the bounded retry is logged loudly and doesn't crash the consumer (tested against a producer that always fails)
- [ ] Task: Run tests, confirm green
- [ ] Task: Verify coverage (`sbt coverage test coverageReport`, target >80% on new code)
- [ ] Task: Conductor - User Manual Verification 'order.created consumer' (final, Protocol in workflow.md)

Three phases, each independently testable before the next builds on it —
Phase 1 is pure infra addition, Phase 2 is the publish side in isolation
(testable without a real consumer), Phase 3 is the consumer wiring
everything together.
