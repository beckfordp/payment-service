# Spec — purekafka adoption + AsyncAPI docs for payment-service (TD-2.4, ADR 0010)

## Overview
payment-service's `OrderReservedConsumer` is a hand-rolled fs2-kafka stream
with no crash recovery or dead-letter routing — the same class of risk
order-service's `StockEventConsumer`/`PaymentEventConsumer` had before
adopting `purekafka` (`purekafka-adoption_20261016`). This track: (1)
restructures `Main.scala` to the same `Telemetry`/`AppRoutes`/`KafkaConsumers`
shape order-service uses (needed scaffolding for the supervised-consumer
wiring, and the same refactor inventory-service's `asyncapi-docs_20261024`
track just applied), (2) adopts `purekafka`'s `StreamSupervisor`
(restart-with-backoff) and `DeadLetter` (bounded-retry-then-DLQ) on
`OrderReservedConsumer` — closing TD-2.4's payment-service gap, (3) wires
AsyncAPI documentation for both the consumed `order-events` (`OrderReserved`)
and published `payment-events` (`PaymentSettled`, `Publish(Cdc)`) channels —
closing the payment-service portion of `gluon/PLAN.md` Rework 3's "Adopt
across..." bullet.

## Functional Requirements
- **Main.scala restructure**: new `Telemetry.scala` (tracer/meter/logger
  bundle) and `AppRoutes.scala` (`def all[F[_]: Async](store, telemetry,
  channels): HttpRoutes[F]`), mirroring order-service's/inventory-service's
  own. `Main.scala` becomes a flat `Resource` for-comprehension. No behavior
  change — verified by the existing test suite staying green.
- **KafkaConsumers.scala** (new): `resource[F[_]: Async](config, store,
  logger, meter): Resource[F, Unit]` — wraps `OrderReservedConsumer.run` in
  `StreamSupervisor.supervise`, backgrounds it, mirroring order-service's
  `KafkaConsumers.scala` (single-consumer version, no `.merge`).
- **Config**: `KafkaConfig` gains `dlqRetry`/`dlqPublishResilience`/
  `streamSupervisor` fields with defaults (mirroring
  `OrderServiceConfig.KafkaConfig` exactly — same types, same default
  values), plus explicit `application.conf` keys (`dlq-retry`,
  `dlq-publish-resilience`, `stream-supervisor`) since `ConfigReader.derived`
  doesn't fall back to Scala defaults when a HOCON key is missing.
- **OrderReservedConsumer**: gains a `deadLetter: DeadLetter[F, String,
  String]` parameter; `handleEvent(...)` wrapped in `deadLetter.guard(topic,
  key, raw)(...)`. Decode failures (null value, envelope decode, payload
  decode) stay logged-and-dropped, unchanged — only the processing step gets
  bounded-retry-then-DLQ. Matches order-service's established convention
  exactly.
- **AsyncApiChannels.scala** (new): registers `OrderReservedEvent` on
  `order-events` as `Subscribe`, `PaymentSettledEvent` on `payment-events` as
  `Publish(Cdc)`. **Not** `PaymentFailedEvent` — confirmed via
  `PaymentStore.update`'s outbox branch that it only ever fires `if status ==
  PaymentStatus.Settled`; a transition to `Failed` writes no outbox event
  today, so `PaymentFailedEvent` (defined in `PaymentEvents.scala`) is never
  actually published. Registering it would misdocument the system. Requires
  adding tapir `Schema[A]` derivation to `OrderReservedEvent`
  (`OrderReservedConsumer.scala`) and `PaymentSettledEvent`
  (`PaymentEvents.scala`).
- **sbt-purekafka-docs wiring**: `project/plugins.sbt` + `build.sbt`
  (`PureKafkaDocsPlugin`, `asyncApiChannelsClass`/`asyncApiTitle`/
  `asyncApiVersion`), `AppRoutes.scala` serves raw spec
  (`/docs/asyncapi.json`) + static HTML (`/docs/asyncapi`), same
  route-ordering fix as inventory-service/order-service.
- **Verify script**: adapt `verify-kafka-asyncapi-docs.sh` from
  inventory-service/order-service (own port).
- **README**: "View Kafka AsyncAPI docs" section, same style as the other two
  services.

## Non-Functional Requirements
- Bump `io.github.beckfordp:purekafka` to 0.2.1 (matching
  order-service/inventory-service's pin).
- No change to `OrderReservedConsumer`'s actual business logic (payment
  creation/settlement) or the outbox/Debezium publish mechanism, including
  the `PaymentFailedEvent`-never-publishes gap — left as a separate,
  unscoped issue.
- DLQ guard scope matches order-service's established convention exactly.

## Acceptance Criteria
- `sbt test` green, `scalafmtCheck` clean, no regressions from the Main.scala
  restructure.
- A Testcontainers-Kafka test proves: (a) a crashing synthetic scenario
  triggers `StreamSupervisor` restart-with-backoff (mirroring order-service's
  `KafkaConsumersSuite`), (b) a permanently-failing message is routed to
  `order-events-dlq` via `DeadLetter` and the stream keeps consuming later
  messages (mirroring order-service's `KafkaConsumersSuite` pattern).
- `sbt compile` produces `target/asyncapi-docs-spec/asyncapi.json` +
  `target/asyncapi-docs/index.html` with both event types (`OrderReserved`,
  `PaymentSettled`).
- `./scripts/verify-kafka-asyncapi-docs.sh` passes end-to-end (build-time
  generation + live raw-spec + live static HTML).

## Out of Scope
- US-6.2 (Redis idempotency keys) — separate backlog item, different concern
  (duplicate-delivery handling, not resilience/observability).
- Real payment provider integration (ADR still open).
- Fixing `PaymentFailedEvent`'s never-publishes gap — separate, unscoped
  issue found during this track's investigation, not this track's job to fix.
- notification-service's own TD-2.3/TD-2.4/AsyncAPI adoption — separate
  track, separate repo.
