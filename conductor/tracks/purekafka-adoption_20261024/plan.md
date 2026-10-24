# Plan — purekafka adoption + AsyncAPI docs for payment-service (TD-2.4, ADR 0010)

## Phase 1: Main.scala restructure (Telemetry + AppRoutes) [checkpoint: 175c847]
- [x] Task: New `Telemetry.scala` (tracer/meter/logger bundle), mirroring
      order-service's/inventory-service's exactly `2bb4275`
- [x] Task: New `AppRoutes.scala` — `def all[F[_]: Async](store,
      telemetry): HttpRoutes[F]`, moving the existing docsRoutes/CORS/
      tracing/metrics assembly out of `Main.scala` verbatim (no AsyncAPI
      routes yet — added in Phase 3) `2bb4275`
- [x] Task: Rewrite `Main.scala` as a flat `Resource` for-comprehension
      calling `AppRoutes.all` (consumer wiring stays inline for now —
      extracted into `KafkaConsumers` in Phase 2) `2bb4275`
- [x] Task: Run full test suite, confirm no regressions (no new tests
      needed — pure refactor, existing suite proves behavior unchanged) —
      74/74 passing, scalafmtCheck clean
- [x] Task: Conductor - User Manual Verification 'Phase 1: Main.scala
      restructure' (Protocol in workflow.md) `175c847` — reduced-prompting
      mode: automated-test verification only (74 passed), no interactive
      walkthrough; see git note on `175c847`

## Phase 2: StreamSupervisor + DeadLetter adoption (TD-2.4) [checkpoint: 6160562]
- [x] Task: `KafkaConfig` gains `dlqRetry`/`dlqPublishResilience`/
      `streamSupervisor` fields (mirroring `OrderServiceConfig.KafkaConfig`
      exactly) + `application.conf` keys `10edae9`
- [x] Task: Bump `io.github.beckfordp:purekafka` to 0.2.1 `10edae9`
- [x] Task: `OrderReservedConsumer.run` gains a `deadLetter: DeadLetter[F,
      String, String]` param; wrap `handleEvent(...)` in
      `deadLetter.guard(topic, key, raw)(...)`. Update the 4 existing test
      call sites in `OrderReservedConsumerSuite.scala` with a no-op
      `DeadLetter` (mirroring order-service's `PaymentEventConsumerSuite`'s
      `noOpDeadLetter` helper) `10edae9`
- [x] Task: New `KafkaConsumers.scala` — `resource[F[_]: Async](config,
      store, logger, meter): Resource[F, Unit]`, wrapping
      `OrderReservedConsumer.run` in `StreamSupervisor.supervise` +
      backgrounding (single-consumer version of order-service's, no
      `.merge`) `10edae9`
- [x] Task: New `KafkaConsumersSuite.scala` `10edae9` — deviation: only the
      DLQ-routing test was adapted, not a "StreamSupervisor restarts on a
      synthetic crash" test. payment-service has one consumer, not two, so
      there's no merge-isolation property to prove (order-service's
      equivalent synthetic-crash test specifically proves that merging two
      individually-supervised streams isolates a crash in one from the
      other — not applicable here). `StreamSupervisor`'s generic
      restart-with-backoff behavior is already proven by `purekafka`'s own
      `StreamSupervisorSuite`; re-proving it again through this thin
      wrapper would add no wiring-specific value. See the suite's own doc
      comment.
- [x] Task: Wire `KafkaConsumers.resource` into `Main.scala` `10edae9`
- [x] Task: Conductor - User Manual Verification 'Phase 2: StreamSupervisor
      + DeadLetter adoption' (Protocol in workflow.md) `6160562` —
      reduced-prompting mode: automated-test verification only (75 passed,
      including a real Testcontainers-Kafka DLQ-routing test), no
      interactive walkthrough; see git note on `6160562`

## Phase 3: AsyncAPI docs
- [ ] Task: Add tapir `Schema.derived` to `OrderReservedEvent`
      (`OrderReservedConsumer.scala`) and `PaymentSettledEvent`
      (`PaymentEvents.scala`) — not `PaymentFailedEvent` (never actually
      published today, confirmed via `PaymentStore.update`'s outbox branch;
      registering it would misdocument the system)
- [ ] Task: New `AsyncApiChannels.scala` — `OrderReservedEvent` on
      `order-events` (`Subscribe`), `PaymentSettledEvent` on
      `payment-events` (`Publish(Cdc)`)
- [ ] Task: `project/plugins.sbt` + `build.sbt` — `sbt-purekafka-docs`/
      `PureKafkaDocsPlugin` wiring, same as inventory-service's track
- [ ] Task: `AppRoutes.scala` gains the raw-spec + static-HTML AsyncAPI
      routes (same route-ordering fix), `channels` param added to
      `AppRoutes.all`
- [ ] Task: Adapt `verify-kafka-asyncapi-docs.sh` (own port)
- [ ] Task: README "View Kafka AsyncAPI docs" section
- [ ] Task: Run full test suite + verify script end-to-end
- [ ] Task: Conductor - User Manual Verification 'Phase 3: AsyncAPI docs'
      (Protocol in workflow.md)
