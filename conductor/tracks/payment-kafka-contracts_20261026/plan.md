# Plan: Rework 7 payment-service — sync-payment-auth Kafka contracts

## Phase 1: Event contracts + consumer retirement (TDD)

- [ ] Task: **Red** — add `AsyncApiChannelsSuite` (payment-service has none; model on
      order-service's): assert exactly four `payment-events` channels — `PaymentAuthorized`,
      `PaymentCaptured`, `PaymentAuthorizationExpired`, `PaymentFailed` — with
      `Publish(PublisherKind.Cdc)`, correct topic, and payload field names/types; assert no
      `OrderReserved`/`PaymentSettled`. Update `PaymentStorePostgresSuite`: a transition to
      `Settled` now writes **no** outbox row (replace the old `PaymentSettled`-row test); keep the
      atomicity tests. Delete `OrderReservedConsumerSuite`. Run `sbt test`; confirm the expected
      failures (missing event types, old channel set, outbox still written).
- [ ] Task: **Green** — rewrite `PaymentEvents.scala` with the four event case classes
      (`PaymentAuthorized(orderId, authId, expiresAt, timestamp)`,
      `PaymentCaptured(orderId, amountCents, timestamp)`,
      `PaymentAuthorizationExpired(orderId, timestamp)`,
      `PaymentFailed(orderId, paymentId, amountCents, reason, timestamp)`), each with `Codec` +
      `Schema`; delete `PaymentSettledEvent` and the local `OrderReservedEvent` mirror. Rewrite
      `AsyncApiChannels.scala` to the four publish channels. Remove the `Settled` outbox-write
      branch from `PaymentStore.update` (and now-unused `EventEnvelope`/JSON imports if nothing
      else uses them). Run `sbt test` green.
- [ ] Task: Retire the consumer — delete `OrderReservedConsumer.scala`, `KafkaConsumers.scala`,
      their remaining tests/config keys and `Main.scala` wiring; remove now-unused purekafka
      consumer/DLQ dependencies only if nothing else needs them (keep the AsyncAPI/schema-registry
      plugin). Run full `sbt test`; remove any config entries (`application.conf`) left unused.
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Event contracts + consumer retirement'
      — prompting is off (`.claude/prompt-off`); verify with `sbt scalafmt test` green and
      `sbt "show Universal/mappings"`/`asyncApiGenerate` producing four channels.

## Phase 2: Docs, scripts, live AsyncAPI + schema registry

- [ ] Task: Update payment-service docs — `README.md` event/consumer sections,
      `conductor/product.md` (remove stale `PaymentEventPublisher`, `OrderReserved` consumption),
      `conductor/tech-stack.md` (consumer/Kafka wiring). Update
      `scripts/verify-kafka-asyncapi-docs.sh` (four new event types, no `OrderReserved`), fix
      `scripts/verify-schema-registry-check.sh` (new artifact list, `gluon` group), delete
      `scripts/verify-order-reserved-consumer.sh`, delete/rewrite the stale
      `scripts/verify-kafka-publisher.sh`.
- [ ] Task: Fix the cross-repo contract docs in gluon — `docs/system-design.md` `PaymentFailed`
      JSON shape gains `reason` (capture-decline only); remove the contradictory “authorize-decline
      also publishes `PaymentFailed`” wording; ensure the service table/topic section says
      payment-service no longer consumes `order-events`.
- [ ] Task: Run `sbt asyncApiGenerate schemaRegistryCheck` against the local Apicurio registry (env
      `SCHEMA_REGISTRY_URL` / existing script setup) — confirm the four new artifacts register
      under group `gluon`; run `./scripts/verify-kafka-asyncapi-docs.sh` end-to-end. Record any
      pre-existing script/group mismatch fixes made along the way.
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Docs, scripts, live AsyncAPI + schema
      registry' — run `sbt test`, `sbt asyncApiGenerate`, `sbt schemaRegistryCheck`,
      `./scripts/verify-kafka-asyncapi-docs.sh`; confirm AsyncAPI JSON contains exactly the four
      new payment event types.

## Phase 3: Commit + track close-out

- [ ] Task: Commit and push payment-service changes (code, tests, docs, scripts) with one-line
      conventional messages; commit the gluon `system-design.md` fix separately.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Commit + track close-out' — confirm
      clean working trees and pushed commits.
