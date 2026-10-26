# Plan: Rework 7 payment-service — sync-payment-auth Kafka contracts

## Phase 1: Event contracts + consumer retirement (TDD)

- [x] Task: **Red** (5e4c8b9) — add `AsyncApiChannelsSuite` (payment-service has none; model on
      order-service's): assert exactly four `payment-events` channels — `PaymentAuthorized`,
      `PaymentCaptured`, `PaymentAuthorizationExpired`, `PaymentFailed` — with
      `Publish(PublisherKind.Cdc)`, correct topic, and payload field names/types; assert no
      `OrderReserved`/`PaymentSettled`. Update `PaymentStorePostgresSuite`: a transition to
      `Settled` now writes **no** outbox row (replace the old `PaymentSettled`-row test); keep the
      atomicity tests. Delete `OrderReservedConsumerSuite`. Run `sbt test`; confirm the expected
      failures (missing event types, old channel set, outbox still written).
- [x] Task: **Green** (5e4c8b9) — rewrite `PaymentEvents.scala` with the four event case classes
      (`PaymentAuthorized(orderId, authId, expiresAt, timestamp)`,
      `PaymentCaptured(orderId, amountCents, timestamp)`,
      `PaymentAuthorizationExpired(orderId, timestamp)`,
      `PaymentFailed(orderId, paymentId, amountCents, reason, timestamp)`), each with `Codec` +
      `Schema`; delete `PaymentSettledEvent` and the local `OrderReservedEvent` mirror. Rewrite
      `AsyncApiChannels.scala` to the four publish channels. Remove the `Settled` outbox-write
      branch from `PaymentStore.update` (and now-unused `EventEnvelope`/JSON imports if nothing
      else uses them). Run `sbt test` green.
- [x] Task: Retire the consumer (5e4c8b9) — delete `OrderReservedConsumer.scala`, `KafkaConsumers.scala`,
      their remaining tests/config keys and `Main.scala` wiring; remove now-unused purekafka
      consumer/DLQ dependencies only if nothing else needs them (keep the AsyncAPI/schema-registry
      plugin). Run full `sbt test`; remove any config entries (`application.conf`) left unused.
- [x] Task: Conductor - User Manual Verification 'Phase 1: Event contracts + consumer retirement' (5e4c8b9)
      — prompting is off (`.claude/prompt-off`); verify with `sbt scalafmt test` green and
      `sbt "show Universal/mappings"`/`asyncApiGenerate` producing four channels.

## Phase 2: Docs, scripts, live AsyncAPI + schema registry

- [x] Task: Update payment-service docs (5e4c8b9) — `README.md` event/consumer sections,
      `conductor/product.md` (remove stale `PaymentEventPublisher`, `OrderReserved` consumption),
      `conductor/tech-stack.md` (consumer/Kafka wiring). Update
      `scripts/verify-kafka-asyncapi-docs.sh` (four new event types, no `OrderReserved`), fix
      `scripts/verify-schema-registry-check.sh` (new artifact list, `gluon` group), delete
      `scripts/verify-order-reserved-consumer.sh`, delete/rewrite the stale
      `scripts/verify-kafka-publisher.sh`.
- [x] Task: Fix the cross-repo contract docs in gluon (4fccab0) — `docs/system-design.md` `PaymentFailed`
      JSON shape gains `reason` (capture-decline only); remove the contradictory “authorize-decline
      also publishes `PaymentFailed`” wording; ensure the service table/topic section says
      payment-service no longer consumes `order-events`.
- [x] Task: Run `sbt asyncApiGenerate schemaRegistryCheck` (5e4c8b9) against the local Apicurio registry (env
      `SCHEMA_REGISTRY_URL` / existing script setup) — confirm the four new artifacts register
      under group `gluon`; run `./scripts/verify-kafka-asyncapi-docs.sh` end-to-end. Record any
      pre-existing script/group mismatch fixes made along the way.
- [x] Task: Conductor - User Manual Verification 'Phase 2: Docs, scripts, live AsyncAPI + schema
      registry' (5e4c8b9) — run `sbt test`, `sbt asyncApiGenerate`, `sbt schemaRegistryCheck`,
      `./scripts/verify-kafka-asyncapi-docs.sh`; confirm AsyncAPI JSON contains exactly the four
      new payment event types.

## Phase 3: Commit + track close-out

- [x] Task: Commit and push payment-service changes (5e4c8b9, 4fccab0) (code, tests, docs, scripts) with one-line
      conventional messages; commit the gluon `system-design.md` fix separately.
- [x] Task: Conductor - User Manual Verification 'Phase 3: Commit + track close-out' (5e4c8b9, 4fccab0) — confirm
      clean working trees and pushed commits.

## Notes

- The shared `gluon` registry group already held `payment-events-PaymentFailed` v1
  (old shape, from order-service's consumer registration); the new required `reason`
  field is BACKWARD-incompatible (Apicurio 500 "could not determine version").
  Per user decision, the local registry artifact was deleted and re-registered with
  the new shape; `PaymentSettled`'s stale artifact was also deleted. Future changes
  keep the BACKWARD rule.
