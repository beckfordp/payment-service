# Plan: Migrate to order-events/payment-events topics with envelope

## Phase 1: Envelope + domain-topic consume/publish [checkpoint: 54ca89d]

- [x] Task: Write failing `PaymentEventPublisherSuite`/
  `OrderReservedConsumerSuite` tests against the new topics/envelope shape,
  including a new "ignores a different eventType" case (Red) `7c2ca59`
- [x] Task: Add `EventEnvelope.scala`; update `PaymentEventPublisher` to
  publish both event types to `payment-events` wrapped in the envelope;
  update `OrderReservedConsumer` to subscribe to `order-events`, decode the
  envelope, branch on `eventType` (Green) `54ca89d`
- [x] Task: Run full `sbt test`, confirm green (73/73) `54ca89d`
- [x] Task: Update `scripts/verify-order-reserved-consumer.sh` for the new
  topic/envelope shape `69aa271`
- [x] Task: Conductor - User Manual Verification 'Envelope + domain-topic
  consume/publish' (Protocol in workflow.md) — verify script run live
  end-to-end against a real `sbt run` instance + local Kafka, all checks
  passed, including a direct `GET /payments/{id}` confirming the `Payment`
  itself settled
