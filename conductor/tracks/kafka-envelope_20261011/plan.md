# Plan: Migrate to order-events/payment-events topics with envelope

## Phase 1: Envelope + domain-topic consume/publish

- [ ] Task: Write failing `PaymentEventPublisherSuite`/
  `OrderReservedConsumerSuite` tests against the new topics/envelope shape,
  including a new "ignores a different eventType" case (Red)
- [ ] Task: Add `EventEnvelope.scala`; update `PaymentEventPublisher` to
  publish both event types to `payment-events` wrapped in the envelope;
  update `OrderReservedConsumer` to subscribe to `order-events`, decode the
  envelope, branch on `eventType` (Green)
- [ ] Task: Run full `sbt test`, confirm green
- [ ] Task: Conductor - User Manual Verification 'Envelope + domain-topic
  consume/publish' (Protocol in workflow.md)
