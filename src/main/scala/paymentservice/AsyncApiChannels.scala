package paymentservice

import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiChannelsProvider,
  ChannelDirection,
  PublisherKind
}

/** Registers payment-service's Kafka event contracts for AsyncAPI documentation
  * (ADR 0010's AsyncAPI follow-up). `OrderReservedConsumer` only ever consumes
  * `OrderReserved` on `order-events` - `Subscribe`. `PaymentSettled` is
  * published via this service's own `outbox_events` table and the Debezium CDC
  * connector's `route.topic.replacement` config
  * (`infra/k8s/local-infra/debezium-connectors-register.yaml`, gluon repo), not
  * a direct producer call - hence `Publish(Cdc)`, mirroring
  * order-service's/inventory-service's own doc-comment convention.
  *
  * `PaymentFailedEvent` (defined in `PaymentEvents.scala`) is deliberately
  * **not** registered here: `PaymentStore.update`'s outbox-write branch only
  * fires `if status == PaymentStatus.Settled` - a transition to `Failed` writes
  * no outbox event today, so `PaymentFailedEvent` is never actually published.
  * Registering it would document something untrue.
  *
  * Implements `AsyncApiChannelsProvider` (not just a plain `List`) so the
  * sbt-purekafka-docs plugin's `AsyncApiSpecWriter` can load this singleton
  * reflectively by class name, configured via `build.sbt`'s
  * `asyncApiChannelsClass`.
  */
object AsyncApiChannels extends AsyncApiChannelsProvider {

  // Not read anywhere else in this app's own code for the publish side - the
  // outbox pattern means payment-service never names that Kafka topic
  // directly, only Debezium's connector config does (see this object's own
  // doc comment). order-events, however, is read directly by
  // OrderReservedConsumer.topic.
  private val paymentEventsTopic = "payment-events"

  val channels: List[AsyncApiChannel[?]] = List(
    AsyncApiChannel[OrderReservedEvent](
      topic = OrderReservedConsumer.topic,
      eventType = "OrderReserved",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[PaymentSettledEvent](
      topic = paymentEventsTopic,
      eventType = "PaymentSettled",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    )
  )
}
