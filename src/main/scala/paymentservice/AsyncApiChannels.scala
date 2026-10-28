package paymentservice

import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiChannelsProvider,
  ChannelDirection,
  PublisherKind
}

/** Registers payment-service's Kafka event contracts for AsyncAPI documentation
  * (ADR 0010's AsyncAPI follow-up). All four are `payment-events` publishes via
  * this service's own `outbox_events` table and the Debezium CDC connector's
  * `route.topic.replacement` config
  * (`infra/k8s/local-infra/debezium-connectors-register.yaml`, gluon repo), not
  * direct producer calls - hence `Publish(Cdc)`, mirroring
  * order-service's/inventory-service's own doc-comment convention.
  *
  * The producers land with Checkout Journeys 1/4/5 (synchronous
  * `POST /payments/authorize`, capture-on-dispatch, and the expiry sweep), so
  * no code writes these rows yet. payment-service no longer consumes
  * `order-events: OrderReserved` - authorization is the synchronous REST call
  * now - so there are no Subscribe channels here.
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
  // doc comment).
  private val paymentEventsTopic = "payment-events"

  val channels: List[AsyncApiChannel[?]] = List(
    AsyncApiChannel[PaymentAuthorizedEvent](
      topic = paymentEventsTopic,
      eventType = "PaymentAuthorized",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    ),
    AsyncApiChannel[PaymentCapturedEvent](
      topic = paymentEventsTopic,
      eventType = "PaymentCaptured",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    ),
    AsyncApiChannel[DispatchFailedEvent](
      topic = "fulfilment-events",
      eventType = "DispatchFailed",
      direction = ChannelDirection.Subscribe
    ),
    AsyncApiChannel[PaymentAuthorizationExpiredEvent](
      topic = paymentEventsTopic,
      eventType = "PaymentAuthorizationExpired",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    ),
    AsyncApiChannel[PaymentFailedEvent](
      topic = paymentEventsTopic,
      eventType = "PaymentFailed",
      direction = ChannelDirection.Publish(PublisherKind.Cdc)
    )
  )
}
