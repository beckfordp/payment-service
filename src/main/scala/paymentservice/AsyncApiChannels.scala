package paymentservice

import purekafka.docs.{
  AsyncApiChannel,
  AsyncApiChannelsProvider,
  ChannelDirection,
  PublisherKind
}

/** Registers payment-service's Kafka event contracts for AsyncAPI documentation
  * (ADR 0010's AsyncAPI follow-up). The `payment-events` publishes go through
  * this service's own `outbox_events` table and the Debezium CDC connector's
  * `route.topic.replacement` config
  * (`infra/k8s/local-infra/debezium-connectors-register.yaml`, gluon repo), not
  * direct producer calls - hence `Publish(Cdc)`, mirroring
  * order-service's/inventory-service's own doc-comment convention.
  *
  * `PaymentAuthorized` is written by Journey 1's synchronous
  * `POST /payments/authorize`; `PaymentCaptured`/`PaymentFailed` by Journey 4's
  * capture-on-dispatch (US-19.1). `PaymentAuthorizationExpired` lands with
  * Journey 5's expiry sweep. Subscribes cover `DispatchFailed` (US-18.2, void)
  * and `DispatchConfirmed` (US-19.1, capture); the old
  * `order-events: OrderReserved` subscription stays retired - authorization is
  * the synchronous REST call now.
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
    AsyncApiChannel[DispatchConfirmedEvent](
      topic = "fulfilment-events",
      eventType = "DispatchConfirmed",
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
