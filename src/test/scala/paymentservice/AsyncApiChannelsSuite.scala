package paymentservice

import paymentservice.TestConversions.given
import scala.language.implicitConversions
import io.circe.parser.parse
import io.circe.syntax._
import munit.FunSuite
import purekafka.docs.{
  AsyncApiDoc,
  AsyncApiSpecWriter,
  ChannelDirection,
  PublisherKind
}

import java.nio.file.Files
import java.time.Instant

class AsyncApiChannelsSuite extends FunSuite {

  private val sampleInstant = Instant.parse("2026-10-26T00:00:00Z")

  test(
    "AsyncApiChannels.channels registers the four payment-events contracts, both consumed fulfilment events and OrderCancelled"
  ) {
    assertEquals(
      AsyncApiChannels.channels.map(c => c.topic -> c.eventType).toSet,
      Set(
        "payment-events" -> "PaymentAuthorized",
        "payment-events" -> "PaymentCaptured",
        "payment-events" -> "PaymentAuthorizationExpired",
        "payment-events" -> "PaymentFailed",
        "fulfilment-events" -> "DispatchFailed",
        "fulfilment-events" -> "DispatchConfirmed",
        "order-events" -> "OrderCancelled"
      )
    )
  }

  test(
    "payment-events contracts are Publish(Cdc); fulfilment-events and order-events contracts are Subscribe-only"
  ) {
    for (
      channel <- AsyncApiChannels.channels.filter(_.topic == "payment-events")
    )
      assertEquals(
        channel.direction,
        ChannelDirection.Publish(PublisherKind.Cdc): ChannelDirection,
        s"expected ${channel.eventType} to be Publish(Cdc)"
      )

    for (
      eventType <- List("DispatchFailed", "DispatchConfirmed", "OrderCancelled")
    ) {
      val consumed = AsyncApiChannels.channels
        .find(_.eventType == eventType)
        .getOrElse(fail(s"expected a $eventType channel"))
      assertEquals(
        consumed.direction,
        ChannelDirection.Subscribe: ChannelDirection,
        s"expected $eventType to be Subscribe"
      )
    }
  }

  test("the retired PaymentSettled and OrderReserved event types are gone") {
    val eventTypes = AsyncApiChannels.channels.map(_.eventType).toSet
    assert(!eventTypes.contains("PaymentSettled"))
    assert(!eventTypes.contains("OrderReserved"))
  }

  test("event payloads carry exactly the pinned system-design.md field names") {
    val authorized = PaymentAuthorizedEvent(
      orderId = "o-1",
      authId = "a-1",
      expiresAt = sampleInstant,
      timestamp = sampleInstant
    )
    val captured = PaymentCapturedEvent(
      orderId = "o-1",
      amountCents = 4999,
      timestamp = sampleInstant
    )
    val expired = PaymentAuthorizationExpiredEvent(
      orderId = "o-1",
      timestamp = sampleInstant
    )
    val failed = PaymentFailedEvent(
      orderId = "o-1",
      paymentId = "p-1",
      amountCents = 4999,
      reason = "capture_failed",
      timestamp = sampleInstant
    )
    val confirmed = DispatchConfirmedEvent(
      orderId = "o-1",
      trackingNo = "TRACK-1",
      items = List(FulfilmentItem("sku-1", "Widget", 1, 4999))
    )
    val cancelled = OrderCancelledEvent(
      orderId = "o-1",
      customerId = "c-1",
      timestamp = sampleInstant,
      reason = Some("customer")
    )

    def keysOf(value: io.circe.Json): Set[String] =
      value.asObject.map(_.keys.toSet).getOrElse(Set.empty)

    assertEquals(
      keysOf(authorized.asJson),
      Set("orderId", "authId", "expiresAt", "timestamp")
    )
    assertEquals(
      keysOf(captured.asJson),
      Set("orderId", "amountCents", "timestamp")
    )
    assertEquals(
      keysOf(expired.asJson),
      Set("orderId", "timestamp")
    )
    assertEquals(
      keysOf(failed.asJson),
      Set("orderId", "paymentId", "amountCents", "reason", "timestamp")
    )
    assertEquals(
      keysOf(confirmed.asJson),
      Set("orderId", "trackingNo", "items")
    )
    assertEquals(
      keysOf(cancelled.asJson),
      Set("orderId", "customerId", "timestamp", "reason")
    )
  }

  test(
    "paymentservice.AsyncApiChannels is reflectively loadable by name, matching build.sbt's asyncApiChannelsClass setting - a regression guard for sbt-purekafka-docs' AsyncApiSpecWriter"
  ) {
    val outputPath =
      Files.createTempFile("payment-service-asyncapi-test", ".json")
    outputPath.toFile.deleteOnExit()

    AsyncApiSpecWriter.main(
      Array(
        "paymentservice.AsyncApiChannels",
        "Payment Service",
        "1.0",
        outputPath.toString
      )
    )

    val written = parse(Files.readString(outputPath)).toOption.get
    val expected =
      AsyncApiDoc.generate("Payment Service", "1.0", AsyncApiChannels.channels)

    assertEquals(written, expected)
  }

  test(
    "every consumed event's generated AsyncAPI message carries a description (copied from the producer's registry artifact)"
  ) {
    val doc = AsyncApiDoc.generate(
      "test",
      "1.0",
      AsyncApiChannels.channels
    )
    val consumed = AsyncApiChannels.channels.filter(
      _.direction == ChannelDirection.Subscribe
    )
    assert(consumed.nonEmpty)
    for (c <- consumed)
      assert(
        doc.hcursor
          .downField("channels")
          .downField(c.topic)
          .downField("messages")
          .downField(c.eventType)
          .downField("payload")
          .downField("description")
          .as[String]
          .isRight,
        s"${c.topic}/${c.eventType} has no description"
      )
  }
}
