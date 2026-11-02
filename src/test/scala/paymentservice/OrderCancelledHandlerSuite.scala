package paymentservice

import cats.effect.{IO, Ref}
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import paymentservice.TestConversions.given
import paymentservice.domain.PaymentStatus
import purekafka.consumer.DeadLetter
import scala.language.implicitConversions
import purekafka.resilience.{
  CircuitBreakerConfig,
  ResilienceConfig,
  RetryConfig
}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

/** `OrderCancelledConsumer.handle` without a broker (US-9.3): the void, its
  * idempotent no-ops, and DLQ routing.
  */
class OrderCancelledHandlerSuite extends CatsEffectSuite {

  private type Dlq = Ref[IO, List[(String, String, Option[String])]]

  private def deadLetter(
      dlq: Dlq,
      maxRetries: Int = 0
  ): IO[DeadLetter[IO, String, Option[String]]] = {
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    DeadLetter.fromPublish[IO, String, Option[String]](
      RetryConfig(maxRetries = maxRetries, baseDelay = 1.milli),
      ResilienceConfig(
        retry = RetryConfig(maxRetries = 0, baseDelay = 1.milli),
        circuitBreaker =
          CircuitBreakerConfig(failureThreshold = 100, resetTimeout = 1.second)
      ),
      (topic, key, value) => dlq.update(_ :+ (topic, key, value))
    )(NoOpLogger[IO])(summon[Meter[IO]])
  }

  private val gateway = PaymentGatewayClient.simulated[IO](
    SimulatedGatewayConfig(
      defaultOutcome = GatewayOutcome.Approve,
      outcomes = Map.empty,
      defaultCaptureOutcome = GatewayOutcome.Approve,
      captureOutcomes = Map.empty,
      authorizationTtl = 7.days
    )
  )

  private final case class Fixture(
      store: PaymentStore[IO],
      outbox: Ref[IO, List[(String, EventEnvelope)]],
      methods: PaymentMethodStore[IO],
      orderId: String
  )

  private val fixture: IO[Fixture] =
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      methods <- PaymentMethodStore.inMemory[IO]
      method <- methods.create(
        "c1",
        Card("4242", CardBrand.Visa, "12/99"),
        Address("1 High St", None, "London", "SW1A 1AA", "GB")
      )
      orderId = UUID.randomUUID().toString
      _ <- store.recordAuthorization(
        orderId,
        4999,
        method.id,
        Authorization("auth-1", Instant.parse("2099-01-01T00:00:00Z"))
      )
    } yield Fixture(store, outbox, methods, orderId)

  private def envelope(
      orderId: String,
      payload: Option[Json] = None
  ): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = "OrderCancelled",
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "order-service",
      correlationId = orderId,
      payload = payload.getOrElse(
        OrderCancelledEvent(
          orderId,
          "cust-1",
          Instant.parse("2026-01-01T00:00:00Z"),
          Some("customer")
        ).asJson
      )
    ).asJson.noSpaces

  private def statusOf(f: Fixture): IO[Option[PaymentStatus]] =
    f.store.findByOrderId(f.orderId).map(_.map(_.status))

  test("OrderCancelled voids an authorized payment") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, Option[String])]](Nil)
      dl <- deadLetter(dlq)
      _ <- OrderCancelledConsumer.handle(
        f.store,
        gateway,
        NoOpLogger[IO],
        dl
      )(f.orderId, Some(envelope(f.orderId)))
      status <- statusOf(f)
      routed <- dlq.get
    } yield {
      assertEquals(status, Some(PaymentStatus.Voided))
      assertEquals(routed, Nil)
    }
  }

  test("a duplicate OrderCancelled is an idempotent no-op") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, Option[String])]](Nil)
      dl <- deadLetter(dlq)
      handle = OrderCancelledConsumer.handle(
        f.store,
        gateway,
        NoOpLogger[IO],
        dl
      ) _
      _ <- handle(f.orderId, Some(envelope(f.orderId)))
      _ <- handle(f.orderId, Some(envelope(f.orderId)))
      status <- statusOf(f)
      routed <- dlq.get
    } yield {
      assertEquals(status, Some(PaymentStatus.Voided))
      assertEquals(routed, Nil)
    }
  }

  test("OrderCancelled does not touch an already-captured payment") {
    for {
      f <- fixture
      _ <- f.store
        .recordCapture(f.orderId)
        .map(_.getOrElse(fail("expected the capture")))
      dlq <- Ref.of[IO, List[(String, String, Option[String])]](Nil)
      dl <- deadLetter(dlq)
      _ <- OrderCancelledConsumer.handle(
        f.store,
        gateway,
        NoOpLogger[IO],
        dl
      )(f.orderId, Some(envelope(f.orderId)))
      status <- statusOf(f)
      reason <- f.store.reconciliationReason(f.orderId)
    } yield {
      assertEquals(status, Some(PaymentStatus.Captured))
      assertEquals(reason, Some("void_after_capture"))
    }
  }

  test("OrderCancelled for an unknown payment is skipped") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, Option[String])]](Nil)
      dl <- deadLetter(dlq)
      unknown = UUID.randomUUID().toString
      _ <- OrderCancelledConsumer.handle(
        f.store,
        gateway,
        NoOpLogger[IO],
        dl
      )(unknown, Some(envelope(unknown)))
      routed <- dlq.get
    } yield assertEquals(routed, Nil)
  }

  test("a malformed OrderCancelled payload is dead-lettered") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, Option[String])]](Nil)
      dl <- deadLetter(dlq)
      _ <- OrderCancelledConsumer.handle(
        f.store,
        gateway,
        NoOpLogger[IO],
        dl
      )(f.orderId, Some("""{"not":"an envelope"}"""))
      routed <- dlq.get
    } yield assertEquals(routed.map(_._1), List("order-events-dlq"))
  }

  test("unrelated order-events event types are ignored") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, Option[String])]](Nil)
      dl <- deadLetter(dlq)
      raw = EventEnvelope(
        eventId = UUID.randomUUID().toString,
        eventType = "OrderConfirmed",
        timestamp = Instant.parse("2026-01-01T00:00:00Z"),
        source = "order-service",
        correlationId = f.orderId,
        payload = Json.obj("orderId" -> Json.fromString(f.orderId))
      ).asJson.noSpaces
      _ <- OrderCancelledConsumer.handle(
        f.store,
        gateway,
        NoOpLogger[IO],
        dl
      )(f.orderId, Some(raw))
      routed <- dlq.get
    } yield assertEquals(routed, Nil)
  }

  test("the void path does not emit a payment event") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, Option[String])]](Nil)
      dl <- deadLetter(dlq)
      _ <- OrderCancelledConsumer.handle(
        f.store,
        gateway,
        NoOpLogger[IO],
        dl
      )(f.orderId, Some(envelope(f.orderId)))
      events <- f.outbox.get
    } yield assertEquals(events.map(_._2.eventType), List("PaymentAuthorized"))
  }
}
