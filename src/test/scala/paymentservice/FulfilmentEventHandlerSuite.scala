package paymentservice

import cats.effect.{IO, Ref}
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import paymentservice.domain.PaymentId
import purekafka.consumer.DeadLetter
import purekafka.resilience.{
  CircuitBreakerConfig,
  ResilienceConfig,
  RetryConfig
}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

/** `FulfilmentEventConsumer.handle` without a broker: the per-record logic, its
  * DLQ routing and its retry behaviour (review findings 2 and 3).
  */
class FulfilmentEventHandlerSuite extends CatsEffectSuite {

  private type Dlq = Ref[IO, List[(String, String, String)]]

  private def deadLetter(
      dlq: Dlq,
      maxRetries: Int = 0
  ): IO[DeadLetter[IO, String, String]] = {
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    DeadLetter.fromPublish[IO, String, String](
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

  /** Delegates to `underlying`; tests override the one write they sabotage. */
  private class Delegating(underlying: PaymentStore[IO])
      extends PaymentStore[IO] {
    def create(o: String, a: Int) = underlying.create(o, a)
    def get(id: PaymentId) = underlying.get(id)
    def delete(id: PaymentId) = underlying.delete(id)
    def ping = underlying.ping
    def findByOrderId(o: String) = underlying.findByOrderId(o)
    def recordAuthorization(
        o: String,
        a: Int,
        m: paymentservice.domain.PaymentMethodId,
        auth: Authorization
    ) = underlying.recordAuthorization(o, a, m, auth)
    def recordDecline(
        o: String,
        a: Int,
        m: paymentservice.domain.PaymentMethodId,
        r: DeclineReason
    ) = underlying.recordDecline(o, a, m, r)
    def recordCapture(o: String) = underlying.recordCapture(o)
    def recordCaptureFailure(o: String, r: String) =
      underlying.recordCaptureFailure(o, r)
    def recordVoid(o: String) = underlying.recordVoid(o)
  }

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

  private def envelope(eventType: String, payload: Json): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "fulfilment-service",
      correlationId = "order-1",
      payload = payload
    ).asJson.noSpaces

  private def confirmed(orderId: String): String =
    envelope(
      "DispatchConfirmed",
      DispatchConfirmedEvent(
        orderId,
        "TRACK-1",
        List(FulfilmentItem("sku-1", "Widget", 1, 4999))
      ).asJson
    )

  private def failed(orderId: String): String =
    envelope("DispatchFailed", DispatchFailedEvent(orderId, "damaged").asJson)

  private def handle(
      store: PaymentStore[IO],
      methods: PaymentMethodStore[IO],
      dl: DeadLetter[IO, String, String],
      gw: PaymentGatewayClient[IO] = gateway
  )(key: String, raw: String): IO[Unit] =
    FulfilmentEventConsumer.handle[IO](
      store,
      methods,
      gw,
      NoOpLogger[IO],
      dl
    )(key, Some(raw))

  test("a capture the store no longer records is dead-lettered") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      store = new Delegating(f.store) {
        override def recordCapture(o: String) = IO.pure(None)
      }
      raw = confirmed(f.orderId)
      _ <- handle(store, f.methods, dl)(f.orderId, raw)
      routed <- dlq.get
    } yield assertEquals(
      routed,
      List(("fulfilment-events-dlq", f.orderId, raw))
    )
  }

  test("a capture failure the store no longer records is dead-lettered") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      store = new Delegating(f.store) {
        override def recordCaptureFailure(o: String, r: String) = IO.pure(None)
      }
      declining = new PaymentGatewayClient[IO] {
        def authorize(t: String, a: Int) = gateway.authorize(t, a)
        def capture(authId: String, t: String) =
          IO.pure(Left(DeclineReason("capture_declined")))
        def void(authId: String) = IO.unit
      }
      _ <- handle(store, f.methods, dl, declining)(
        f.orderId,
        confirmed(f.orderId)
      )
      routed <- dlq.get
    } yield assertEquals(routed.map(_._1), List("fulfilment-events-dlq"))
  }

  test("a void the store no longer records is dead-lettered") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      store = new Delegating(f.store) {
        override def recordVoid(o: String) = IO.pure(None)
      }
      _ <- handle(store, f.methods, dl)(f.orderId, failed(f.orderId))
      routed <- dlq.get
    } yield assertEquals(routed.map(_._1), List("fulfilment-events-dlq"))
  }

  test(
    "a DB failure after capture is retried and records exactly one PaymentCaptured"
  ) {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq, maxRetries = 1)
      attempts <- Ref.of[IO, Int](0)
      captures <- Ref.of[IO, List[String]](Nil)
      store = new Delegating(f.store) {
        override def recordCapture(o: String) =
          attempts.getAndUpdate(_ + 1).flatMap { n =>
            if (n == 0) IO.raiseError(new RuntimeException("db down"))
            else f.store.recordCapture(o)
          }
      }
      counting = new PaymentGatewayClient[IO] {
        def authorize(t: String, a: Int) = gateway.authorize(t, a)
        def capture(authId: String, t: String) =
          captures.update(_ :+ authId) *> gateway.capture(authId, t)
        def void(authId: String) = gateway.void(authId)
      }
      _ <- handle(store, f.methods, dl, counting)(
        f.orderId,
        confirmed(f.orderId)
      )
      routed <- dlq.get
      events <- f.outbox.get
      payment <- f.store.findByOrderId(f.orderId)
      captured <- captures.get
    } yield {
      assertEquals(routed, Nil)
      assertEquals(payment.map(_.status), Some(PaymentStatus.Captured))
      assertEquals(
        events.map(_._2.eventType).count(_ == "PaymentCaptured"),
        1
      )
      assertEquals(captured, List("auth-1", "auth-1"))
    }
  }

  test("a duplicate DispatchConfirmed after a capture is a no-op") {
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      raw = confirmed(f.orderId)
      _ <- handle(f.store, f.methods, dl)(f.orderId, raw)
      _ <- handle(f.store, f.methods, dl)(f.orderId, raw)
      routed <- dlq.get
      events <- f.outbox.get
    } yield {
      assertEquals(routed, Nil)
      assertEquals(
        events.map(_._2.eventType).count(_ == "PaymentCaptured"),
        1
      )
    }
  }

  private def assertDeadLettered(raw: String) =
    for {
      f <- fixture
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      _ <- handle(f.store, f.methods, dl)("k", raw)
      routed <- dlq.get
    } yield assertEquals(routed, List(("fulfilment-events-dlq", "k", raw)))

  test("an undecodable envelope is dead-lettered raw") {
    assertDeadLettered("{not json")
  }

  test("an undecodable DispatchConfirmed payload is dead-lettered raw") {
    assertDeadLettered(envelope("DispatchConfirmed", Json.obj()))
  }

  test("an undecodable DispatchFailed payload is dead-lettered raw") {
    assertDeadLettered(envelope("DispatchFailed", Json.obj()))
  }

  test("an unknown event type is dead-lettered raw") {
    assertDeadLettered(envelope("DispatchExploded", Json.obj()))
  }

  test(
    "DispatchConfirmed for an expired authorization is dead-lettered without a gateway call"
  ) {
    for {
      (store, _) <- PaymentStore.inMemoryWithOutbox[IO]
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
        Authorization("auth-old", Instant.parse("2020-01-01T00:00:00Z"))
      )
      dlq <- Ref.of[IO, List[(String, String, String)]](Nil)
      dl <- deadLetter(dlq)
      captures <- Ref.of[IO, List[String]](Nil)
      counting = new PaymentGatewayClient[IO] {
        def authorize(t: String, a: Int) = gateway.authorize(t, a)
        def capture(authId: String, t: String) =
          captures.update(_ :+ authId) *> gateway.capture(authId, t)
        def void(authId: String) = gateway.void(authId)
      }
      _ <- handle(store, methods, dl, counting)(orderId, confirmed(orderId))
      routed <- dlq.get
      captured <- captures.get
      after <- store.findByOrderId(orderId)
    } yield {
      assertEquals(routed.map(_._1), List("fulfilment-events-dlq"))
      assertEquals(captured, Nil)
      assertEquals(after.map(_.status), Some(PaymentStatus.Authorized))
    }
  }
}
