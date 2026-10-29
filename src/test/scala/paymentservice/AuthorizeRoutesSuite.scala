package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import paymentservice.domain.{PaymentId, PaymentMethodId}
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Response, Status}
import org.typelevel.log4cats.noop.NoOpLogger

import scala.concurrent.duration._

class AuthorizeRoutesSuite extends CatsEffectSuite {

  private val gateway = PaymentGatewayClient.simulated[IO](
    SimulatedGatewayConfig(
      defaultOutcome = GatewayOutcome.Approve,
      outcomes = Map(
        "tok_visa_0002" -> GatewayOutcome.Decline("insufficient_funds")
      ),
      defaultCaptureOutcome = GatewayOutcome.Approve,
      captureOutcomes = Map.empty,
      authorizationTtl = 7.days
    )
  )

  private val billing = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private final case class Fixture(
      payments: PaymentStore[IO],
      outbox: cats.effect.Ref[IO, List[(String, EventEnvelope)]],
      methods: PaymentMethodStore[IO],
      approveId: String,
      declineId: String
  ) {
    def authorize(req: AuthorizePaymentRequest): IO[Response[IO]] =
      AuthorizeRoutes
        .routes[IO](payments, methods, gateway, NoOpLogger[IO])
        .orNotFound
        .run(Request[IO](Method.POST, uri"/payments/authorize").withEntity(req))
  }

  private val fixture: IO[Fixture] =
    for {
      (payments, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      methods <- PaymentMethodStore.inMemory[IO]
      approve <- methods.create(
        "c1",
        Card("4242", CardBrand.Visa, "12/99"),
        billing
      )
      decline <- methods.create(
        "c1",
        Card("0002", CardBrand.Visa, "12/99"),
        billing
      )
    } yield Fixture(
      payments,
      outbox,
      methods,
      PaymentMethodId.render(approve.id),
      PaymentMethodId.render(decline.id)
    )

  test(
    "POST /payments/authorize returns 200 {authId, expiresAt} and records the outbox event"
  ) {
    for {
      f <- fixture
      before <- IO.realTimeInstant
      response <- f.authorize(
        AuthorizePaymentRequest("order-1", 4999, f.approveId)
      )
      body <- response.as[AuthorizePaymentResponse]
      events <- f.outbox.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(body.authId.startsWith("auth_"))
      assert(body.expiresAt.isAfter(before.plusSeconds(6 * 24 * 3600)))
      assertEquals(events.map(_._1), List("order-1"))
      assertEquals(events.map(_._2.eventType), List("PaymentAuthorized"))
    }
  }

  test(
    "POST /payments/authorize publishes the same authId/expiresAt it returns"
  ) {
    for {
      f <- fixture
      response <- f.authorize(
        AuthorizePaymentRequest("order-1", 4999, f.approveId)
      )
      body <- response.as[AuthorizePaymentResponse]
      events <- f.outbox.get
      payload = events.head._2.payload.as[PaymentAuthorizedEvent].toOption.get
    } yield {
      assertEquals(payload.authId, body.authId)
      assertEquals(payload.expiresAt, body.expiresAt)
    }
  }

  test(
    "POST /payments/authorize returns 402 {error, reason} on decline, nothing published"
  ) {
    for {
      f <- fixture
      response <- f.authorize(
        AuthorizePaymentRequest("order-2", 4999, f.declineId)
      )
      body <- response.as[DeclinedResponse]
      events <- f.outbox.get
    } yield {
      assertEquals(response.status, Status.PaymentRequired)
      assertEquals(
        body,
        DeclinedResponse("Payment declined", "insufficient_funds")
      )
      assertEquals(events, Nil)
    }
  }

  test("POST /payments/authorize records the decline as a Failed payment") {
    for {
      payments <- PaymentStore.inMemory[IO]
      f0 <- fixture
      recorded <- cats.effect.Ref.of[IO, List[Payment]](Nil)
      spying = new PaymentStore[IO] {
        def create(o: String, a: Int) = payments.create(o, a)
        def get(id: PaymentId) = payments.get(id)
        def recordVoid(o: String) = payments.recordVoid(o)
        def delete(id: PaymentId) = payments.delete(id)
        def ping = payments.ping
        def findByOrderId(o: String) = payments.findByOrderId(o)
        def recordAuthorization(
            o: String,
            a: Int,
            m: PaymentMethodId,
            auth: Authorization
        ) =
          payments
            .recordAuthorization(o, a, m, auth)
            .flatTap {
              case Some(p) => recorded.update(_ :+ p)
              case None    => IO.unit
            }
        def recordDecline(
            o: String,
            a: Int,
            m: PaymentMethodId,
            r: DeclineReason
        ) =
          payments
            .recordDecline(o, a, m, r)
            .flatTap {
              case Some(p) => recorded.update(_ :+ p)
              case None    => IO.unit
            }
        def recordCapture(o: String) = payments.recordCapture(o)
        def recordCaptureFailure(o: String, r: String) =
          payments.recordCaptureFailure(o, r)
      }
      _ <- f0
        .copy(payments = spying)
        .authorize(
          AuthorizePaymentRequest("order-2", 4999, f0.declineId)
        )
      all <- recorded.get
    } yield {
      assertEquals(all.map(_.status), List(PaymentStatus.Failed))
      assertEquals(all.flatMap(_.declineReason), List("insufficient_funds"))
      assertEquals(
        all.flatMap(_.paymentMethodId).map(PaymentMethodId.render),
        List(f0.declineId)
      )
      assertEquals(all.map(_.orderId), List("order-2"))
      assertEquals(all.map(_.amountCents), List(4999))
    }
  }

  test("POST /payments/authorize returns 404 for an unknown payment method") {
    for {
      f <- fixture
      response <- f.authorize(
        AuthorizePaymentRequest(
          "order-1",
          4999,
          java.util.UUID.randomUUID().toString
        )
      )
      body <- response.as[ErrorResponse]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assertEquals(body.error, "Payment method not found")
    }
  }

  test("POST /payments/authorize returns 400 for a non-positive amount") {
    for {
      f <- fixture
      zero <- f.authorize(AuthorizePaymentRequest("order-1", 0, f.approveId))
      negative <- f.authorize(
        AuthorizePaymentRequest("order-1", -5, f.approveId)
      )
      body <- zero.as[ErrorResponse]
      events <- f.outbox.get
    } yield {
      assertEquals(zero.status, Status.BadRequest)
      assertEquals(negative.status, Status.BadRequest)
      assertEquals(body.error, "amountCents must be positive")
      assertEquals(events, Nil)
    }
  }

  test("POST /payments/authorize returns 400 for a blank orderId") {
    for {
      f <- fixture
      response <- f.authorize(AuthorizePaymentRequest(" ", 4999, f.approveId))
      body <- response.as[ErrorResponse]
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assertEquals(body.error, "orderId is required")
    }
  }

  test("POST /payments/authorize voids the hold when recording it fails") {
    for {
      f <- fixture
      voided <- cats.effect.Ref.of[IO, List[String]](Nil)
      spyGateway = new PaymentGatewayClient[IO] {
        def authorize(token: String, amount: Int) =
          gateway.authorize(token, amount)
        def capture(authId: String, cardToken: String) =
          gateway.capture(authId, cardToken)
        def void(authId: String) = voided.update(_ :+ authId)
      }
      failingPayments = new PaymentStore[IO] {
        def create(o: String, a: Int) = f.payments.create(o, a)
        def get(id: PaymentId) = f.payments.get(id)
        def recordVoid(o: String) = f.payments.recordVoid(o)
        def delete(id: PaymentId) = f.payments.delete(id)
        def ping = f.payments.ping
        def findByOrderId(o: String) = f.payments.findByOrderId(o)
        def recordAuthorization(
            o: String,
            a: Int,
            m: PaymentMethodId,
            auth: Authorization
        ) =
          IO.raiseError(new RuntimeException("db down"))
        def recordDecline(
            o: String,
            a: Int,
            m: PaymentMethodId,
            r: DeclineReason
        ) =
          f.payments.recordDecline(o, a, m, r)
        def recordCapture(o: String) = f.payments.recordCapture(o)
        def recordCaptureFailure(o: String, r: String) =
          f.payments.recordCaptureFailure(o, r)
      }
      response <- AuthorizeRoutes
        .routes[IO](failingPayments, f.methods, spyGateway, NoOpLogger[IO])
        .orNotFound
        .run(
          Request[IO](Method.POST, uri"/payments/authorize")
            .withEntity(AuthorizePaymentRequest("order-1", 4999, f.approveId))
        )
        .attempt
      released <- voided.get
    } yield {
      assert(response.fold(_ => true, _.status.code >= 500), response)
      assertEquals(released.size, 1)
      assert(released.head.startsWith("auth_"))
    }
  }

  private def post(
      routes: org.http4s.HttpRoutes[IO],
      req: AuthorizePaymentRequest
  ): IO[Response[IO]] =
    routes.orNotFound.run(
      Request[IO](Method.POST, uri"/payments/authorize").withEntity(req)
    )

  private def countingGateway(
      calls: cats.effect.Ref[IO, Int],
      voided: cats.effect.Ref[IO, List[String]]
  ): PaymentGatewayClient[IO] =
    new PaymentGatewayClient[IO] {
      def authorize(token: String, amount: Int) =
        calls.update(_ + 1) *> gateway.authorize(token, amount)
      def capture(authId: String, cardToken: String) =
        gateway.capture(authId, cardToken)
      def void(authId: String) = voided.update(_ :+ authId)
    }

  test(
    "a repeated authorize replays the same authId without a second gateway call or outbox row"
  ) {
    for {
      f <- fixture
      calls <- cats.effect.Ref.of[IO, Int](0)
      voided <- cats.effect.Ref.of[IO, List[String]](Nil)
      routes = AuthorizeRoutes.routes[IO](
        f.payments,
        f.methods,
        countingGateway(calls, voided),
        NoOpLogger[IO]
      )
      req = AuthorizePaymentRequest("order-replay", 4999, f.approveId)
      first <- post(routes, req)
      second <- post(routes, req)
      firstBody <- first.as[AuthorizePaymentResponse]
      secondBody <- second.as[AuthorizePaymentResponse]
      gatewayCalls <- calls.get
      voidedIds <- voided.get
      events <- f.outbox.get
    } yield {
      assertEquals(first.status, Status.Ok)
      assertEquals(second.status, Status.Ok)
      assertEquals(secondBody, firstBody)
      assertEquals(gatewayCalls, 1)
      assertEquals(voidedIds, Nil)
      assertEquals(events.size, 1)
    }
  }

  test("a replayed decline returns the stored 402 reason") {
    for {
      f <- fixture
      calls <- cats.effect.Ref.of[IO, Int](0)
      voided <- cats.effect.Ref.of[IO, List[String]](Nil)
      routes = AuthorizeRoutes.routes[IO](
        f.payments,
        f.methods,
        countingGateway(calls, voided),
        NoOpLogger[IO]
      )
      req = AuthorizePaymentRequest("order-decline-replay", 4999, f.declineId)
      first <- post(routes, req)
      second <- post(routes, req)
      firstBody <- first.as[DeclinedResponse]
      secondBody <- second.as[DeclinedResponse]
      gatewayCalls <- calls.get
      events <- f.outbox.get
    } yield {
      assertEquals(first.status, Status.PaymentRequired)
      assertEquals(second.status, Status.PaymentRequired)
      assertEquals(secondBody, firstBody)
      assertEquals(gatewayCalls, 1)
      assertEquals(events, Nil)
    }
  }

  test("a different amount for the same order returns 409 and places no hold") {
    for {
      f <- fixture
      calls <- cats.effect.Ref.of[IO, Int](0)
      voided <- cats.effect.Ref.of[IO, List[String]](Nil)
      routes = AuthorizeRoutes.routes[IO](
        f.payments,
        f.methods,
        countingGateway(calls, voided),
        NoOpLogger[IO]
      )
      _ <- post(
        routes,
        AuthorizePaymentRequest("order-amount", 4999, f.approveId)
      )
      conflict <- post(
        routes,
        AuthorizePaymentRequest("order-amount", 5000, f.approveId)
      )
      body <- conflict.as[ErrorResponse]
      gatewayCalls <- calls.get
      events <- f.outbox.get
    } yield {
      assertEquals(conflict.status, Status.Conflict)
      assertEquals(
        body.error,
        "Order already has a payment with a different amount or payment method"
      )
      assertEquals(gatewayCalls, 1)
      assertEquals(events.size, 1)
    }
  }

  test("a different payment method for the same order returns 409") {
    for {
      f <- fixture
      calls <- cats.effect.Ref.of[IO, Int](0)
      voided <- cats.effect.Ref.of[IO, List[String]](Nil)
      routes = AuthorizeRoutes.routes[IO](
        f.payments,
        f.methods,
        countingGateway(calls, voided),
        NoOpLogger[IO]
      )
      _ <- post(
        routes,
        AuthorizePaymentRequest("order-method", 4999, f.approveId)
      )
      conflict <- post(
        routes,
        AuthorizePaymentRequest("order-method", 4999, f.declineId)
      )
      gatewayCalls <- calls.get
    } yield {
      assertEquals(conflict.status, Status.Conflict)
      assertEquals(gatewayCalls, 1)
    }
  }

  test(
    "losing an insert race voids the fresh hold and replays the winning outcome"
  ) {
    for {
      f <- fixture
      winner <- f.payments
        .recordAuthorization(
          "order-race",
          4999,
          PaymentMethodId.parse(f.approveId).toOption.get,
          Authorization(
            "auth_winner",
            java.time.Instant.parse("2099-11-03T12:00:00Z")
          )
        )
        .map(_.getOrElse(fail("expected the winner row")))
      lookups <- cats.effect.Ref.of[IO, Int](0)
      voided <- cats.effect.Ref.of[IO, List[String]](Nil)
      racingStore = new PaymentStore[IO] {
        def create(o: String, a: Int) = f.payments.create(o, a)
        def get(id: PaymentId) = f.payments.get(id)
        def recordVoid(o: String) = f.payments.recordVoid(o)
        def delete(id: PaymentId) = f.payments.delete(id)
        def ping = f.payments.ping
        def findByOrderId(o: String) =
          lookups.modify(n => (n + 1, n)).flatMap { n =>
            if (n == 0) IO.pure(None) else f.payments.findByOrderId(o)
          }
        def recordAuthorization(
            o: String,
            a: Int,
            m: PaymentMethodId,
            auth: Authorization
        ) = IO.pure(None)
        def recordDecline(
            o: String,
            a: Int,
            m: PaymentMethodId,
            r: DeclineReason
        ) =
          f.payments.recordDecline(o, a, m, r)
        def recordCapture(o: String) = f.payments.recordCapture(o)
        def recordCaptureFailure(o: String, r: String) =
          f.payments.recordCaptureFailure(o, r)
      }
      routes = AuthorizeRoutes.routes[IO](
        racingStore,
        f.methods,
        countingGateway(cats.effect.Ref.unsafe[IO, Int](0), voided),
        NoOpLogger[IO]
      )
      response <- post(
        routes,
        AuthorizePaymentRequest("order-race", 4999, f.approveId)
      )
      body <- response.as[AuthorizePaymentResponse]
      released <- voided.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.authId, "auth_winner")
      assertEquals(released.size, 1)
      assert(released.head.startsWith("auth_"))
      assertNotEquals(released.head, winner.authId.getOrElse(""))
    }
  }

  test("a missing row after an insert conflict returns 500") {
    for {
      f <- fixture
      voided <- cats.effect.Ref.of[IO, List[String]](Nil)
      racingStore = new PaymentStore[IO] {
        def create(o: String, a: Int) = f.payments.create(o, a)
        def get(id: PaymentId) = f.payments.get(id)
        def recordVoid(o: String) = f.payments.recordVoid(o)
        def delete(id: PaymentId) = f.payments.delete(id)
        def ping = f.payments.ping
        def findByOrderId(o: String) = IO.pure(None)
        def recordAuthorization(
            o: String,
            a: Int,
            m: PaymentMethodId,
            auth: Authorization
        ) = IO.pure(None)
        def recordDecline(
            o: String,
            a: Int,
            m: PaymentMethodId,
            r: DeclineReason
        ) =
          f.payments.recordDecline(o, a, m, r)
        def recordCapture(o: String) = f.payments.recordCapture(o)
        def recordCaptureFailure(o: String, r: String) =
          f.payments.recordCaptureFailure(o, r)
      }
      routes = AuthorizeRoutes.routes[IO](
        racingStore,
        f.methods,
        countingGateway(cats.effect.Ref.unsafe[IO, Int](0), voided),
        NoOpLogger[IO]
      )
      response <- post(
        routes,
        AuthorizePaymentRequest("order-missing", 4999, f.approveId)
      )
      body <- response.as[ErrorResponse]
      released <- voided.get
    } yield {
      assertEquals(response.status, Status.InternalServerError)
      assertEquals(body.error, "Payment could not be recorded or read back")
      assertEquals(released.size, 1)
    }
  }

  test(
    "POST /payments/authorize cancelled while recording voids the fresh hold"
  ) {
    for {
      f <- fixture
      voided <- cats.effect.Ref.of[IO, List[String]](Nil)
      recording <- cats.effect.Deferred[IO, Unit]
      hangingStore = new PaymentStore[IO] {
        def create(o: String, a: Int) = f.payments.create(o, a)
        def get(id: PaymentId) = f.payments.get(id)
        def recordVoid(o: String) = f.payments.recordVoid(o)
        def delete(id: PaymentId) = f.payments.delete(id)
        def ping = f.payments.ping
        def findByOrderId(o: String) = f.payments.findByOrderId(o)
        def recordAuthorization(
            o: String,
            a: Int,
            m: PaymentMethodId,
            auth: Authorization
        ) = recording.complete(()) *> IO.never
        def recordDecline(
            o: String,
            a: Int,
            m: PaymentMethodId,
            r: DeclineReason
        ) = f.payments.recordDecline(o, a, m, r)
        def recordCapture(o: String) = f.payments.recordCapture(o)
        def recordCaptureFailure(o: String, r: String) =
          f.payments.recordCaptureFailure(o, r)
      }
      routes = AuthorizeRoutes.routes[IO](
        hangingStore,
        f.methods,
        countingGateway(cats.effect.Ref.unsafe[IO, Int](0), voided),
        NoOpLogger[IO]
      )
      fiber <- post(
        routes,
        AuthorizePaymentRequest("order-cancel", 4999, f.approveId)
      ).start
      _ <- recording.get
      _ <- fiber.cancel
      released <- voided.get
    } yield {
      assertEquals(released.size, 1)
      assert(released.head.startsWith("auth_"), released.head)
    }
  }

  test("replaying an expired authorization is a 409, not the stale authId") {
    for {
      f <- fixture
      _ <- f.payments.recordAuthorization(
        "order-expired",
        4999,
        PaymentMethodId.parse(f.approveId).toOption.get,
        Authorization(
          "auth_stale",
          java.time.Instant.parse("2020-01-01T00:00:00Z")
        )
      )
      response <- f.authorize(
        AuthorizePaymentRequest("order-expired", 4999, f.approveId)
      )
      body <- response.as[ErrorResponse]
    } yield {
      assertEquals(response.status, Status.Conflict)
      assert(body.error.toLowerCase.contains("expired"), body.error)
    }
  }
}
