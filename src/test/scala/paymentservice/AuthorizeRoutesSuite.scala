package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
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
    } yield Fixture(payments, outbox, methods, approve.id, decline.id)

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
        def get(id: String) = payments.get(id)
        def update(id: String, s: PaymentStatus) = payments.update(id, s)
        def delete(id: String) = payments.delete(id)
        def ping = payments.ping
        def recordAuthorization(
            o: String,
            a: Int,
            m: String,
            auth: Authorization
        ) =
          payments
            .recordAuthorization(o, a, m, auth)
            .flatTap(p => recorded.update(_ :+ p))
        def recordDecline(o: String, a: Int, m: String, r: DeclineReason) =
          payments
            .recordDecline(o, a, m, r)
            .flatTap(p => recorded.update(_ :+ p))
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
      assertEquals(all.flatMap(_.paymentMethodId), List(f0.declineId))
      assertEquals(all.map(_.orderId), List("order-2"))
      assertEquals(all.map(_.amountCents), List(4999))
    }
  }

  test("POST /payments/authorize returns 404 for an unknown payment method") {
    for {
      f <- fixture
      response <- f.authorize(AuthorizePaymentRequest("order-1", 4999, "nope"))
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
        def capture(authId: String) = gateway.capture(authId)
        def void(authId: String) = voided.update(_ :+ authId)
      }
      failingPayments = new PaymentStore[IO] {
        def create(o: String, a: Int) = f.payments.create(o, a)
        def get(id: String) = f.payments.get(id)
        def update(id: String, s: PaymentStatus) = f.payments.update(id, s)
        def delete(id: String) = f.payments.delete(id)
        def ping = f.payments.ping
        def recordAuthorization(
            o: String,
            a: Int,
            m: String,
            auth: Authorization
        ) =
          IO.raiseError(new RuntimeException("db down"))
        def recordDecline(o: String, a: Int, m: String, r: DeclineReason) =
          f.payments.recordDecline(o, a, m, r)
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
}
