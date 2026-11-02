package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{HttpRoutes, Method, Request, Status, Uri}
import org.typelevel.log4cats.noop.NoOpLogger
import paymentservice.TestConversions.given
import paymentservice.domain.{PaymentMethodId, PaymentStatus}
import scala.language.implicitConversions
import sttp.tapir.server.http4s.Http4sServerInterpreter

import java.time.Instant
import java.util.UUID

class ExpiryTriggerRoutesSuite extends CatsEffectSuite {

  private val auth = OperatorAuth(Some("test-token"), "op-1")
  private val methodId = PaymentMethodId.fromUuid(UUID.randomUUID())
  private val liveAuthorization =
    Authorization("auth_live", Instant.now().plusSeconds(86400))

  private def routes(
      store: PaymentStore[IO],
      operatorAuth: OperatorAuth = auth
  ): HttpRoutes[IO] =
    Http4sServerInterpreter[IO]().toRoutes(
      ExpiryTriggerRoutes.expireServerEndpoint[IO](
        store,
        operatorAuth,
        NoOpLogger[IO],
        100
      )
    )

  private def expire(
      routes: HttpRoutes[IO],
      orderId: String,
      token: Option[String] = Some("Bearer test-token")
  ) = {
    val request = Request[IO](
      Method.POST,
      Uri.unsafeFromString(s"/payments/authorizations/$orderId/expire")
    )
    routes.orNotFound.run(
      token.fold(request)(value => request.putHeaders("Authorization" -> value))
    )
  }

  test("expires a live authorization through the sweep path") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      _ <- store.recordAuthorization(
        "order-1",
        4999,
        methodId,
        liveAuthorization
      )
      response <- expire(routes(store), "order-1")
      body <- response.as[PaymentResponse]
      events <- outbox.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.status, "voided")
      assertEquals(
        events.map(_._2.eventType),
        List("PaymentAuthorized", "PaymentAuthorizationExpired")
      )
    }
  }

  test("a second trigger is a 409 and publishes nothing more") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      _ <- store.recordAuthorization(
        "order-1",
        4999,
        methodId,
        liveAuthorization
      )
      _ <- expire(routes(store), "order-1")
      repeat <- expire(routes(store), "order-1")
      body <- repeat.as[ErrorResponse]
      events <- outbox.get
    } yield {
      assertEquals(repeat.status, Status.Conflict)
      assertEquals(body, ErrorResponse("Payment is voided, not authorized"))
      assertEquals(events.size, 2)
    }
  }

  test("a captured payment is a 409 and stays captured") {
    for {
      store <- PaymentStore.inMemory[IO]
      _ <- store.recordAuthorization(
        "order-1",
        4999,
        methodId,
        liveAuthorization
      )
      _ <- store.recordCapture("order-1")
      response <- expire(routes(store), "order-1")
      payment <- store.findByOrderId("order-1")
    } yield {
      assertEquals(response.status, Status.Conflict)
      assertEquals(payment.map(_.status), Some(PaymentStatus.Captured))
    }
  }

  test("an unknown order is a 404") {
    for {
      store <- PaymentStore.inMemory[IO]
      response <- expire(routes(store), "no-such-order")
    } yield assertEquals(response.status, Status.NotFound)
  }

  test("requires the operator token, and is off when none is configured") {
    for {
      store <- PaymentStore.inMemory[IO]
      _ <- store.recordAuthorization(
        "order-1",
        4999,
        methodId,
        liveAuthorization
      )
      missing <- expire(routes(store), "order-1", None)
      wrong <- expire(routes(store), "order-1", Some("Bearer nope"))
      disabled <- expire(routes(store, OperatorAuth.disabled), "order-1")
      payment <- store.findByOrderId("order-1")
    } yield {
      assertEquals(missing.status, Status.Unauthorized)
      assertEquals(wrong.status, Status.Unauthorized)
      assertEquals(disabled.status, Status.Unauthorized)
      assertEquals(payment.map(_.status), Some(PaymentStatus.Authorized))
    }
  }
}
