package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{HttpRoutes, Method, Request, Status, Uri}
import org.typelevel.log4cats.noop.NoOpLogger
import paymentservice.domain.OrderId
import sttp.tapir.server.http4s.Http4sServerInterpreter

class PaymentReconciliationSuite extends CatsEffectSuite {

  private val auth = OperatorAuth(Some("test-token"), "op-1")

  private def routes(store: PaymentStore[IO]): HttpRoutes[IO] =
    Http4sServerInterpreter[IO]().toRoutes(
      List(
        ReconciliationRoutes.listServerEndpoint[IO](store, NoOpLogger[IO]),
        ReconciliationRoutes.resolveServerEndpoint[IO](
          store,
          auth,
          NoOpLogger[IO]
        )
      )
    )

  private def seeded(store: PaymentStore[IO]): IO[OrderId] = {
    val orderId = OrderId.parse("order-1").getOrElse(fail("bad id"))
    store.create(orderId, 1000).as(orderId)
  }

  private def get(routes: HttpRoutes[IO], path: String) =
    routes.orNotFound.run(Request[IO](Method.GET, Uri.unsafeFromString(path)))

  private def resolve(
      routes: HttpRoutes[IO],
      orderId: OrderId,
      token: Option[String],
      body: ResolveReconciliationRequest
  ) = {
    val request = Request[IO](
      Method.PATCH,
      Uri.unsafeFromString(s"/reconciliations/${OrderId.render(orderId)}")
    ).withEntity(body)
    routes.orNotFound.run(
      token.fold(request)(value => request.putHeaders("Authorization" -> value))
    )
  }

  test("GET /reconciliations lists and filters the queue") {
    for {
      store <- PaymentStore.inMemory[IO]
      orderId <- seeded(store)
      _ <- store.flagReconciliation(orderId, "capture_after_void")
      response <- get(routes(store), "/reconciliations?status=open")
      body <- response.as[List[ReconciliationResponse]]
      invalid <- get(routes(store), "/reconciliations?status=bogus")
      unknownOrder <-
        get(routes(store), "/reconciliations?orderId=not-a-payment-order")
      unknownBody <- unknownOrder.as[List[ReconciliationResponse]]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.map(_.orderId), List("order-1"))
      assertEquals(body.map(_.reason), List("capture_after_void"))
      assertEquals(invalid.status, Status.BadRequest)
      // Payment order ids are non-blank text, so an unknown id is not 400.
      assertEquals(unknownOrder.status, Status.Ok)
      assertEquals(unknownBody, Nil)
    }
  }

  test("PATCH /reconciliations requires the operator token") {
    for {
      store <- PaymentStore.inMemory[IO]
      orderId <- seeded(store)
      _ <- store.flagReconciliation(orderId, "capture_after_void")
      noToken <- resolve(
        routes(store),
        orderId,
        None,
        ResolveReconciliationRequest("resolved")
      )
      wrong <- resolve(
        routes(store),
        orderId,
        Some("Bearer wrong"),
        ResolveReconciliationRequest("resolved")
      )
    } yield {
      assertEquals(noToken.status, Status.Unauthorized)
      assertEquals(wrong.status, Status.Unauthorized)
    }
  }

  test("PATCH /reconciliations resolves once and is idempotent") {
    for {
      store <- PaymentStore.inMemory[IO]
      orderId <- seeded(store)
      _ <- store.flagReconciliation(orderId, "capture_after_void")
      first <- resolve(
        routes(store),
        orderId,
        Some("Bearer test-token"),
        ResolveReconciliationRequest("resolved", Some("refunded"))
      )
      firstBody <- first.as[ReconciliationResponse]
      second <- resolve(
        routes(store),
        orderId,
        Some("Bearer test-token"),
        ResolveReconciliationRequest("resolved", Some("later"))
      )
      secondBody <- second.as[ReconciliationResponse]
    } yield {
      assertEquals(first.status, Status.Ok)
      assertEquals(firstBody.status, "resolved")
      assertEquals(firstBody.operatorId, Some("op-1"))
      assertEquals(firstBody.note, Some("refunded"))
      assertEquals(second.status, Status.Ok)
      assertEquals(secondBody.note, Some("refunded"))
      assertEquals(secondBody.resolvedAt, firstBody.resolvedAt)
    }
  }

  test("PATCH /reconciliations validates status and unknown flags") {
    for {
      store <- PaymentStore.inMemory[IO]
      orderId <- seeded(store)
      _ <- store.flagReconciliation(orderId, "capture_after_void")
      invalid <- resolve(
        routes(store),
        orderId,
        Some("Bearer test-token"),
        ResolveReconciliationRequest("open")
      )
      missing <- resolve(
        routes(store),
        OrderId.parse("no-flag").getOrElse(fail("bad id")),
        Some("Bearer test-token"),
        ResolveReconciliationRequest("resolved")
      )
    } yield {
      assertEquals(invalid.status, Status.BadRequest)
      assertEquals(missing.status, Status.NotFound)
    }
  }
}
