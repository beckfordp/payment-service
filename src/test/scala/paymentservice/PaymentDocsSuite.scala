package paymentservice

import paymentservice.TestConversions.given
import scala.language.implicitConversions
import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import purerest.docs.Docs
import sttp.tapir.EndpointOutput
import sttp.tapir.server.ServerEndpoint

class PaymentDocsSuite extends CatsEffectSuite {

  test(
    "the tapir-described endpoint is served and documented via purerest.docs"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      endpoint = PaymentRoutes.serverEndpoint[IO](store, NoOpLogger[IO])
      routes = Docs.routes[IO]("Payment Service", "1.0", List(endpoint))
      request = Request[IO](Method.POST, uri"/payments")
        .withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      response <- routes.orNotFound.run(request)
      entity <- response.as[PaymentResponse]
      docsResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/docs.yaml")
      )
      docsBody <- docsResponse.bodyText.compile.string
    } yield {
      assertEquals(response.status, Status.Created)
      assert(entity.id.nonEmpty)
      assertEquals(docsResponse.status, Status.Ok)
      assert(clue(docsBody).contains("/payments"))
    }
  }

  test("POST /payments/methods is documented via purerest.docs") {
    for {
      methods <- PaymentMethodStore.inMemory[IO]
      routes = Docs.routes[IO](
        "Payment Service",
        "1.0",
        List(
          PaymentMethodRoutes
            .createMethodServerEndpoint[IO](methods, NoOpLogger[IO])
        )
      )
      docsResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/docs.yaml")
      )
      docsBody <- docsResponse.bodyText.compile.string
    } yield assert(clue(docsBody).contains("/payments/methods"))
  }

  test("POST /payments/authorize is documented with its 402 decline") {
    for {
      payments <- PaymentStore.inMemory[IO]
      methods <- PaymentMethodStore.inMemory[IO]
      gateway = PaymentGatewayClient.simulated[IO](
        SimulatedGatewayConfig(
          GatewayOutcome.Approve,
          Map.empty,
          GatewayOutcome.Approve,
          Map.empty,
          scala.concurrent.duration.Duration(1, "day")
        )
      )
      routes = Docs.routes[IO](
        "Payment Service",
        "1.0",
        List(
          AuthorizeRoutes.authorizeServerEndpoint[IO](
            payments,
            methods,
            gateway,
            NoOpLogger[IO]
          )
        )
      )
      docsResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/docs/docs.yaml")
      )
      docsBody <- docsResponse.bodyText.compile.string
    } yield {
      assert(clue(docsBody).contains("/payments/authorize"))
      assert(clue(docsBody).contains("'402'"))
    }
  }

  private val docsGateway = PaymentGatewayClient.simulated[IO](
    SimulatedGatewayConfig(
      GatewayOutcome.Approve,
      Map.empty,
      GatewayOutcome.Approve,
      Map.empty,
      scala.concurrent.duration.Duration(1, "day")
    )
  )

  /** Every fixed status an output can produce (tapir's own traversal is
    * internal).
    */
  private def statuses(output: EndpointOutput[?]): List[Int] =
    output match {
      case EndpointOutput.FixedStatusCode(code, _, _) => List(code.code)
      case EndpointOutput.OneOf(variants, _)          =>
        variants.flatMap(v => statuses(v.output))
      case EndpointOutput.Pair(left, right, _, _) =>
        statuses(left) ++ statuses(right)
      case EndpointOutput.MappedPair(pair, _) => statuses(pair)
      case _                                  => Nil
    }

  private def errorStatuses(endpoint: ServerEndpoint[Any, IO]): List[Int] =
    statuses(endpoint.endpoint.errorOutput).sorted

  /** Exactly the endpoints `AppRoutes` passes to `Docs.routes`. */
  private def documentedEndpoints(
      store: PaymentStore[IO],
      methods: PaymentMethodStore[IO]
  ): List[ServerEndpoint[Any, IO]] = {
    val logger = NoOpLogger[IO]
    List(
      PaymentRoutes.serverEndpoint[IO](store, logger),
      PaymentRoutes.getPaymentServerEndpoint[IO](store, logger),
      PaymentRoutes.deletePaymentServerEndpoint[IO](store, logger),
      PaymentMethodRoutes.createMethodServerEndpoint[IO](methods, logger),
      AuthorizeRoutes.authorizeServerEndpoint[IO](
        store,
        methods,
        docsGateway,
        logger
      ),
      ReconciliationRoutes.listServerEndpoint[IO](store, logger),
      ReconciliationRoutes.resolveServerEndpoint[IO](
        store,
        OperatorAuth.disabled,
        logger
      ),
      ExpiryTriggerRoutes.expireServerEndpoint[IO](
        store,
        OperatorAuth.disabled,
        logger,
        100
      ),
      HealthRoutes.healthServerEndpoint[IO],
      HealthRoutes.readyServerEndpoint[IO](store)
    )
  }

  test("each endpoint documents only the errors it can return") {
    for {
      store <- PaymentStore.inMemory[IO]
      methods <- PaymentMethodStore.inMemory[IO]
    } yield {
      val endpoints = documentedEndpoints(store, methods)
      val expected = List(
        List(400, 409), // POST /payments
        List(404), // GET /payments/{id}
        List(404), // DELETE /payments/{id}
        List(400), // POST /payments/methods
        List(400, 402, 404, 409, 500), // POST /payments/authorize
        List(400), // GET /reconciliations
        List(400, 401, 404), // PATCH /reconciliations/{orderId}
        List(401, 404, 409), // POST /payments/authorizations/{orderId}/expire
        Nil, // GET /health
        List(503) // GET /health/ready
      )
      assertEquals(endpoints.map(errorStatuses), expected)
    }
  }

  test("every endpoint is documented") {
    for {
      store <- PaymentStore.inMemory[IO]
      methods <- PaymentMethodStore.inMemory[IO]
    } yield assertEquals(
      Docs
        .undocumented(documentedEndpoints(store, methods).map(_.endpoint))
        .map(_.render),
      Nil
    )
  }
}
