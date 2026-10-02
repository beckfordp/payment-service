package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import purerest.docs.Docs

class PaymentDocsSuite extends CatsEffectSuite {

  test(
    "the tapir-described endpoint is served and documented via purerest.docs"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      endpoint = PaymentRoutes.serverEndpoint[IO](store, NoOpLogger[IO])
      routes = Docs.routes[IO]("Payment Service", "1.0", List(endpoint))
      request = Request[IO](Method.POST, uri"/payments")
        .withEntity(CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999))
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
}
