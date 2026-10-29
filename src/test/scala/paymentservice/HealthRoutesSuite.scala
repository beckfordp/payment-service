package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import paymentservice.domain.{PaymentId, PaymentMethodId}
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}

class HealthRoutesSuite extends CatsEffectSuite {

  private val readyStore: PaymentStore[IO] =
    new PaymentStore[IO] {
      def create(orderId: String, amountCents: Int): IO[Payment] =
        IO.raiseError(new NotImplementedError())
      def get(id: PaymentId): IO[Option[Payment]] = IO.pure(None)
      def update(
          id: PaymentId,
          status: PaymentStatus
      ): IO[Option[Payment]] = IO.pure(None)
      def delete(id: PaymentId): IO[Boolean] = IO.pure(false)
      def ping: IO[Boolean] = IO.pure(true)
      def findByOrderId(orderId: String): IO[Option[Payment]] =
        IO.pure(None)
      def recordAuthorization(
          orderId: String,
          amountCents: Int,
          paymentMethodId: PaymentMethodId,
          authorization: Authorization
      ): IO[Option[Payment]] = IO.raiseError(new NotImplementedError)
      def recordDecline(
          orderId: String,
          amountCents: Int,
          paymentMethodId: PaymentMethodId,
          reason: DeclineReason
      ): IO[Option[Payment]] = IO.raiseError(new NotImplementedError)
      def recordCapture(orderId: String): IO[Option[Payment]] =
        IO.raiseError(new NotImplementedError)
      def recordCaptureFailure(
          orderId: String,
          reason: String
      ): IO[Option[Payment]] = IO.raiseError(new NotImplementedError)
    }

  private val notReadyStore: PaymentStore[IO] =
    new PaymentStore[IO] {
      def create(orderId: String, amountCents: Int): IO[Payment] =
        IO.raiseError(new NotImplementedError())
      def get(id: PaymentId): IO[Option[Payment]] = IO.pure(None)
      def update(
          id: PaymentId,
          status: PaymentStatus
      ): IO[Option[Payment]] = IO.pure(None)
      def delete(id: PaymentId): IO[Boolean] = IO.pure(false)
      def ping: IO[Boolean] = IO.pure(false)
      def findByOrderId(orderId: String): IO[Option[Payment]] =
        IO.pure(None)
      def recordAuthorization(
          orderId: String,
          amountCents: Int,
          paymentMethodId: PaymentMethodId,
          authorization: Authorization
      ): IO[Option[Payment]] = IO.raiseError(new NotImplementedError)
      def recordDecline(
          orderId: String,
          amountCents: Int,
          paymentMethodId: PaymentMethodId,
          reason: DeclineReason
      ): IO[Option[Payment]] = IO.raiseError(new NotImplementedError)
      def recordCapture(orderId: String): IO[Option[Payment]] =
        IO.raiseError(new NotImplementedError)
      def recordCaptureFailure(
          orderId: String,
          reason: String
      ): IO[Option[Payment]] = IO.raiseError(new NotImplementedError)
    }

  test("GET /health returns 200") {
    val routes = HealthRoutes.routes[IO](readyStore)
    for {
      response <- routes.orNotFound.run(Request[IO](Method.GET, uri"/health"))
    } yield assertEquals(response.status, Status.Ok)
  }

  test("GET /health/ready returns 200 when the store is ready") {
    val routes = HealthRoutes.routes[IO](readyStore)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
    } yield assertEquals(response.status, Status.Ok)
  }

  test("GET /health/ready returns 503 when the store is not ready") {
    val routes = HealthRoutes.routes[IO](notReadyStore)
    for {
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/health/ready")
      )
    } yield assertEquals(response.status, Status.ServiceUnavailable)
  }
}
