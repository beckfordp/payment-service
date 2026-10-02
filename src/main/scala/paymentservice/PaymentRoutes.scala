package paymentservice

import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.HttpRoutes
import org.typelevel.log4cats.StructuredLogger
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

final case class CreatePaymentRequest(orderId: String, amountCents: Int)

object CreatePaymentRequest {
  implicit val codec: Codec[CreatePaymentRequest] = deriveCodec
}

final case class UpdatePaymentRequest(status: String)

object UpdatePaymentRequest {
  implicit val codec: Codec[UpdatePaymentRequest] = deriveCodec
}

final case class PaymentResponse(
    id: String,
    orderId: String,
    amountCents: Int,
    status: String,
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

object PaymentResponse {
  implicit val codec: Codec[PaymentResponse] = deriveCodec

  def apply(entity: Payment): PaymentResponse =
    PaymentResponse(
      entity.id,
      entity.orderId,
      entity.amountCents,
      entity.status,
      entity.createdAt,
      entity.updatedAt
    )
}

final case class ErrorResponse(error: String)

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

object PaymentRoutes {

  private val createPaymentEndpoint: PublicEndpoint[
    CreatePaymentRequest,
    Unit,
    PaymentResponse,
    Any
  ] =
    endpoint.post
      .in("payments")
      .in(jsonBody[CreatePaymentRequest])
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[PaymentResponse])

  private val notFoundOutput: EndpointOutput[PaymentError] =
    statusCode(StatusCode.NotFound)
      .and(jsonBody[ErrorResponse])
      .map[PaymentError](_ => PaymentNotFound)(_ =>
        ErrorResponse("Payment not found")
      )

  private val getPaymentEndpoint: PublicEndpoint[
    String,
    PaymentError,
    PaymentResponse,
    Any
  ] =
    endpoint.get
      .in("payments" / path[String]("id"))
      .out(jsonBody[PaymentResponse])
      .errorOut(notFoundOutput)

  private val updatePaymentEndpoint: PublicEndpoint[
    (String, UpdatePaymentRequest),
    PaymentError,
    PaymentResponse,
    Any
  ] =
    endpoint.patch
      .in("payments" / path[String]("id"))
      .in(jsonBody[UpdatePaymentRequest])
      .out(jsonBody[PaymentResponse])
      .errorOut(notFoundOutput)

  private val replacePaymentEndpoint: PublicEndpoint[
    (String, UpdatePaymentRequest),
    PaymentError,
    PaymentResponse,
    Any
  ] =
    endpoint.put
      .in("payments" / path[String]("id"))
      .in(jsonBody[UpdatePaymentRequest])
      .out(jsonBody[PaymentResponse])
      .errorOut(notFoundOutput)

  private val deletePaymentEndpoint
      : PublicEndpoint[String, PaymentError, Unit, Any] =
    endpoint.delete
      .in("payments" / path[String]("id"))
      .out(statusCode(StatusCode.NoContent))
      .errorOut(notFoundOutput)

  def serverEndpoint[F[_]: Async](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    createPaymentEndpoint.serverLogicSuccess[F] { req =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "POST",
            "path" -> "/payments"
          )
        )("Received request")
        entity <- store.create(req.orderId, req.amountCents).onError {
          case error =>
            logger.error(Map.empty, error)("Persisting the payment failed")
        }
        _ <- logger.info(
          Map("payment_id" -> entity.id)
        )("Request completed")
      } yield PaymentResponse(entity)
    }

  def getPaymentServerEndpoint[F[_]: Async](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    getPaymentEndpoint.serverLogic[F] { id =>
      for {
        _ <- logger.info(
          Map("method" -> "GET", "path" -> s"/payments/$id", "payment_id" -> id)
        )(
          "Received request"
        )
        result <- store.get(id).flatMap {
          case Some(entity) =>
            logger
              .info(Map("payment_id" -> id))("Request completed")
              .as(Right(PaymentResponse(entity)))
          case None =>
            logger
              .warn(Map("payment_id" -> id))("Payment not found")
              .as(Left(PaymentNotFound))
        }
      } yield result
    }

  /** Shared handler for `PATCH` (partial update) and `PUT` (full replace) —
    * both call `PaymentStore.update` with the same required update body; only
    * the logged HTTP method differs.
    */
  private def updateLogic[F[_]: Async](
      store: PaymentStore[F],
      logger: StructuredLogger[F],
      httpMethod: String
  )(
      id: String,
      req: UpdatePaymentRequest
  ): F[Either[PaymentError, PaymentResponse]] =
    for {
      _ <- logger.info(
        Map(
          "method" -> httpMethod,
          "path" -> s"/payments/$id",
          "payment_id" -> id
        )
      )("Received request")
      result <- store.update(id, req.status).flatMap {
        case Some(entity) =>
          logger
            .info(Map("payment_id" -> id))("Request completed")
            .as(Right(PaymentResponse(entity)))
        case None =>
          logger
            .warn(Map("payment_id" -> id))("Payment not found")
            .as(Left(PaymentNotFound))
      }
    } yield result

  def updatePaymentServerEndpoint[F[_]: Async](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    updatePaymentEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PATCH")(id, req)
    }

  def replacePaymentServerEndpoint[F[_]: Async](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    replacePaymentEndpoint.serverLogic[F] { case (id, req) =>
      updateLogic(store, logger, "PUT")(id, req)
    }

  def deletePaymentServerEndpoint[F[_]: Async](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    deletePaymentEndpoint.serverLogic[F] { id =>
      for {
        _ <- logger.info(
          Map(
            "method" -> "DELETE",
            "path" -> s"/payments/$id",
            "payment_id" -> id
          )
        )("Received request")
        result <- store.delete(id).flatMap {
          case true =>
            logger
              .info(Map("payment_id" -> id))("Request completed")
              .as(Right(()))
          case false =>
            logger
              .warn(Map("payment_id" -> id))("Payment not found")
              .as(Left(PaymentNotFound))
        }
      } yield result
    }

  def routes[F[_]: Async](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      List(
        serverEndpoint(store, logger),
        getPaymentServerEndpoint(store, logger),
        updatePaymentServerEndpoint(store, logger),
        replacePaymentServerEndpoint(store, logger),
        deletePaymentServerEndpoint(store, logger)
      )
    )
}
