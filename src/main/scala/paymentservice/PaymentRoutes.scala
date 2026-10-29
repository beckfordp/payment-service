package paymentservice

import cats.{Monad, MonadThrow}
import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import paymentservice.domain.{OrderId, PaymentId}
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
      PaymentId.render(entity.id),
      OrderId.render(entity.orderId),
      entity.amountCents,
      entity.status.asString,
      entity.createdAt,
      entity.updatedAt
    )
}

final case class ErrorResponse(error: String)

sealed trait CreatePaymentError

object CreatePaymentError {
  final case class Invalid(error: String) extends CreatePaymentError
  final case class Conflict(error: String) extends CreatePaymentError
}

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

object PaymentRoutes {

  private val createPaymentEndpoint: PublicEndpoint[
    CreatePaymentRequest,
    CreatePaymentError,
    PaymentResponse,
    Any
  ] =
    endpoint.post
      .in("payments")
      .in(jsonBody[CreatePaymentRequest])
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[PaymentResponse])
      .errorOut(
        oneOf[CreatePaymentError](
          oneOfVariant(
            statusCode(StatusCode.BadRequest)
              .and(jsonBody[ErrorResponse])
              .map[CreatePaymentError.Invalid](e =>
                CreatePaymentError.Invalid(e.error)
              )(e => ErrorResponse(e.error))
          ),
          oneOfVariant(
            statusCode(StatusCode.Conflict)
              .and(jsonBody[ErrorResponse])
              .map[CreatePaymentError.Conflict](e =>
                CreatePaymentError.Conflict(e.error)
              )(e => ErrorResponse(e.error))
          )
        )
      )

  private def validate(req: CreatePaymentRequest): Either[String, OrderId] =
    for {
      orderId <- OrderId.parse(req.orderId)
      _ <- Either.cond(req.amountCents > 0, (), "amountCents must be positive")
    } yield orderId

  private val notFoundVariant
      : EndpointOutput.OneOfVariant[PaymentNotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(jsonBody[ErrorResponse])
        .map[PaymentNotFound.type](_ => PaymentNotFound)(_ =>
          ErrorResponse("Payment not found")
        )
    )

  private val paymentErrorOutput: EndpointOutput[PaymentError] =
    oneOf[PaymentError](notFoundVariant)

  private val getPaymentEndpoint: PublicEndpoint[
    PaymentId,
    PaymentError,
    PaymentResponse,
    Any
  ] =
    endpoint.get
      .in("payments" / path[PaymentId]("id"))
      .out(jsonBody[PaymentResponse])
      .errorOut(paymentErrorOutput)

  private val deletePaymentEndpoint
      : PublicEndpoint[PaymentId, PaymentError, Unit, Any] =
    endpoint.delete
      .in("payments" / path[PaymentId]("id"))
      .out(statusCode(StatusCode.NoContent))
      .errorOut(paymentErrorOutput)

  def serverEndpoint[F[_]: MonadThrow](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    createPaymentEndpoint.serverLogic[F] { req =>
      def create(
          orderId: OrderId
      ): F[Either[CreatePaymentError, PaymentResponse]] =
        store
          .create(orderId, req.amountCents)
          .attemptNarrow[PaymentAlreadyExists]
          .flatMap {
            case Right(entity) =>
              logger
                .info(Map("payment_id" -> PaymentId.render(entity.id)))(
                  "Request completed"
                )
                .as(Right(PaymentResponse(entity)))
            case Left(conflict) =>
              logger
                .warn(Map("order_id" -> req.orderId))(
                  "Order already has a payment"
                )
                .as(Left(CreatePaymentError.Conflict(conflict.getMessage)))
          }
          .onError { case error =>
            logger.error(Map.empty, error)("Persisting the payment failed")
          }

      logger.info(
        Map(
          "method" -> "POST",
          "path" -> "/payments"
        )
      )("Received request") *> (validate(req) match {
        case Left(error) =>
          logger
            .warn(Map("error" -> error))("Invalid payment request")
            .as(Left(CreatePaymentError.Invalid(error)))
        case Right(orderId) => create(orderId)
      })
    }

  def getPaymentServerEndpoint[F[_]: Monad](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    getPaymentEndpoint.serverLogic[F] { id =>
      val renderedId = PaymentId.render(id)
      for {
        _ <- logger.info(
          Map(
            "method" -> "GET",
            "path" -> s"/payments/$renderedId",
            "payment_id" -> renderedId
          )
        )("Received request")
        result <- store.get(id).flatMap {
          case Some(entity) =>
            logger
              .info(Map("payment_id" -> renderedId))("Request completed")
              .as(Right(PaymentResponse(entity)))
          case None =>
            logger
              .warn(Map("payment_id" -> renderedId))("Payment not found")
              .as(Left(PaymentNotFound))
        }
      } yield result
    }

  def deletePaymentServerEndpoint[F[_]: Monad](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    deletePaymentEndpoint.serverLogic[F] { id =>
      val renderedId = PaymentId.render(id)
      for {
        _ <- logger.info(
          Map(
            "method" -> "DELETE",
            "path" -> s"/payments/$renderedId",
            "payment_id" -> renderedId
          )
        )("Received request")
        result <- store.delete(id).flatMap {
          case true =>
            logger
              .info(Map("payment_id" -> renderedId))("Request completed")
              .as(Right(()))
          case false =>
            logger
              .warn(Map("payment_id" -> renderedId))("Payment not found")
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
        deletePaymentServerEndpoint(store, logger)
      )
    )
}
