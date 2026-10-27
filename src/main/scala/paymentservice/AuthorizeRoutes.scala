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

import java.time.Instant

final case class AuthorizePaymentRequest(
    orderId: String,
    amountCents: Int,
    paymentMethodId: String
)

object AuthorizePaymentRequest {
  implicit val codec: Codec[AuthorizePaymentRequest] = deriveCodec
}

final case class AuthorizePaymentResponse(authId: String, expiresAt: Instant)

object AuthorizePaymentResponse {
  implicit val codec: Codec[AuthorizePaymentResponse] = deriveCodec
}

final case class DeclinedResponse(error: String, reason: String)

object DeclinedResponse {
  implicit val codec: Codec[DeclinedResponse] = deriveCodec
}

sealed trait AuthorizeError

object AuthorizeError {
  final case class Invalid(error: String) extends AuthorizeError
  case object MethodNotFound extends AuthorizeError
  final case class Declined(reason: String) extends AuthorizeError
}

/** order-service-facing synchronous `POST /payments/authorize` (US-14.1) -
  * contract pinned in `gluon/docs/system-design.md`'s "REST contracts". Success
  * also writes `payment-events: PaymentAuthorized` via the outbox; a decline is
  * answered here and publishes nothing.
  */
object AuthorizeRoutes {

  private val invalidVariant
      : EndpointOutput.OneOfVariant[AuthorizeError.Invalid] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(jsonBody[ErrorResponse])
        .map[AuthorizeError.Invalid](e => AuthorizeError.Invalid(e.error))(e =>
          ErrorResponse(e.error)
        )
    )

  private val notFoundVariant
      : EndpointOutput.OneOfVariant[AuthorizeError.MethodNotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(jsonBody[ErrorResponse])
        .map[AuthorizeError.MethodNotFound.type](_ =>
          AuthorizeError.MethodNotFound
        )(_ => ErrorResponse("Payment method not found"))
    )

  private val declinedVariant
      : EndpointOutput.OneOfVariant[AuthorizeError.Declined] =
    oneOfVariant(
      statusCode(StatusCode.PaymentRequired)
        .and(jsonBody[DeclinedResponse])
        .map[AuthorizeError.Declined](d => AuthorizeError.Declined(d.reason))(
          d => DeclinedResponse("Payment declined", d.reason)
        )
    )

  private val authorizeEndpoint: PublicEndpoint[
    AuthorizePaymentRequest,
    AuthorizeError,
    AuthorizePaymentResponse,
    Any
  ] =
    endpoint.post
      .in("payments" / "authorize")
      .in(jsonBody[AuthorizePaymentRequest])
      .out(jsonBody[AuthorizePaymentResponse])
      .errorOut(
        oneOf[AuthorizeError](invalidVariant, notFoundVariant, declinedVariant)
      )

  private def validate(req: AuthorizePaymentRequest): Either[String, Unit] =
    for {
      _ <- Either.cond(req.orderId.trim.nonEmpty, (), "orderId is required")
      _ <- Either.cond(req.amountCents > 0, (), "amountCents must be positive")
    } yield ()

  def authorizeServerEndpoint[F[_]: Async](
      payments: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    authorizeEndpoint.serverLogic[F] { req =>
      val context = Map(
        "order_id" -> req.orderId,
        "payment_method_id" -> req.paymentMethodId
      )
      def authorizeWith(
          method: PaymentMethod
      ): F[Either[AuthorizeError, AuthorizePaymentResponse]] =
        gateway.authorize(method.cardToken, req.amountCents).flatMap {
          case Right(authorization) =>
            payments
              .recordAuthorization(
                req.orderId,
                req.amountCents,
                method.id,
                authorization
              )
              // Don't leave an unrecorded hold at the gateway if persisting
              // the authorization fails - release it before the 500.
              .onError { case _ => gateway.void(authorization.authId) }
              .flatMap { payment =>
                logger
                  .info(context + ("payment_id" -> payment.id))(
                    "Payment authorized"
                  )
                  .as(
                    Right(
                      AuthorizePaymentResponse(
                        authorization.authId,
                        authorization.expiresAt
                      )
                    )
                  )
              }
          case Left(decline) =>
            payments
              .recordDecline(
                req.orderId,
                req.amountCents,
                method.id,
                decline
              )
              .flatMap { payment =>
                logger
                  .warn(
                    context ++ Map(
                      "payment_id" -> payment.id,
                      "reason" -> decline.value
                    )
                  )("Payment declined")
                  .as(Left(AuthorizeError.Declined(decline.value)))
              }
        }

      for {
        _ <- logger.info(
          context ++ Map("method" -> "POST", "path" -> "/payments/authorize")
        )("Received request")
        result <- validate(req) match {
          case Left(error) =>
            logger
              .warn(context + ("error" -> error))("Invalid authorize request")
              .as(Left(AuthorizeError.Invalid(error)))
          case Right(()) =>
            methods.get(req.paymentMethodId).flatMap {
              case None =>
                logger
                  .warn(context)("Payment method not found")
                  .as(Left(AuthorizeError.MethodNotFound))
              case Some(method) =>
                authorizeWith(method).onError { case error =>
                  logger.error(context, error)("Authorizing the payment failed")
                }
            }
        }
      } yield result
    }

  def routes[F[_]: Async](
      payments: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      authorizeServerEndpoint(payments, methods, gateway, logger)
    )
}
