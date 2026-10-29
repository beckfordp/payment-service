package paymentservice

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import paymentservice.domain.{PaymentId, PaymentMethodId}
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
  final case class AlreadyProcessed(error: String) extends AuthorizeError
  final case class Internal(error: String) extends AuthorizeError
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

  private val alreadyProcessedVariant
      : EndpointOutput.OneOfVariant[AuthorizeError.AlreadyProcessed] =
    oneOfVariant(
      statusCode(StatusCode.Conflict)
        .and(jsonBody[ErrorResponse])
        .map[AuthorizeError.AlreadyProcessed](e =>
          AuthorizeError.AlreadyProcessed(e.error)
        )(e => ErrorResponse(e.error))
    )

  private val internalVariant
      : EndpointOutput.OneOfVariant[AuthorizeError.Internal] =
    oneOfVariant(
      statusCode(StatusCode.InternalServerError)
        .and(jsonBody[ErrorResponse])
        .map[AuthorizeError.Internal](e => AuthorizeError.Internal(e.error))(
          e => ErrorResponse(e.error)
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
        oneOf[AuthorizeError](
          invalidVariant,
          notFoundVariant,
          declinedVariant,
          alreadyProcessedVariant,
          internalVariant
        )
      )

  private def validate(req: AuthorizePaymentRequest): Either[String, Unit] =
    for {
      _ <- Either.cond(req.orderId.trim.nonEmpty, (), "orderId is required")
      _ <- Either.cond(req.amountCents > 0, (), "amountCents must be positive")
    } yield ()

  def authorizeServerEndpoint[F[_]: MonadThrow](
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

      /** A stored outcome for this order is replayed instead of placing a
        * second hold (TD-10.1). Different amount or method is always a `409`; a
        * replayed decline is a `402` with the original reason.
        */
      def replay(
          existing: Payment,
          methodId: PaymentMethodId
      ): Either[AuthorizeError, AuthorizePaymentResponse] =
        if (
          existing.amountCents != req.amountCents ||
          !existing.paymentMethodId.contains(methodId)
        )
          Left(
            AuthorizeError.AlreadyProcessed(
              "Order already has a payment with a different amount or payment method"
            )
          )
        else
          existing.status match {
            case PaymentStatus.Authorized =>
              (existing.authId, existing.expiresAt) match {
                case (Some(authId), Some(expiresAt)) =>
                  Right(AuthorizePaymentResponse(authId, expiresAt))
                case _ =>
                  Left(
                    AuthorizeError.Internal(
                      "Stored authorization is incomplete"
                    )
                  )
              }
            case PaymentStatus.Failed =>
              existing.declineReason match {
                case Some(reason) => Left(AuthorizeError.Declined(reason))
                case None         =>
                  Left(
                    AuthorizeError.Internal("Stored decline has no reason")
                  )
              }
            case status @ (PaymentStatus.Pending | PaymentStatus.Captured |
                PaymentStatus.Voided) =>
              Left(
                AuthorizeError.AlreadyProcessed(
                  s"Order already has a payment in status ${status.asString}"
                )
              )
          }

      def replayExisting(
          existing: Payment,
          methodId: PaymentMethodId
      ): F[Either[AuthorizeError, AuthorizePaymentResponse]] =
        logger
          .info(context + ("payment_id" -> PaymentId.render(existing.id)))(
            "Replaying the stored payment outcome"
          )
          .as(replay(existing, methodId))

      /** A concurrent call inserted the row first: release this call's fresh
        * hold (best-effort; the sweep covers a failed void) and replay the
        * winner.
        */
      def replayAfterConflict(
          voidedAuthId: Option[String],
          methodId: PaymentMethodId
      ): F[Either[AuthorizeError, AuthorizePaymentResponse]] = {
        val release = voidedAuthId match {
          case None         => MonadThrow[F].unit
          case Some(authId) =>
            gateway.void(authId).handleErrorWith { error =>
              logger.warn(context + ("auth_id" -> authId), error)(
                "Releasing the losing authorization failed"
              )
            }
        }
        release *> payments.findByOrderId(req.orderId).flatMap {
          case Some(existing) => replayExisting(existing, methodId)
          case None           =>
            logger
              .error(context)(
                "Payment row disappeared after an insert conflict"
              )
              .as(
                Left(
                  AuthorizeError.Internal(
                    "Payment could not be recorded or read back"
                  )
                )
              )
        }
      }

      def authorizeWith(
          method: PaymentMethod
      ): F[Either[AuthorizeError, AuthorizePaymentResponse]] = {
        val methodId = method.id
        payments.findByOrderId(req.orderId).flatMap {
          case Some(existing) => replayExisting(existing, methodId)
          case None           =>
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
                  .flatMap {
                    case Some(payment) =>
                      logger
                        .info(
                          context + (
                            "payment_id" -> PaymentId.render(payment.id)
                          )
                        )(
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
                    case None =>
                      replayAfterConflict(Some(authorization.authId), methodId)
                  }
              case Left(decline) =>
                payments
                  .recordDecline(
                    req.orderId,
                    req.amountCents,
                    method.id,
                    decline
                  )
                  .flatMap {
                    case Some(payment) =>
                      logger
                        .warn(
                          context ++ Map(
                            "payment_id" -> PaymentId.render(payment.id),
                            "reason" -> decline.value
                          )
                        )("Payment declined")
                        .as(Left(AuthorizeError.Declined(decline.value)))
                    case None => replayAfterConflict(None, methodId)
                  }
            }
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
            PaymentMethodId.parse(req.paymentMethodId) match {
              case Left(error) =>
                logger
                  .warn(context + ("error" -> error))(
                    "Invalid payment method id"
                  )
                  .as(Left(AuthorizeError.Invalid(error)))
              case Right(methodId) =>
                methods.get(methodId).flatMap {
                  case None =>
                    logger
                      .warn(context)("Payment method not found")
                      .as(Left(AuthorizeError.MethodNotFound))
                  case Some(method) =>
                    authorizeWith(method).onError { case error =>
                      logger.error(context, error)(
                        "Authorizing the payment failed"
                      )
                    }
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
