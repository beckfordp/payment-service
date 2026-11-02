package paymentservice

import cats.Monad
import cats.syntax.all._
import org.typelevel.log4cats.StructuredLogger
import paymentservice.domain.{OrderId, PaymentStatus}
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint

/** ADM-5 expiry-trigger API errors. */
sealed trait ExpiryTriggerError

case object ExpiryPaymentNotFound extends ExpiryTriggerError
case object ExpiryUnauthorized extends ExpiryTriggerError
final case class ExpiryNotAuthorized(status: String) extends ExpiryTriggerError

/** ADM-5: a dev/demo operator trigger that expires one authorization now, so
  * the admin console can demonstrate the US-20.1 sweep without SQL. It only
  * pulls `expires_at` forward and then runs one sweep tick: the void,
  * `PaymentAuthorizationExpired` outbox row and downstream fan-out are the real
  * sweep path, not a parallel one.
  */
object ExpiryTriggerRoutes {

  private val tag = "payments"

  private val examplePayment = PaymentResponse(
    "9b2f1c3d-4e5a-4b6c-8d7e-9f0a1b2c3d4e",
    "3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f",
    4999,
    "voided",
    java.time.Instant.parse("2026-11-01T12:00:00Z"),
    java.time.Instant.parse("2026-11-02T09:30:00Z")
  )

  private val orderIdPath: EndpointInput.PathCapture[String] =
    path[String]("orderId")
      .description("Order id whose authorization to expire.")
      .example("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")

  private def errorBody(
      what: String,
      example: ErrorResponse
  ): EndpointIO.Body[String, ErrorResponse] =
    jsonBody[ErrorResponse].description(what).example(example)

  private val notFoundVariant
      : EndpointOutput.OneOfVariant[ExpiryPaymentNotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(
          errorBody(
            "The order has no payment.",
            ErrorResponse("Payment not found")
          )
        )
        .map(_ => ExpiryPaymentNotFound)(_ =>
          ErrorResponse("Payment not found")
        )
    )

  private val unauthorizedVariant
      : EndpointOutput.OneOfVariant[ExpiryUnauthorized.type] =
    oneOfVariant(
      statusCode(StatusCode.Unauthorized)
        .and(
          errorBody(
            "The dev operator token is absent, malformed or does not match.",
            ErrorResponse("Invalid operator token")
          )
        )
        .map(_ => ExpiryUnauthorized)(_ =>
          ErrorResponse("Invalid operator token")
        )
    )

  private val conflictVariant
      : EndpointOutput.OneOfVariant[ExpiryNotAuthorized] =
    oneOfVariant(
      statusCode(StatusCode.Conflict)
        .and(
          errorBody(
            "The payment is not a live authorization (already captured, voided or failed).",
            ErrorResponse("Payment is captured, not authorized")
          )
        )
        .map(body =>
          ExpiryNotAuthorized(
            body.error
              .stripPrefix("Payment is ")
              .stripSuffix(", not authorized")
          )
        )(error => ErrorResponse(s"Payment is ${error.status}, not authorized"))
    )

  private val expireEndpoint: PublicEndpoint[
    (String, Option[String]),
    ExpiryTriggerError,
    PaymentResponse,
    Any
  ] =
    endpoint.post
      .in("payments" / "authorizations" / orderIdPath / "expire")
      .in(
        header[Option[String]]("Authorization")
          .description("Dev operator token: `Bearer <token>` (ADR 0024).")
      )
      .out(jsonBody[PaymentResponse].example(examplePayment))
      .errorOut(
        oneOf[ExpiryTriggerError](
          notFoundVariant,
          unauthorizedVariant,
          conflictVariant
        )
      )
      .summary("Expire an authorization now (dev/demo)")
      .description(
        "Simulates authorization expiry for one order (ADM-5): pulls the hold's expiry to now and runs one sweep tick, so the payment is voided and `PaymentAuthorizationExpired` is published through the normal US-20.1 path. Requires the dev operator token (401). 404 when the order has no payment, 409 when it is not `authorized`."
      )
      .tag(tag)

  def expireServerEndpoint[F[_]: Monad](
      store: PaymentStore[F],
      operatorAuth: OperatorAuth,
      logger: StructuredLogger[F],
      sweepBatchSize: Int
  ): ServerEndpoint[Any, F] =
    expireEndpoint.serverLogic[F] { case (rawOrderId, authorization) =>
      val context = Map(
        "method" -> "POST",
        "path" -> s"/payments/authorizations/$rawOrderId/expire",
        "order_id" -> rawOrderId
      )
      def notAuthorized(
          payment: Payment
      ): F[Either[ExpiryTriggerError, PaymentResponse]] =
        logger
          .warn(context + ("status" -> payment.status.asString))(
            "Payment is not authorized"
          )
          .as(Left(ExpiryNotAuthorized(payment.status.asString)))

      def notFound: F[Either[ExpiryTriggerError, PaymentResponse]] =
        logger
          .warn(context)("Payment not found")
          .as(Left(ExpiryPaymentNotFound))

      operatorAuth.requireOperator(authorization) match {
        case Left(_) =>
          logger
            .warn(context)("Operator token rejected")
            .as(Left(ExpiryUnauthorized))
        case Right(operatorId) =>
          OrderId.parse(rawOrderId) match {
            case Left(_)        => notFound
            case Right(orderId) =>
              logger.info(context + ("operator_id" -> operatorId))(
                "Received request"
              ) *> store.expireAuthorizationNow(orderId).flatMap {
                case Some(_) =>
                  ExpirySweep.sweepOnce(store, logger, sweepBatchSize) *>
                    store.findByOrderId(orderId).flatMap {
                      case Some(payment) =>
                        logger
                          .info(
                            context + ("status" -> payment.status.asString)
                          )(
                            "Authorization expired on operator request"
                          )
                          .as(Right(PaymentResponse(payment)))
                      case None => notFound
                    }
                case None =>
                  store.findByOrderId(orderId).flatMap {
                    case Some(payment)
                        if payment.status != PaymentStatus.Authorized =>
                      notAuthorized(payment)
                    case _ => notFound
                  }
              }
          }
      }
    }
}
