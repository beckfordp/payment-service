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

import java.time.{YearMonth, ZoneOffset}

/** `cardNumber` is the simulated PAN - request-only, tokenized immediately and
  * never stored, logged, or echoed back.
  */
final case class CreatePaymentMethodRequest(
    customerId: String,
    cardNumber: String,
    expiry: String,
    billingAddress: Address
)

object CreatePaymentMethodRequest {
  implicit val codec: Codec[CreatePaymentMethodRequest] = deriveCodec
}

final case class PaymentMethodResponse(
    id: String,
    last4: String,
    brand: String,
    expiry: String
)

object PaymentMethodResponse {
  implicit val codec: Codec[PaymentMethodResponse] = deriveCodec

  def apply(method: PaymentMethod): PaymentMethodResponse =
    PaymentMethodResponse(
      method.id,
      method.last4,
      method.brand.asString,
      method.expiry
    )
}

/** gshop-facing `POST /payments/methods` (US-13.2, billing address US-12.1) -
  * contract pinned in `gluon/docs/system-design.md`'s "REST contracts".
  */
object PaymentMethodRoutes {

  private val createMethodEndpoint: PublicEndpoint[
    CreatePaymentMethodRequest,
    ErrorResponse,
    PaymentMethodResponse,
    Any
  ] =
    endpoint.post
      .in("payments" / "methods")
      .in(jsonBody[CreatePaymentMethodRequest])
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[PaymentMethodResponse])
      .errorOut(statusCode(StatusCode.BadRequest).and(jsonBody[ErrorResponse]))

  private def validate(
      req: CreatePaymentMethodRequest,
      today: YearMonth
  ): Either[String, (Card, Address)] =
    for {
      _ <- Either.cond(
        req.customerId.trim.nonEmpty,
        (),
        "customerId is required"
      )
      card <- Card.validate(req.cardNumber, req.expiry, today)
      address <- Address.validate(req.billingAddress)
    } yield (card, address)

  def createMethodServerEndpoint[F[_]: Async](
      store: PaymentMethodStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    createMethodEndpoint.serverLogic[F] { req =>
      val context = Map("customer_id" -> req.customerId)
      for {
        _ <- logger.info(
          context ++ Map("method" -> "POST", "path" -> "/payments/methods")
        )("Received request")
        now <- Async[F].realTimeInstant
        result <- validate(req, YearMonth.from(now.atZone(ZoneOffset.UTC))) match {
          case Left(error) =>
            logger
              .warn(context + ("error" -> error))("Invalid payment method")
              .as(Left(ErrorResponse(error)))
          case Right((card, address)) =>
            store
              .create(req.customerId, card, address)
              .onError { case error =>
                logger.error(context, error)(
                  "Persisting the payment method failed"
                )
              }
              .flatMap { method =>
                logger
                  .info(context + ("payment_method_id" -> method.id))(
                    "Request completed"
                  )
                  .as(Right(PaymentMethodResponse(method)))
              }
        }
      } yield result
    }

  def routes[F[_]: Async](
      store: PaymentMethodStore[F],
      logger: StructuredLogger[F]
  ): HttpRoutes[F] =
    Http4sServerInterpreter[F]().toRoutes(
      createMethodServerEndpoint(store, logger)
    )
}
