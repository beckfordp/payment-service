package paymentservice

import cats.{MonadThrow}
import cats.effect.{Async, Clock}
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.HttpRoutes
import org.typelevel.log4cats.StructuredLogger
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.Schema.annotations.{description, encodedExample}
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

import java.time.{YearMonth, ZoneOffset}

/** `cardNumber` is the simulated PAN - request-only, tokenized immediately and
  * never stored, logged, or echoed back.
  */
@description("A card and billing address to tokenize.")
final case class CreatePaymentMethodRequest(
    @description("Customer id (UUID).")
    @encodedExample("c1f2a3b4-5d6e-4f70-8a9b-0c1d2e3f4a5b")
    customerId: String,
    @description(
      "Simulated PAN. Validated (length, digits, Luhn) and immediately tokenized; never stored, logged or returned."
    )
    @encodedExample("4242 4242 4242 4242")
    cardNumber: String,
    @description("Card expiry, `MM/YY`; must not be in the past.")
    @encodedExample("12/99")
    expiry: String,
    @description("Billing address stored with the method.")
    billingAddress: Address
)

object CreatePaymentMethodRequest {
  implicit val codec: Codec[CreatePaymentMethodRequest] = deriveCodec
}

@description("A tokenized payment method; never the PAN.")
final case class PaymentMethodResponse(
    @description("Payment method id (UUID).")
    @encodedExample("5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a8b")
    id: String,
    @description("Last four PAN digits.") @encodedExample("4242")
    last4: String,
    @description("Card brand: `visa`, `mastercard`, `amex` or `other`.")
    @encodedExample("visa")
    brand: String,
    @description("Card expiry, `MM/YY`.") @encodedExample("12/99")
    expiry: String
)

object PaymentMethodResponse {
  implicit val codec: Codec[PaymentMethodResponse] = deriveCodec

  def apply(method: PaymentMethod): PaymentMethodResponse =
    PaymentMethodResponse(
      paymentservice.domain.PaymentMethodId.render(method.id),
      method.last4,
      method.brand.asString,
      method.expiry
    )
}

sealed trait PaymentMethodError

object PaymentMethodError {
  final case class Invalid(error: String) extends PaymentMethodError
}

/** gshop-facing `POST /payments/methods` (US-13.2, billing address US-12.1) -
  * contract pinned in `gluon/docs/system-design.md`'s "REST contracts".
  */
object PaymentMethodRoutes {

  // REST docs convention (purerest README "Documenting endpoints"): every
  // endpoint, parameter, DTO field and error is described; PaymentDocsSuite
  // asserts Docs.undocumented finds no gaps.
  private val tag = "payment-methods"

  private val exampleAddress =
    Address("1 High Street", Some("Flat 2"), "London", "SW1A 1AA", "GB")

  private val exampleMethod = PaymentMethodResponse(
    "5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a8b",
    "4242",
    "visa",
    "12/99"
  )

  private val invalidVariant
      : EndpointOutput.OneOfVariant[PaymentMethodError.Invalid] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(
          jsonBody[ErrorResponse]
            .description(
              "`customerId` is blank, the card fails validation (length, digits, Luhn, or an expiry that isn't `MM/YY` or is in the past), or the billing address is invalid."
            )
            .example(ErrorResponse("Invalid card number"))
        )
        .map[PaymentMethodError.Invalid](e =>
          PaymentMethodError.Invalid(e.error)
        )(e => ErrorResponse(e.error))
    )

  private val createMethodEndpoint: PublicEndpoint[
    CreatePaymentMethodRequest,
    PaymentMethodError,
    PaymentMethodResponse,
    Any
  ] =
    endpoint.post
      .in("payments" / "methods")
      .in(
        jsonBody[CreatePaymentMethodRequest].example(
          CreatePaymentMethodRequest(
            "c1f2a3b4-5d6e-4f70-8a9b-0c1d2e3f4a5b",
            "4242 4242 4242 4242",
            "12/99",
            exampleAddress
          )
        )
      )
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[PaymentMethodResponse].example(exampleMethod))
      .errorOut(oneOf[PaymentMethodError](invalidVariant))
      .summary("Create a payment method")
      .description(
        "Validates the card (length, digits, Luhn, expiry) and billing address, then stores only a simulated token, last four digits, brand and expiry. The PAN is never stored, logged or echoed back. Not idempotent — each call creates a new method."
      )
      .tag(tag)

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

  def createMethodServerEndpoint[F[_]: MonadThrow: Clock](
      store: PaymentMethodStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    createMethodEndpoint.serverLogic[F] { req =>
      val context = Map("customer_id" -> req.customerId)
      for {
        _ <- logger.info(
          context ++ Map("method" -> "POST", "path" -> "/payments/methods")
        )("Received request")
        now <- Clock[F].realTimeInstant
        result <- validate(
          req,
          YearMonth.from(now.atZone(ZoneOffset.UTC))
        ) match {
          case Left(error) =>
            logger
              .warn(context + ("error" -> error))("Invalid payment method")
              .as(Left(PaymentMethodError.Invalid(error)))
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
                  .info(
                    context + (
                      "payment_method_id" -> paymentservice.domain.PaymentMethodId
                        .render(method.id)
                    )
                  )(
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
