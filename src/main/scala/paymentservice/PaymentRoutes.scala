package paymentservice

import cats.{Monad, MonadThrow}
import cats.effect.Async
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import paymentservice.domain.{PaymentRules, OrderId, PaymentId}
import org.http4s.HttpRoutes
import org.typelevel.log4cats.StructuredLogger
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.Schema.annotations.{description, encodedExample}
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

import java.util.UUID

@description("A new payment to record.")
final case class CreatePaymentRequest(
    @description(
      "Order id this payment is for; non-blank, unique across payments."
    )
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Amount to charge, in cents; must be positive.")
    @encodedExample(4999)
    amountCents: Int
)

object CreatePaymentRequest {
  implicit val codec: Codec[CreatePaymentRequest] = deriveCodec
}

@description("A payment as stored.")
final case class PaymentResponse(
    @description("Payment id (UUID).")
    @encodedExample("9b2f1c3d-4e5a-4b6c-8d7e-9f0a1b2c3d4e")
    id: String,
    @description("Order id the payment is for.")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Amount to charge, in cents.")
    @encodedExample(4999)
    amountCents: Int,
    @description(
      "Lifecycle status: `pending`, `authorized`, `captured`, `failed` or `voided`."
    )
    @encodedExample("pending")
    status: String,
    @description("Creation time (UTC).")
    @encodedExample("2026-11-01T12:00:00Z")
    createdAt: java.time.Instant,
    @description("Last status change (UTC).")
    @encodedExample("2026-11-01T12:00:00Z")
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

@description("A failed request.")
final case class ErrorResponse(
    @description("Human-readable reason.") @encodedExample("Payment not found")
    error: String
)

sealed trait CreatePaymentError

object CreatePaymentError {
  final case class Invalid(error: String) extends CreatePaymentError
  final case class Conflict(error: String) extends CreatePaymentError
}

object ErrorResponse {
  implicit val codec: Codec[ErrorResponse] = deriveCodec
}

object PaymentRoutes {

  // REST docs convention (purerest README "Documenting endpoints"): every
  // endpoint, parameter, DTO field and error is described; PaymentDocsSuite
  // asserts Docs.undocumented finds no gaps.
  private val tag = "payments"

  private val examplePayment = PaymentResponse(
    "9b2f1c3d-4e5a-4b6c-8d7e-9f0a1b2c3d4e",
    "3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f",
    4999,
    "pending",
    java.time.Instant.parse("2026-11-01T12:00:00Z"),
    java.time.Instant.parse("2026-11-01T12:00:00Z")
  )

  private def errorBody(
      what: String,
      example: ErrorResponse
  ): EndpointIO.Body[String, ErrorResponse] =
    jsonBody[ErrorResponse].description(what).example(example)

  private val idPath: EndpointInput.PathCapture[PaymentId] =
    path[PaymentId]("id")
      .description("Payment id (UUID).")
      .example(
        PaymentId.fromUuid(
          UUID.fromString("9b2f1c3d-4e5a-4b6c-8d7e-9f0a1b2c3d4e")
        )
      )

  private val createPaymentEndpoint: PublicEndpoint[
    CreatePaymentRequest,
    CreatePaymentError,
    PaymentResponse,
    Any
  ] =
    endpoint.post
      .in("payments")
      .in(
        jsonBody[CreatePaymentRequest].example(
          CreatePaymentRequest(
            "3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f",
            4999
          )
        )
      )
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[PaymentResponse].example(examplePayment))
      .errorOut(
        oneOf[CreatePaymentError](
          oneOfVariant(
            statusCode(StatusCode.BadRequest)
              .and(
                errorBody(
                  "`orderId` is blank or `amountCents` isn't positive.",
                  ErrorResponse("orderId is required")
                )
              )
              .map[CreatePaymentError.Invalid](e =>
                CreatePaymentError.Invalid(e.error)
              )(e => ErrorResponse(e.error))
          ),
          oneOfVariant(
            statusCode(StatusCode.Conflict)
              .and(
                errorBody(
                  "The order already has a payment.",
                  ErrorResponse(
                    "Order 3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f already has a payment"
                  )
                )
              )
              .map[CreatePaymentError.Conflict](e =>
                CreatePaymentError.Conflict(e.error)
              )(e => ErrorResponse(e.error))
          )
        )
      )
      .summary("Create a payment")
      .description(
        "Records a new payment in `pending` status for an order. `orderId` is unique: a second payment for the same order is `409`. Does not charge, authorize or publish an event — that is `POST /payments/authorize`."
      )
      .tag(tag)

  private def validate(req: CreatePaymentRequest): Either[String, OrderId] =
    PaymentRules.validate(req.orderId, req.amountCents)

  private val notFoundVariant
      : EndpointOutput.OneOfVariant[PaymentNotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(
          errorBody(
            "No payment with this id.",
            ErrorResponse("Payment not found")
          )
        )
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
      .in("payments" / idPath)
      .out(jsonBody[PaymentResponse].example(examplePayment))
      .errorOut(paymentErrorOutput)
      .summary("Get a payment")
      .description("One payment by its UUID. Read-only.")
      .tag(tag)

  private val deletePaymentEndpoint
      : PublicEndpoint[PaymentId, PaymentError, Unit, Any] =
    endpoint.delete
      .in("payments" / idPath)
      .out(statusCode(StatusCode.NoContent))
      .errorOut(paymentErrorOutput)
      .summary("Delete a payment")
      .description(
        "Hard-deletes the payment record. No event published. `404` when the id is unknown."
      )
      .tag(tag)

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
                .as(Left(CreatePaymentError.Conflict(conflict.message)))
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
