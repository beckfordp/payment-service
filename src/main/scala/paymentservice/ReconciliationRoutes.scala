package paymentservice

import cats.Monad
import cats.effect.Clock
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.typelevel.log4cats.StructuredLogger
import paymentservice.domain.{
  OrderId,
  Reconciliation,
  ReconciliationStatus,
  ResolveReconciliationOutcome
}
import sttp.model.StatusCode
import sttp.tapir._
import sttp.tapir.Schema.annotations.{description, encodedExample}
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ServerEndpoint

import java.time.Instant

/** ADM-4 reconciliation API errors (ADR 0026). */
sealed trait ReconciliationError

case object ReconciliationFlagNotFound extends ReconciliationError
case object ReconciliationUnauthorized extends ReconciliationError
final case class InvalidReconciliationStatus(raw: String)
    extends ReconciliationError

@description("One manual-reconciliation flag and its resolution state.")
final case class ReconciliationResponse(
    @description("Order the flag belongs to.")
    @encodedExample("3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90")
    orderId: String,
    @description("Why manual reconciliation is needed.")
    @encodedExample("capture_after_void")
    reason: String,
    @description("open or resolved.")
    @encodedExample("open")
    status: String,
    @description("When the flag was recorded.")
    createdAt: Instant,
    @description("Operator who resolved it, when resolved.")
    operatorId: Option[String],
    @description("Operator's note, when supplied.")
    note: Option[String],
    @description("When it was resolved.")
    resolvedAt: Option[Instant]
)

object ReconciliationResponse {
  implicit val codec: Codec[ReconciliationResponse] = deriveCodec

  def apply(row: Reconciliation): ReconciliationResponse =
    ReconciliationResponse(
      OrderId.render(row.orderId),
      row.reason,
      row.status.asString,
      row.createdAt,
      row.operatorId,
      row.note,
      row.resolvedAt
    )
}

@description("Resolve a reconciliation flag.")
final case class ResolveReconciliationRequest(
    @description("Only `resolved` is accepted (ADM-4).")
    @encodedExample("resolved")
    status: String,
    @description("Optional operator note explaining the resolution.")
    @encodedExample("Refunded manually")
    note: Option[String] = None
)

object ResolveReconciliationRequest {
  implicit val codec: Codec[ResolveReconciliationRequest] = deriveCodec
}

object ReconciliationRoutes {

  private val tag = "reconciliations"

  private val exampleReconciliation = ReconciliationResponse(
    orderId = "3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90",
    reason = "capture_after_void",
    status = "open",
    createdAt = Instant.parse("2026-11-02T10:15:30Z"),
    operatorId = None,
    note = None,
    resolvedAt = None
  )

  private val idPath: EndpointInput.PathCapture[String] =
    path[String]("orderId")
      .description("Order id whose flag to resolve.")
      .example("3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90")

  private def errorBody(
      what: String,
      example: ErrorResponse
  ): EndpointIO.Body[String, ErrorResponse] =
    jsonBody[ErrorResponse].description(what).example(example)

  private val notFoundVariant
      : EndpointOutput.OneOfVariant[ReconciliationFlagNotFound.type] =
    oneOfVariant(
      statusCode(StatusCode.NotFound)
        .and(
          errorBody(
            "No flag for this order.",
            ErrorResponse("Reconciliation flag not found")
          )
        )
        .map[ReconciliationFlagNotFound.type](_ => ReconciliationFlagNotFound)(
          _ => ErrorResponse("Reconciliation flag not found")
        )
    )

  private val unauthorizedVariant
      : EndpointOutput.OneOfVariant[ReconciliationUnauthorized.type] =
    oneOfVariant(
      statusCode(StatusCode.Unauthorized)
        .and(
          errorBody(
            "The dev operator token is absent, malformed or does not match.",
            ErrorResponse("Invalid operator token")
          )
        )
        .map(_ => ReconciliationUnauthorized)(_ =>
          ErrorResponse("Invalid operator token")
        )
    )

  private val invalidStatusVariant
      : EndpointOutput.OneOfVariant[InvalidReconciliationStatus] =
    oneOfVariant(
      statusCode(StatusCode.BadRequest)
        .and(
          errorBody(
            "Only `resolved` is accepted.",
            ErrorResponse("Invalid reconciliation status: 'open'")
          )
        )
        .map(_ => InvalidReconciliationStatus("open"))(_ =>
          ErrorResponse(
            "Invalid reconciliation status: only 'resolved' is accepted"
          )
        )
    )

  private val listEndpoint: PublicEndpoint[
    (Option[String], Option[String]),
    ReconciliationError,
    List[ReconciliationResponse],
    Any
  ] =
    endpoint.get
      .in("reconciliations")
      .in(
        query[Option[String]]("status")
          .description("open or resolved (default: both).")
          .example(Some("open"))
      )
      .in(
        query[Option[String]]("orderId")
          .description("Only this order's flag.")
          .example(Some("3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90"))
      )
      .out(
        jsonBody[List[ReconciliationResponse]].example(
          List(exampleReconciliation)
        )
      )
      .errorOut(oneOf[ReconciliationError](invalidStatusVariant))
      .summary("List reconciliation flags")
      .description(
        "The manual-reconciliation queue, newest first. Both filters are optional and ANDed; an unknown status → 400 and an unknown orderId returns an empty list. Unauthenticated local/demo read (ADM-4)."
      )
      .tag(tag)

  private val resolveEndpoint: PublicEndpoint[
    (String, Option[String], ResolveReconciliationRequest),
    ReconciliationError,
    ReconciliationResponse,
    Any
  ] =
    endpoint.patch
      .in("reconciliations" / idPath)
      .in(
        header[Option[String]]("Authorization")
          .description("Dev operator token: `Bearer <token>` (ADR 0024/0026).")
      )
      .in(
        jsonBody[ResolveReconciliationRequest].example(
          ResolveReconciliationRequest("resolved", Some("Refunded manually"))
        )
      )
      .out(jsonBody[ReconciliationResponse].example(exampleReconciliation))
      .errorOut(
        oneOf[ReconciliationError](
          invalidStatusVariant,
          notFoundVariant,
          unauthorizedVariant
        )
      )
      .summary("Resolve a reconciliation flag")
      .description(
        "Marks an open flag resolved, recording the operator id from the token, an optional note and the timestamp. The first resolver wins: a repeat is an idempotent 200 and never overwrites the original. 401 without a valid token, 404 when no flag exists."
      )
      .tag(tag)

  def listServerEndpoint[F[_]: Monad](
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    listEndpoint.serverLogic[F] { case (statusRaw, orderIdRaw) =>
      val status = statusRaw.map(_.trim).filter(_.nonEmpty)
      val orderId = orderIdRaw.map(_.trim).filter(_.nonEmpty)
      logger.info(
        Map(
          "method" -> "GET",
          "path" -> "/reconciliations",
          "status" -> status.getOrElse(""),
          "order_id" -> orderId.getOrElse("")
        )
      )("Received request") *>
        (status.traverse(ReconciliationStatus.fromString) match {
          case Left(_) =>
            Monad[F].pure(
              Left[ReconciliationError, List[ReconciliationResponse]](
                InvalidReconciliationStatus(status.getOrElse(""))
              )
            )
          case Right(parsedStatus) =>
            orderId match {
              case Some(raw) =>
                OrderId.parse(raw) match {
                  case Left(_) =>
                    Monad[F].pure(
                      Right(List.empty[ReconciliationResponse])
                    )
                  case Right(parsedOrderId) =>
                    store
                      .listReconciliations(parsedStatus, Some(parsedOrderId))
                      .map(rows =>
                        Right(rows.map(ReconciliationResponse.apply))
                      )
                }
              case None =>
                store
                  .listReconciliations(parsedStatus, None)
                  .map(rows => Right(rows.map(ReconciliationResponse.apply)))
            }
        })
    }

  def resolveServerEndpoint[F[_]: Monad: Clock](
      store: PaymentStore[F],
      operatorAuth: OperatorAuth,
      logger: StructuredLogger[F]
  ): ServerEndpoint[Any, F] =
    resolveEndpoint.serverLogic[F] {
      case (rawOrderId, authorization, request) =>
        val requested = request.status.trim
        val context = Map(
          "method" -> "PATCH",
          "path" -> s"/reconciliations/$rawOrderId",
          "order_id" -> rawOrderId
        )
        operatorAuth.requireOperator(authorization) match {
          case Left(_) =>
            logger
              .warn(context)("Operator token rejected")
              .as(Left(ReconciliationUnauthorized))
          case Right(operatorId)
              if requested != ReconciliationStatus.Resolved.asString =>
            logger
              .warn(context + ("status" -> requested))(
                "Invalid reconciliation status"
              )
              .as(Left(InvalidReconciliationStatus(requested)))
          case Right(operatorId) =>
            OrderId.parse(rawOrderId) match {
              case Left(_) =>
                logger
                  .warn(context)("Reconciliation flag not found")
                  .as(Left(ReconciliationFlagNotFound))
              case Right(orderId) =>
                for {
                  _ <- logger.info(context)("Received request")
                  now <- Clock[F].realTimeInstant
                  result <- store.resolveReconciliation(
                    orderId,
                    operatorId,
                    request.note.map(_.trim).filter(_.nonEmpty),
                    now
                  )
                  response <- result match {
                    case ResolveReconciliationOutcome.Resolved(row) =>
                      logger
                        .info(context)("Reconciliation resolved")
                        .as(
                          Right(ReconciliationResponse(row))
                        )
                    case ResolveReconciliationOutcome.AlreadyResolved(row) =>
                      logger
                        .info(context)("Reconciliation already resolved")
                        .as(
                          Right(ReconciliationResponse(row))
                        )
                    case ResolveReconciliationOutcome.NotFound =>
                      logger
                        .warn(context)("Reconciliation flag not found")
                        .as(Left(ReconciliationFlagNotFound))
                  }
                } yield response
            }
        }
    }
}
