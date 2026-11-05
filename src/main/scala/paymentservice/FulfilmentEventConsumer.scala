package paymentservice

import paymentservice.domain.{
  LiveAuthorization,
  OrderId,
  PaymentRace,
  PaymentStatus
}
import cats.MonadThrow
import cats.effect.{Async, Clock}
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.trace.Tracer
import purekafka.consumer.{DeadLetter, EventAge}
import purekafka.tracing.KafkaTracing
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

/** Local mirror of fulfilment-service's `DispatchFailed` payload (US-18.2). */
@description(
  "Dispatch failed: pick/pack/dispatch hit an error. payment-service voids the hold; order-service marks the order failed."
)
final case class DispatchFailedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Why dispatch failed.")
    @encodedExample("carrier_unavailable")
    reason: String
)

object DispatchFailedEvent {
  implicit val codec: Codec[DispatchFailedEvent] = deriveCodec
  implicit val schema: Schema[DispatchFailedEvent] = Schema.derived
}

/** Local mirror of fulfilment-service's `DispatchConfirmed` item shape. */
final case class FulfilmentItem(
    @description("Product SKU.")
    @encodedExample("SKU-0042")
    sku: String,
    @description("Product name at dispatch time.")
    @encodedExample("Espresso cup")
    productName: String,
    @description("Units dispatched.")
    @encodedExample(2)
    quantity: Int,
    @description("Unit price, in cents.")
    @encodedExample(1250)
    unitPriceCents: Int
)

object FulfilmentItem {
  implicit val codec: Codec[FulfilmentItem] = deriveCodec

  // Inlined (no $defs name), matching order-service's mirror: the AsyncAPI
  // generator only collects top-level channel payloads' defs, so a named
  // nested schema would dangle as a missing `#/$defs/FulfilmentItem`.
  implicit val schema: Schema[FulfilmentItem] =
    Schema.derived[FulfilmentItem].copy(name = None)
}

/** Local mirror of fulfilment-service's `DispatchConfirmed` payload (US-19.1).
  *
  * Descriptions/examples are copied from the producer's artifact in gluon's
  * `registry/` (TD-19); nothing checks they stay current.
  */
@description(
  "Order dispatched: pick/pack/dispatch succeeded. Carries a full item snapshot so consumers need no order lookup."
)
final case class DispatchConfirmedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Carrier tracking number.")
    @encodedExample("TRK-123456789")
    trackingNo: String,
    @description("Items dispatched. Always sent.")
    items: List[FulfilmentItem]
)

object DispatchConfirmedEvent {
  implicit val codec: Codec[DispatchConfirmedEvent] = deriveCodec
  implicit val schema: Schema[DispatchConfirmedEvent] = Schema.derived
}

/** Consumes `fulfilment-events` (US-18.2, US-19.1):
  *   - `DispatchFailed` voids the gateway hold for an authorized payment, then
  *     records it `voided`. Nothing is published.
  *   - `DispatchConfirmed` captures the authorized payment, publishing
  *     `PaymentCaptured` or - on a capture decline - `PaymentFailed {reason:
  *     capture_failed}` via the outbox.
  *
  * Missing or non-authorized payments are logged and skipped; a gateway or
  * store failure is retried through the DLQ and leaves the payment authorized.
  * A null-value record has nothing to decode: it is dead-lettered with its null
  * value preserved. A gateway call is idempotent by `authId`, so a retry after
  * a failed store write is safe. A store write that matches no authorized row
  * after the gateway call raises [[UnrecordedTransition]] rather than
  * committing. A duplicate `DispatchConfirmed` finds the payment already
  * `captured` and no-ops.
  */
object FulfilmentEventConsumer {

  val topic: String = "fulfilment-events"

  val groupId: String = "payment-service-fulfilment-events"

  private val captureFailedReason = "capture_failed"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  def run[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, Option[String]],
      eventAge: EventAge[F],
      tracer: Tracer[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalTap(committable =>
        committable.record.timestamp.createTime.traverse_(
          eventAge.record(topic, groupId, _)
        )
      )
      .evalMap { committable =>
        KafkaTracing.traced(tracer)(
          committable.record.headers,
          s"$topic process",
          topic
        )(
          handle(store, methods, gateway, logger, deadLetter)(
            committable.record.key.getOrElse(""),
            committable.record.value
          )
        ) *> committable.offset.commit
      }

  /** One `fulfilment-events` record, minus the offset commit - split from
    * [[run]] so the logic and its DLQ routing are testable without a broker.
    */
  def handle[F[_]: MonadThrow: Clock](
      store: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, Option[String]]
  )(key: String, value: Option[String]): F[Unit] = {

    /** Schema drift must not silently stop captures: anything this consumer
      * can't act on goes to the DLQ raw instead of being committed with only a
      * log line.
      */
    def deadLetterRaw(raw: String, error: Throwable): F[Unit] =
      deadLetter.guard(topic, key, Some(raw))(MonadThrow[F].raiseError(error))

    def handleDispatchFailed(
        orderId: OrderId,
        event: DispatchFailedEvent
    ): F[Unit] =
      store.findByOrderId(orderId).flatMap {
        case None =>
          logger.warn(Map("order_id" -> event.orderId))(
            "DispatchFailed for an unknown payment - skipping"
          )
        case Some(payment) if payment.status == PaymentStatus.Voided =>
          logger.info(Map("order_id" -> event.orderId))(
            "DispatchFailed for an already-voided payment - skipping duplicate"
          )
        case Some(payment) if payment.status == PaymentStatus.Captured =>
          logger.warn(
            Map(
              "order_id" -> event.orderId,
              "status" -> payment.status.asString
            )
          )("DispatchFailed after capture - reconciliation flagged") *>
            store.flagReconciliation(orderId, "void_after_capture").void
        case Some(payment) if payment.status != PaymentStatus.Authorized =>
          logger.warn(
            Map(
              "order_id" -> event.orderId,
              "status" -> payment.status.asString
            )
          )("DispatchFailed for a non-authorized payment - skipping")
        case Some(payment) =>
          payment.authId match {
            case None =>
              MonadThrow[F].raiseError(
                new RuntimeException(
                  s"Authorized payment ${payment.id} has no authId"
                )
              )
            case Some(authId) =>
              gateway.void(authId) *>
                store.recordVoid(orderId).flatMap {
                  case Some(_) =>
                    logger.info(
                      Map("order_id" -> event.orderId)
                    )("Authorization voided")
                  case None =>
                    PaymentRace.afterNone(
                      store,
                      logger,
                      orderId,
                      PaymentStatus.Voided,
                      "DispatchFailed"
                    )
                }
          }
      }

    def handleDispatchConfirmed(
        orderId: OrderId,
        event: DispatchConfirmedEvent
    ): F[Unit] =
      store.findByOrderId(orderId).flatMap {
        case None =>
          logger.warn(Map("order_id" -> event.orderId))(
            "DispatchConfirmed for an unknown payment - skipping"
          )
        case Some(payment) if payment.status == PaymentStatus.Captured =>
          logger.info(Map("order_id" -> event.orderId))(
            "DispatchConfirmed for an already-captured payment - skipping duplicate"
          )
        case Some(payment) if payment.status == PaymentStatus.Voided =>
          logger.warn(
            Map(
              "order_id" -> event.orderId,
              "status" -> payment.status.asString
            )
          )("Capture after void - reconciliation flagged") *>
            store.flagReconciliation(orderId, "capture_after_void").void
        case Some(payment) if payment.status == PaymentStatus.Failed =>
          logger.warn(
            Map(
              "order_id" -> event.orderId,
              "status" -> payment.status.asString
            )
          )("Capture after a failed payment - reconciliation flagged") *>
            store.flagReconciliation(orderId, "capture_after_failed").void
        case Some(payment) if payment.status != PaymentStatus.Authorized =>
          logger.warn(
            Map(
              "order_id" -> event.orderId,
              "status" -> payment.status.asString
            )
          )("DispatchConfirmed for a non-authorized payment - skipping")
        case Some(payment) =>
          (payment.authId, payment.paymentMethodId) match {
            case (None, _) =>
              MonadThrow[F].raiseError(
                new RuntimeException(
                  s"Authorized payment ${payment.id} has no authId"
                )
              )
            case (_, None) =>
              MonadThrow[F].raiseError(
                new RuntimeException(
                  s"Authorized payment ${payment.id} has no paymentMethodId"
                )
              )
            case (Some(authId), Some(paymentMethodId)) =>
              methods.get(paymentMethodId).flatMap {
                case None =>
                  MonadThrow[F].raiseError(
                    new RuntimeException(
                      s"Authorized payment ${payment.id} references unknown payment method $paymentMethodId"
                    )
                  )
                case Some(method) =>
                  Clock[F].realTimeInstant.flatMap { now =>
                    if (
                      LiveAuthorization.permits(
                        PaymentStatus.Captured,
                        payment.expiresAt,
                        now
                      )
                    )
                      gateway.capture(authId, method.cardToken).flatMap {
                        case Right(_) =>
                          store.recordCapture(orderId).flatMap {
                            case Some(_) =>
                              logger.info(
                                Map("order_id" -> event.orderId)
                              )("Payment captured")
                            case None =>
                              PaymentRace.afterNone(
                                store,
                                logger,
                                orderId,
                                PaymentStatus.Captured,
                                "DispatchConfirmed"
                              )
                          }
                        case Left(decline) =>
                          logger.warn(
                            Map(
                              "order_id" -> event.orderId,
                              "reason" -> decline.value
                            )
                          )("Capture declined - recording PaymentFailed") *>
                            store
                              .recordCaptureFailure(
                                orderId,
                                captureFailedReason
                              )
                              .flatMap {
                                case Some(_) => MonadThrow[F].unit
                                case None    =>
                                  PaymentRace.afterNone(
                                    store,
                                    logger,
                                    orderId,
                                    PaymentStatus.Failed,
                                    "DispatchConfirmed"
                                  )
                              }
                      }
                    else
                      store
                        .flagReconciliation(
                          orderId,
                          "dispatch_after_authorization_expired"
                        ) *>
                        logger.warn(
                          Map("order_id" -> event.orderId)
                        )(
                          "Capture after authorization expiry - reconciliation flagged"
                        )
                  }
              }
          }
      }

    value match {
      case None =>
        logger.error(Map.empty)(
          "Received fulfilment-events record with a null value - dead-lettering it"
        ) *> deadLetter.guard(topic, key, None)(
          MonadThrow[F].raiseError(
            new IllegalArgumentException(
              "fulfilment-events record has a null value"
            )
          )
        )
      case Some(raw) =>
        decode[EventEnvelope](raw) match {
          case Left(error) =>
            logger.error(
              Map("topic" -> topic, "bytes" -> raw.length.toString),
              error
            )(
              "Failed to decode fulfilment-events envelope"
            ) *> deadLetterRaw(raw, error)
          case Right(envelope) =>
            envelope.eventType match {
              case "DispatchFailed" =>
                envelope.payload.as[DispatchFailedEvent] match {
                  case Left(error) =>
                    logger.error(
                      Map("topic" -> topic, "bytes" -> raw.length.toString),
                      error
                    )(
                      "Failed to decode DispatchFailed payload"
                    ) *> deadLetterRaw(raw, error)
                  case Right(event) =>
                    OrderId.parse(event.orderId) match {
                      case Left(error) =>
                        deadLetterRaw(raw, new IllegalArgumentException(error))
                      case Right(orderId) =>
                        deadLetter.guard(topic, key, Some(raw))(
                          handleDispatchFailed(orderId, event)
                        )
                    }
                }
              case "DispatchConfirmed" =>
                envelope.payload.as[DispatchConfirmedEvent] match {
                  case Left(error) =>
                    logger.error(
                      Map("topic" -> topic, "bytes" -> raw.length.toString),
                      error
                    )(
                      "Failed to decode DispatchConfirmed payload"
                    ) *> deadLetterRaw(raw, error)
                  case Right(event) =>
                    OrderId.parse(event.orderId) match {
                      case Left(error) =>
                        deadLetterRaw(raw, new IllegalArgumentException(error))
                      case Right(orderId) =>
                        deadLetter.guard(topic, key, Some(raw))(
                          handleDispatchConfirmed(orderId, event)
                        )
                    }
                }
              case other =>
                logger.warn(
                  Map("topic" -> topic, "event_type" -> other)
                )("Unknown fulfilment-events event type") *>
                  deadLetterRaw(
                    raw,
                    new IllegalArgumentException(
                      s"Unknown fulfilment-events event type '$other'"
                    )
                  )
            }
        }
    }
  }
}

final case class UnrecordedTransition(orderId: OrderId, target: PaymentStatus)
    extends RuntimeException(
      s"Payment for order ${OrderId.render(orderId)} was not authorized when recording ${target.asString}"
    )
