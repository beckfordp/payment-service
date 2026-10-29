package paymentservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
import paymentservice.domain.OrderId
import purekafka.consumer.DeadLetter
import sttp.tapir.Schema

/** Local mirror of fulfilment-service's `DispatchFailed` payload (US-18.2). */
final case class DispatchFailedEvent(orderId: String, reason: String)

object DispatchFailedEvent {
  implicit val codec: Codec[DispatchFailedEvent] = deriveCodec
  implicit val schema: Schema[DispatchFailedEvent] = Schema.derived
}

/** Local mirror of fulfilment-service's `DispatchConfirmed` item shape. */
final case class FulfilmentItem(
    sku: String,
    productName: String,
    quantity: Int,
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
  */
final case class DispatchConfirmedEvent(
    orderId: String,
    trackingNo: String,
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
  * A gateway call is idempotent by `authId`, so a retry after a failed store
  * write is safe. A store write that matches no authorized row after the
  * gateway call raises [[UnrecordedTransition]] rather than committing. A
  * duplicate `DispatchConfirmed` finds the payment already `captured` and
  * no-ops.
  */
object FulfilmentEventConsumer {

  val topic: String = "fulfilment-events"

  private val captureFailedReason = "capture_failed"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("payment-service-fulfilment-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  def run[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        handle(store, methods, gateway, logger, deadLetter)(
          committable.record.key.getOrElse(""),
          committable.record.value
        ) *> committable.offset.commit
      }

  /** One `fulfilment-events` record, minus the offset commit - split from
    * [[run]] so the logic and its DLQ routing are testable without a broker.
    */
  def handle[F[_]: Async](
      store: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  )(key: String, value: Option[String]): F[Unit] = {
    def unrecorded(orderId: OrderId, target: PaymentStatus): F[Unit] =
      Async[F].raiseError(UnrecordedTransition(orderId, target))

    /** Schema drift must not silently stop captures: anything this consumer
      * can't act on goes to the DLQ raw instead of being committed with only a
      * log line.
      */
    def deadLetterRaw(raw: String, error: Throwable): F[Unit] =
      deadLetter.guard(topic, key, raw)(Async[F].raiseError(error))

    def handleDispatchFailed(
        orderId: OrderId,
        event: DispatchFailedEvent
    ): F[Unit] =
      store.findByOrderId(orderId).flatMap {
        case None =>
          logger.warn(Map("order_id" -> event.orderId))(
            "DispatchFailed for an unknown payment - skipping"
          )
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
              Async[F].raiseError(
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
                    unrecorded(orderId, PaymentStatus.Voided)
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
              Async[F].raiseError(
                new RuntimeException(
                  s"Authorized payment ${payment.id} has no authId"
                )
              )
            case (_, None) =>
              Async[F].raiseError(
                new RuntimeException(
                  s"Authorized payment ${payment.id} has no paymentMethodId"
                )
              )
            case (Some(authId), Some(paymentMethodId)) =>
              methods.get(paymentMethodId).flatMap {
                case None =>
                  Async[F].raiseError(
                    new RuntimeException(
                      s"Authorized payment ${payment.id} references unknown payment method $paymentMethodId"
                    )
                  )
                case Some(method) =>
                  Async[F].realTimeInstant.flatMap { now =>
                    Async[F].raiseUnless(
                      payment.expiresAt.exists(_.isAfter(now))
                    )(AuthorizationExpired(orderId))
                  } *> gateway.capture(authId, method.cardToken).flatMap {
                    case Right(_) =>
                      store.recordCapture(orderId).flatMap {
                        case Some(_) =>
                          logger.info(
                            Map("order_id" -> event.orderId)
                          )("Payment captured")
                        case None =>
                          unrecorded(orderId, PaymentStatus.Captured)
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
                            case Some(_) => Async[F].unit
                            case None    =>
                              unrecorded(orderId, PaymentStatus.Failed)
                          }
                  }
              }
          }
      }

    value match {
      case None =>
        logger.error(Map.empty)(
          "Received fulfilment-events record with a null value - skipping"
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
                        deadLetter.guard(topic, key, raw)(
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
                        deadLetter.guard(topic, key, raw)(
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

/** The gateway call succeeded but the conditional store write matched no
  * authorized row - raised so the record is retried / dead-lettered instead of
  * committed with the payment out of step with the gateway.
  */
final case class AuthorizationExpired(orderId: OrderId)
    extends RuntimeException(
      s"Authorization for order ${OrderId.render(orderId)} expired before it was captured"
    )

final case class UnrecordedTransition(orderId: OrderId, target: PaymentStatus)
    extends RuntimeException(
      s"Payment for order ${OrderId.render(orderId)} was not authorized when recording ${target.asString}"
    )
