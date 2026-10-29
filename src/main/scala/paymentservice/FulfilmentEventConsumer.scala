package paymentservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
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
  * Missing or non-authorized payments are logged and skipped; a gateway failure
  * is retried through the DLQ and leaves the payment authorized. A duplicate
  * `DispatchConfirmed` finds the payment already `captured` and no-ops.
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
        def handleDispatchFailed(event: DispatchFailedEvent): F[Unit] =
          store.findByOrderId(event.orderId).flatMap {
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
                    store.update(payment.id, PaymentStatus.Voided).flatMap {
                      case Some(voided) =>
                        logger.info(
                          Map("order_id" -> event.orderId)
                        )("Authorization voided")
                      case None =>
                        logger.warn(
                          Map(
                            "payment_id" -> paymentservice.domain.PaymentId
                              .render(payment.id)
                          )
                        )("Voided payment disappeared before the status update")
                    }
              }
          }

        def handleDispatchConfirmed(event: DispatchConfirmedEvent): F[Unit] =
          store.findByOrderId(event.orderId).flatMap {
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
                      gateway.capture(authId, method.cardToken).flatMap {
                        case Right(_) =>
                          store.recordCapture(event.orderId).flatMap {
                            case Some(_) =>
                              logger.info(
                                Map("order_id" -> event.orderId)
                              )("Payment captured")
                            case None =>
                              logger.warn(
                                Map("order_id" -> event.orderId)
                              )(
                                "Payment was no longer authorized when the capture was recorded"
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
                                event.orderId,
                                captureFailedReason
                              )
                              .flatMap {
                                case Some(_) => Async[F].unit
                                case None    =>
                                  logger.warn(
                                    Map("order_id" -> event.orderId)
                                  )(
                                    "Payment was no longer authorized when the capture failure was recorded"
                                  )
                              }
                      }
                  }
              }
          }

        val key = committable.record.key.getOrElse("")

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received fulfilment-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(Map("raw" -> raw), error)(
                  "Failed to decode fulfilment-events envelope"
                )
              case Right(envelope) =>
                envelope.eventType match {
                  case "DispatchFailed" =>
                    envelope.payload.as[DispatchFailedEvent] match {
                      case Left(error) =>
                        logger.error(Map("raw" -> raw), error)(
                          "Failed to decode DispatchFailed payload"
                        )
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handleDispatchFailed(event)
                        )
                    }
                  case "DispatchConfirmed" =>
                    envelope.payload.as[DispatchConfirmedEvent] match {
                      case Left(error) =>
                        logger.error(Map("raw" -> raw), error)(
                          "Failed to decode DispatchConfirmed payload"
                        )
                      case Right(event) =>
                        deadLetter.guard(topic, key, raw)(
                          handleDispatchConfirmed(event)
                        )
                    }
                  case _ => Async[F].unit
                }
            }
        }
        handled *> committable.offset.commit
      }
}
