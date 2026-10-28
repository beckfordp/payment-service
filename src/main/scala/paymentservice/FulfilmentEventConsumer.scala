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

/** Consumes `DispatchFailed` on `fulfilment-events` (US-18.2): voids the
  * gateway hold for an authorized payment, then records it `voided`. Missing or
  * non-authorized payments are logged and skipped; a gateway failure is retried
  * through the DLQ and leaves the payment authorized.
  */
object FulfilmentEventConsumer {

  val topic: String = "fulfilment-events"

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
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        def handleEvent(event: DispatchFailedEvent): F[Unit] =
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
                          Map("payment_id" -> payment.id)
                        )("Voided payment disappeared before the status update")
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
              case Right(envelope) if envelope.eventType != "DispatchFailed" =>
                Async[F].unit
              case Right(envelope) =>
                envelope.payload.as[DispatchFailedEvent] match {
                  case Left(error) =>
                    logger.error(Map("raw" -> raw), error)(
                      "Failed to decode DispatchFailed payload"
                    )
                  case Right(event) =>
                    deadLetter.guard(topic, key, raw)(handleEvent(event))
                }
            }
        }
        handled *> committable.offset.commit
      }
}
