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

import java.time.Instant

/** Local mirror of order-service's pinned `order.reserved` payload, per
  * `gluon/docs/system-design.md`'s "Payload contracts" section - that
  * cross-repo doc, not order-service's own case class, is the source of truth.
  * No shared library between the two services; each side keeps its own copy.
  */
final case class OrderReservedEvent(
    orderId: String,
    customerId: String,
    totalCents: Int,
    timestamp: Instant
)

object OrderReservedEvent {
  implicit val codec: Codec[OrderReservedEvent] = deriveCodec
  implicit val schema: Schema[OrderReservedEvent] = Schema.derived
}

/** Consumes `OrderReserved` envelopes on `order-events` (US-6.1; migrated to
  * the domain topic + envelope shape per ADR 0009): creates a `Payment` for the
  * order, simulates a charge (always succeeds in this track - no real payment
  * provider decided yet, see the ADR backlog item), and publishes
  * `PaymentSettled` on `payment-events`. `order-events` also carries other
  * event types (e.g. `OrderStatusChanged`) this consumer doesn't care about
  *   - those are silently skipped by `eventType`, not logged as malformed. A
  *     decode failure (null value, envelope, or payload) is logged and the
  *     offset still committed - no retry, an already-accepted gap. Only the
  *     *processing* step (`handleEvent`) is wrapped in
  *     `purekafka.consumer.DeadLetter.guard` (TD-2.4, ADR 0010): bounded retry,
  *     then `order-events-dlq` on exhaustion - mirroring order-service's own
  *     consumers' convention exactly. No duplicate-delivery guard: a
  *     redelivered `OrderReserved` creates a second `Payment` row for the same
  *     `orderId` - that's US-6.2's job (Redis idempotency keys), not this
  *     track's.
  */
object OrderReservedConsumer {

  val topic: String = "order-events"
  private val eventType: String = "OrderReserved"

  // Key/value typed as Option[String] (via fs2-kafka's null-safe
  // Deserializer.option) rather than plain String: a plain String
  // deserializer throws on a null key or value - e.g. a producer that sends
  // no key (confirmed live against a real broker with a bare
  // kafka-console-producer.sh call) - which kills this whole background
  // consumer stream silently, since Main.scala runs it via `.background.use`
  // and never observes the fiber's outcome.
  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("payment-service-order-reserved")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  def run[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, String]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        def handleEvent(event: OrderReservedEvent): F[Unit] =
          for {
            payment <- store.create(event.orderId, event.totalCents)
            settled <- store.update(payment.id, PaymentStatus.Settled)
            _ <- settled match {
              case Some(settledPayment) =>
                // No separate Kafka publish here (ADR 0008 / TD-3.4):
                // `store.update` already wrote the matching `outbox_events`
                // row atomically alongside the Settled transition above.
                logger.info(
                  Map(
                    "order_id" -> event.orderId,
                    "payment_id" -> settledPayment.id
                  )
                )(
                  "Payment created and settled from order.reserved event"
                )
              case None =>
                logger.error(
                  Map(
                    "order_id" -> event.orderId,
                    "payment_id" -> payment.id
                  )
                )(
                  "Payment vanished between create and settle - this should never happen"
                )
            }
          } yield ()

        val key = committable.record.key.getOrElse("")

        val handled: F[Unit] = committable.record.value match {
          case None =>
            logger.error(Map.empty)(
              "Received order-events record with a null value - skipping"
            )
          case Some(raw) =>
            decode[EventEnvelope](raw) match {
              case Left(error) =>
                logger.error(
                  Map("raw" -> raw),
                  error
                )("Failed to decode order-events envelope")
              case Right(envelope) if envelope.eventType != eventType =>
                Async[F].unit
              case Right(envelope) =>
                envelope.payload.as[OrderReservedEvent] match {
                  case Left(error) =>
                    logger.error(
                      Map("raw" -> raw),
                      error
                    )("Failed to decode OrderReserved payload")
                  case Right(event) =>
                    // Decode failures above are a different, already-accepted
                    // gap (logged and dropped, never retried) - only a
                    // message that decodes fine but fails *processing* (e.g.
                    // a transient store error) gets the bounded-retry-then-DLQ
                    // treatment, via purekafka.consumer.DeadLetter (TD-2.4,
                    // ADR 0010).
                    deadLetter.guard(topic, key, raw)(handleEvent(event))
                }
            }
        }
        handled *> committable.offset.commit
      }
}
