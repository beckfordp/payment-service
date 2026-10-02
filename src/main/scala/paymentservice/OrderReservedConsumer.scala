package paymentservice

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger

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
}

/** Consumes `order.reserved` (US-6.1): creates a `Payment` for the order,
  * simulates a charge (always succeeds in this track - no real payment provider
  * decided yet, see the ADR backlog item), and publishes `payment.settled`. No
  * retry on the consume/processing side (a decode failure is logged and the
  * offset still committed) - same at-least-once, commit-after-process risk
  * profile as order-service's own `StockEventConsumer`. No duplicate-delivery
  * guard: a redelivered `order.reserved` creates a second `Payment` row for the
  * same `orderId` - that's US-6.2's job (Redis idempotency keys), not this
  * track's.
  */
object OrderReservedConsumer {

  val topic: String = "order.reserved"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("payment-service-order-reserved")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  def run[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      publisher: PaymentEventPublisher[F],
      logger: StructuredLogger[F]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        val handled: F[Unit] =
          decode[OrderReservedEvent](committable.record.value) match {
            case Left(error) =>
              logger.error(
                Map("raw" -> committable.record.value),
                error
              )("Failed to decode order.reserved")
            case Right(event) =>
              for {
                payment <- store.create(event.orderId, event.totalCents)
                settled <- store.update(payment.id, PaymentStatus.Settled)
                _ <- settled match {
                  case Some(settledPayment) =>
                    logger.info(
                      Map(
                        "order_id" -> event.orderId,
                        "payment_id" -> settledPayment.id
                      )
                    )(
                      "Payment created and settled from order.reserved event"
                    ) *> publisher.publishSettled(
                      PaymentSettledEvent(
                        orderId = event.orderId,
                        paymentId = settledPayment.id,
                        amountCents = settledPayment.amountCents,
                        timestamp = settledPayment.updatedAt
                      )
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
          }
        handled *> committable.offset.commit
      }
}
