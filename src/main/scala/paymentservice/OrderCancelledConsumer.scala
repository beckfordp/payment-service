package paymentservice

import cats.MonadThrow
import cats.effect.{Async, Clock}
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import org.typelevel.log4cats.StructuredLogger
import paymentservice.domain.{OrderId, PaymentStatus}
import purekafka.consumer.DeadLetter
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

import java.time.Instant

/** Local mirror of order-service's `OrderCancelled` payload (US-9.2). `reason`
  * is optional in the wire schema so old reason-less messages still decode; it
  * is not needed for the void itself.
  */
@description(
  "Order cancelled: a customer cancel or a consumed PaymentAuthorizationExpired moved the order to cancelled."
)
final case class OrderCancelledEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Customer who placed the order.")
    @encodedExample("7c9e6679-7425-40de-944b-e07fc1f90ae7")
    customerId: String,
    @description("When the order was cancelled (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant,
    @description(
      "Cancellation trigger: customer | auth_expired | stale | operator."
    )
    @encodedExample("customer")
    reason: Option[String] = None
)

object OrderCancelledEvent {
  implicit val codec: Codec[OrderCancelledEvent] = deriveCodec
  implicit val schema: Schema[OrderCancelledEvent] = Schema.derived
}

/** US-9.3: consumes order-service's shared `OrderCancelled` event and voids an
  * authorized hold. A duplicate (already `Voided`) or an order whose payment
  * already moved to `Captured`/`Failed` is a logged no-op; a missing `authId`
  * or a store transition that matches nothing after the gateway call raises so
  * the record is retried/DLQ'd. The subsequent capture path already no-ops
  * because its conditional update only matches `Authorized`.
  */
object OrderCancelledConsumer {

  val topic: String = "order-events"

  private def consumerSettings[F[_]: Async](
      config: KafkaConfig
  ): ConsumerSettings[F, Option[String], Option[String]] =
    ConsumerSettings[F, Option[String], Option[String]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId("payment-service-order-events")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private def handleCancelled[F[_]: MonadThrow](
      store: PaymentStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F]
  )(orderId: OrderId, event: OrderCancelledEvent): F[Unit] =
    store.findByOrderId(orderId).flatMap {
      case None =>
        logger.warn(Map("order_id" -> event.orderId))(
          "OrderCancelled for an unknown payment - skipping"
        )
      case Some(payment) if payment.status == PaymentStatus.Voided =>
        logger.info(
          Map(
            "order_id" -> event.orderId,
            "reason" -> event.reason.getOrElse("-")
          )
        )("OrderCancelled for an already-voided payment - skipping duplicate")
      case Some(payment) if payment.status != PaymentStatus.Authorized =>
        logger.warn(
          Map(
            "order_id" -> event.orderId,
            "status" -> payment.status.asString,
            "reason" -> event.reason.getOrElse("-")
          )
        )("OrderCancelled for a non-authorized payment - skipping")
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
                    Map(
                      "order_id" -> event.orderId,
                      "reason" -> event.reason.getOrElse("-")
                    )
                  )("Authorization voided on order cancellation")
                case None =>
                  MonadThrow[F].raiseError(
                    UnrecordedTransition(orderId, PaymentStatus.Voided)
                  )
              }
        }
    }

  /** One `order-events` record, minus the offset commit - exposed for
    * broker-free handler tests.
    */
  def handle[F[_]: MonadThrow: Clock](
      store: PaymentStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, Option[String]]
  )(key: String, value: Option[String]): F[Unit] = {
    def deadLetterRaw(raw: String, error: Throwable): F[Unit] =
      deadLetter.guard(topic, key, Some(raw))(MonadThrow[F].raiseError(error))

    value match {
      case None =>
        logger.error(Map.empty)(
          "Received order-events record with a null value - dead-lettering it"
        ) *> deadLetter.guard(topic, key, None)(
          MonadThrow[F].raiseError(
            new IllegalArgumentException("order-events record has a null value")
          )
        )
      case Some(raw) =>
        decode[EventEnvelope](raw) match {
          case Left(error) =>
            logger.error(
              Map("topic" -> topic, "bytes" -> raw.length.toString),
              error
            )("Failed to decode order-events envelope") *>
              deadLetterRaw(raw, error)
          case Right(envelope) =>
            envelope.eventType match {
              case "OrderCancelled" =>
                envelope.payload.as[OrderCancelledEvent] match {
                  case Left(error) =>
                    logger.error(
                      Map("topic" -> topic, "bytes" -> raw.length.toString),
                      error
                    )("Failed to decode OrderCancelled payload") *>
                      deadLetterRaw(raw, error)
                  case Right(event) =>
                    OrderId.parse(event.orderId) match {
                      case Left(error) =>
                        deadLetterRaw(raw, new IllegalArgumentException(error))
                      case Right(orderId) =>
                        deadLetter.guard(topic, key, Some(raw))(
                          handleCancelled(store, gateway, logger)(
                            orderId,
                            event
                          )
                        )
                    }
                }
              case other =>
                logger.info(
                  Map("topic" -> topic, "event_type" -> other)
                )("Ignoring unrelated order-events event type")
            }
        }
    }
  }

  def run[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      deadLetter: DeadLetter[F, String, Option[String]]
  ): Stream[F, Unit] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .subscribeTo(topic)
      .records
      .evalMap { committable =>
        handle(store, gateway, logger, deadLetter)(
          committable.record.key.getOrElse(""),
          committable.record.value
        ) *> committable.offset.commit
      }
}
