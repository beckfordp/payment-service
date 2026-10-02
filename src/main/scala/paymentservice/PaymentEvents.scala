package paymentservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

import java.time.Instant

/** Payload shape pinned in `gluon/docs/system-design.md`'s "Payload contracts"
  * section - that cross-repo doc, not this case class, is the source of truth
  * order-service's consumer (`order.status-changed` -> `confirmed`) should
  * read. `orderId` is what order-service actually needs to correlate back to
  * the order it charged; `paymentId` is this service's own record id, included
  * for cross-service log/trace correlation.
  */
final case class PaymentSettledEvent(
    orderId: String,
    paymentId: String,
    amountCents: Int,
    timestamp: Instant
)

object PaymentSettledEvent {
  implicit val codec: Codec[PaymentSettledEvent] = deriveCodec
}

final case class PaymentFailedEvent(
    orderId: String,
    paymentId: String,
    amountCents: Int,
    timestamp: Instant
)

object PaymentFailedEvent {
  implicit val codec: Codec[PaymentFailedEvent] = deriveCodec
}
