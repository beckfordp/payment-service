package paymentservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

import java.time.Instant

/** Payload shapes pinned in `gluon/docs/system-design.md`'s "Payload contracts"
  * section - that cross-repo doc, not these case classes, is the source of
  * truth. All four are published on `payment-events` via this service's own
  * `outbox_events` table + Debezium CDC; the producers land with Checkout
  * Journeys 1/4/5, so nothing writes them yet.
  */

/** Published as an async side effect of the synchronous `POST
  * /payments/authorize` succeeding (US-14.1, Journey 1). A decline stays
  * synchronous and publishes nothing. Consumed by fulfilment-service to start
  * pick/pack - deliberately not order-service, which learns the outcome from
  * the same HTTP response.
  */
final case class PaymentAuthorizedEvent(
    orderId: String,
    authId: String,
    expiresAt: Instant,
    timestamp: Instant
)

object PaymentAuthorizedEvent {
  implicit val codec: Codec[PaymentAuthorizedEvent] = deriveCodec
  implicit val schema: Schema[PaymentAuthorizedEvent] = Schema.derived
}

/** Published once per consumed `fulfilment-events: DispatchConfirmed` (US-19.1,
  * Journey 4) - the point money actually moves.
  */
final case class PaymentCapturedEvent(
    orderId: String,
    amountCents: Int,
    timestamp: Instant
)

object PaymentCapturedEvent {
  implicit val codec: Codec[PaymentCapturedEvent] = deriveCodec
  implicit val schema: Schema[PaymentCapturedEvent] = Schema.derived
}

/** Published by the background expiry sweep over `{status: Authorized,
  * expiresAt < now}` (US-20.1, Journey 5).
  */
final case class PaymentAuthorizationExpiredEvent(
    orderId: String,
    timestamp: Instant
)

object PaymentAuthorizationExpiredEvent {
  implicit val codec: Codec[PaymentAuthorizationExpiredEvent] = deriveCodec
  implicit val schema: Schema[PaymentAuthorizationExpiredEvent] =
    Schema.derived
}

/** Published on capture decline only (US-19.1, Journey 4), with
  * `reason = "capture_failed"`. An authorize decline stays purely synchronous
  * and publishes nothing. `paymentId` is this service's own record id, included
  * for cross-service log/trace correlation.
  */
final case class PaymentFailedEvent(
    orderId: String,
    paymentId: String,
    amountCents: Int,
    reason: String,
    timestamp: Instant
)

object PaymentFailedEvent {
  implicit val codec: Codec[PaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[PaymentFailedEvent] = Schema.derived
}
