package paymentservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

import java.time.Instant

/** Payload shapes pinned in `gluon/docs/system-design.md`'s "Payload contracts"
  * section - that cross-repo doc, not these case classes, is the source of
  * truth. All four are published on `payment-events` via this service's own
  * `outbox_events` table + Debezium CDC. `PaymentAuthorized` (Journey 1) and
  * `PaymentCaptured`/`PaymentFailed` (Journey 4) are written today;
  * `PaymentAuthorizationExpired` lands with Journey 5's expiry sweep.
  */

/** Published as an async side effect of the synchronous `POST
  * /payments/authorize` succeeding (US-14.1, Journey 1). A decline stays
  * synchronous and publishes nothing. Consumed by fulfilment-service to start
  * pick/pack - deliberately not order-service, which learns the outcome from
  * the same HTTP response.
  */
@description(
  "Funds held: POST /payments/authorize succeeded. Consumed by fulfilment-service to start pick/pack."
)
final case class PaymentAuthorizedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Payment provider's authorization reference.")
    @encodedExample("auth-7f3a9c")
    authId: String,
    @description(
      "When the hold lapses if never captured (ISO-8601 instant)."
    )
    @encodedExample("2026-11-07T10:15:30Z")
    expiresAt: Instant,
    @description("When the authorization was made (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object PaymentAuthorizedEvent {
  implicit val codec: Codec[PaymentAuthorizedEvent] = deriveCodec
  implicit val schema: Schema[PaymentAuthorizedEvent] = Schema.derived
}

/** Published once per consumed `fulfilment-events: DispatchConfirmed` (US-19.1,
  * Journey 4) - the point money actually moves.
  */
@description(
  "Money moved: capture succeeded after DispatchConfirmed. Published once per order."
)
final case class PaymentCapturedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("Amount captured, in cents.")
    @encodedExample(4999)
    amountCents: Int,
    @description("When the capture happened (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object PaymentCapturedEvent {
  implicit val codec: Codec[PaymentCapturedEvent] = deriveCodec
  implicit val schema: Schema[PaymentCapturedEvent] = Schema.derived
}

/** Published by the background expiry sweep over `{status: Authorized,
  * expiresAt < now}` (US-20.1, Journey 5).
  */
@description(
  "Authorization lapsed uncaptured: found by the background expiry sweep."
)
final case class PaymentAuthorizationExpiredEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description("When the sweep expired the hold (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
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
@description(
  "Capture declined after DispatchConfirmed. Authorize declines are synchronous and publish nothing."
)
final case class PaymentFailedEvent(
    @description("Order UUID (order-service's order id).")
    @encodedExample("3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")
    orderId: String,
    @description(
      "payment-service's own Payment id, for log/trace correlation only."
    )
    @encodedExample("9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d")
    paymentId: String,
    @description("Amount that failed to charge, in cents.")
    @encodedExample(4999)
    amountCents: Int,
    @description("Failure reason.")
    @encodedExample("capture_failed")
    reason: String,
    @description("When the capture was declined (ISO-8601 instant).")
    @encodedExample("2026-10-31T10:15:30Z")
    timestamp: Instant
)

object PaymentFailedEvent {
  implicit val codec: Codec[PaymentFailedEvent] = deriveCodec
  implicit val schema: Schema[PaymentFailedEvent] = Schema.derived
}
