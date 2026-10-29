package paymentservice

sealed trait PaymentError

case object PaymentNotFound extends PaymentError

/** A second payment for an order that already has one - `order_id` is unique
  * (V8). Raised by `PaymentStore.create`; `POST /payments` answers `409`.
  */
final case class PaymentAlreadyExists(orderId: String)
    extends RuntimeException(s"Order $orderId already has a payment")
