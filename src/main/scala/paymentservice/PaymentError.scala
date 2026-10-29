package paymentservice

import paymentservice.domain.OrderId

sealed trait PaymentError

case object PaymentNotFound extends PaymentError

/** A second payment for an order that already has one - `order_id` is unique
  * (V8). Raised by `PaymentStore.create`; `POST /payments` answers `409`.
  */
final case class PaymentAlreadyExists(orderId: OrderId)
    extends RuntimeException(
      s"Order ${OrderId.render(orderId)} already has a payment"
    )
