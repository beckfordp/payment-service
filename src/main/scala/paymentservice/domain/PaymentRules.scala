package paymentservice.domain

import cats.syntax.all._

/** The request rule `POST /payments` and `POST /payments/authorize` share,
  * defined once (TD-16.1): a non-blank `orderId` (checked first) and a positive
  * `amountCents`. Messages are the `400` bodies both endpoints return.
  */
object PaymentRules {

  def validate(orderId: String, amountCents: Int): Either[String, OrderId] =
    OrderId.parse(orderId) <* Either.cond(
      amountCents > 0,
      (),
      "amountCents must be positive"
    )
}
