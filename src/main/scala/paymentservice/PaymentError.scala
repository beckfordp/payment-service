package paymentservice

sealed trait PaymentError

case object PaymentNotFound extends PaymentError

final case class InvalidStatus(raw: String) extends PaymentError
