package paymentservice

sealed trait PaymentError

case object PaymentNotFound extends PaymentError
