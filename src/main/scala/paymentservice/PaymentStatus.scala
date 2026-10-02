package paymentservice

sealed trait PaymentStatus {
  def asString: String = this match {
    case PaymentStatus.Pending => "pending"
    case PaymentStatus.Settled => "settled"
    case PaymentStatus.Failed  => "failed"
  }
}

object PaymentStatus {
  case object Pending extends PaymentStatus
  case object Settled extends PaymentStatus
  case object Failed extends PaymentStatus

  def fromString(raw: String): Either[String, PaymentStatus] = raw match {
    case "pending" => Right(Pending)
    case "settled" => Right(Settled)
    case "failed"  => Right(Failed)
    case other     => Left(s"Invalid payment status: '$other'")
  }
}
