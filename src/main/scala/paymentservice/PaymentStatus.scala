package paymentservice

/** ADR 0011's authorize/capture split: `Authorized` holds funds (US-14.1),
  * `Captured` means money actually moved (US-19.1, Journey 4).
  */
sealed trait PaymentStatus {
  def asString: String = this match {
    case PaymentStatus.Pending    => "pending"
    case PaymentStatus.Authorized => "authorized"
    case PaymentStatus.Captured   => "captured"
    case PaymentStatus.Failed     => "failed"
  }
}

object PaymentStatus {
  case object Pending extends PaymentStatus
  case object Authorized extends PaymentStatus
  case object Captured extends PaymentStatus
  case object Failed extends PaymentStatus

  def fromString(raw: String): Either[String, PaymentStatus] = raw match {
    case "pending"    => Right(Pending)
    case "authorized" => Right(Authorized)
    case "captured"   => Right(Captured)
    case "failed"     => Right(Failed)
    case other        => Left(s"Invalid payment status: '$other'")
  }
}
