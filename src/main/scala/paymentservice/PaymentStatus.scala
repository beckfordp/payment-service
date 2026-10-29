package paymentservice

/** ADR 0011's authorize/capture split: `Authorized` holds funds (US-14.1),
  * `Captured` means money actually moved (US-19.1, Journey 4). `Voided`
  * releases the hold (US-18.2).
  */
sealed trait PaymentStatus {
  def asString: String = this match {
    case PaymentStatus.Pending    => "pending"
    case PaymentStatus.Authorized => "authorized"
    case PaymentStatus.Captured   => "captured"
    case PaymentStatus.Failed     => "failed"
    case PaymentStatus.Voided     => "voided"
  }
}

object PaymentStatus {
  case object Pending extends PaymentStatus
  case object Authorized extends PaymentStatus
  case object Captured extends PaymentStatus
  case object Failed extends PaymentStatus
  case object Voided extends PaymentStatus

  def fromString(raw: String): Either[String, PaymentStatus] = raw match {
    case "pending"    => Right(Pending)
    case "authorized" => Right(Authorized)
    case "captured"   => Right(Captured)
    case "failed"     => Right(Failed)
    case "voided"     => Right(Voided)
    case other        => Left(s"Invalid payment status: '$other'")
  }

  /** The payment lifecycle as a pure function: `pending -> authorized|failed`,
    * `authorized -> captured|voided|failed`; `captured`, `voided` and `failed`
    * are terminal. Every status write goes through this before it reaches the
    * store's conditional update.
    */
  def transition(
      from: PaymentStatus,
      to: PaymentStatus
  ): Either[IllegalTransition, PaymentStatus] =
    (from, to) match {
      case (Pending, Authorized | Failed)                         => Right(to)
      case (Authorized, Captured | Voided | Failed)               => Right(to)
      case (Pending | Authorized | Captured | Failed | Voided, _) =>
        Left(IllegalTransition(from, to))
    }
}

final case class IllegalTransition(from: PaymentStatus, to: PaymentStatus)
    extends RuntimeException(
      s"Illegal payment transition ${from.asString} -> ${to.asString}"
    )
