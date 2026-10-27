package paymentservice

import java.time.YearMonth

sealed trait CardBrand {
  def asString: String = this match {
    case CardBrand.Visa       => "visa"
    case CardBrand.Mastercard => "mastercard"
    case CardBrand.Amex       => "amex"
    case CardBrand.Other      => "other"
  }
}

object CardBrand {
  case object Visa extends CardBrand
  case object Mastercard extends CardBrand
  case object Amex extends CardBrand
  case object Other extends CardBrand

  /** Prefix rules only - enough for the simulator, not a full BIN table. */
  def fromNumber(digits: String): CardBrand = {
    def prefix(n: Int): Int = digits.take(n).toIntOption.getOrElse(-1)
    if (digits.startsWith("4")) Visa
    else if (
      (51 to 55).contains(prefix(2)) || (2221 to 2720).contains(prefix(4))
    )
      Mastercard
    else if (prefix(2) == 34 || prefix(2) == 37) Amex
    else Other
  }

  def fromString(raw: String): Either[String, CardBrand] = raw match {
    case "visa"       => Right(Visa)
    case "mastercard" => Right(Mastercard)
    case "amex"       => Right(Amex)
    case "other"      => Right(Other)
    case other        => Left(s"Invalid card brand: '$other'")
  }
}

/** What survives validation of a submitted card. The PAN itself is dropped here -
  * only `last4`/`brand`/`expiry` ever leave this object (US-13.1).
  */
final case class Card(last4: String, brand: CardBrand, expiry: String) {

  /** Simulated tokenizer: deterministic and human-readable so PureConfig
    * test-card outcomes are expressible (`gluon/docs/system-design.md`,
    * `POST /payments/methods`). Simulator-only and collision-prone - two cards
    * sharing brand and last four get the same token.
    */
  def token: String = s"tok_${brand.asString}_$last4"
}

object Card {

  private val expiryPattern = "^(0[1-9]|1[0-2])/(\\d{2})$".r

  def validate(
      cardNumber: String,
      expiry: String,
      today: YearMonth
  ): Either[String, Card] = {
    val digits = cardNumber.replace(" ", "")
    for {
      _ <- Either.cond(
        digits.length >= 13 && digits.length <= 19 &&
          digits.forall(c => c >= '0' && c <= '9') && luhn(digits),
        (),
        "Invalid card number"
      )
      expiresIn <- expiry match {
        case expiryPattern(mm, yy) =>
          Right(YearMonth.of(2000 + yy.toInt, mm.toInt))
        case _ => Left("Invalid expiry, expected MM/YY")
      }
      _ <- Either.cond(!expiresIn.isBefore(today), (), "Card has expired")
    } yield Card(digits.takeRight(4), CardBrand.fromNumber(digits), expiry)
  }

  private def luhn(digits: String): Boolean =
    digits.reverse
      .map(_.asDigit)
      .zipWithIndex
      .map { case (d, i) =>
        if (i % 2 == 0) d else if (d * 2 > 9) d * 2 - 9 else d * 2
      }
      .sum % 10 == 0
}
