package paymentservice.domain

import cats.syntax.all._
import sttp.tapir.{Codec, CodecFormat, DecodeResult, Schema}

import java.util.UUID

/** Opaque payment identifier, parsed once at the HTTP boundary (parse, don't
  * validate - scala-fp.md law 5). Inner layers never see a raw String.
  */
opaque type PaymentId = UUID

object PaymentId {

  def parse(raw: String): Either[String, PaymentId] =
    Either
      .catchOnly[IllegalArgumentException](UUID.fromString(raw))
      .leftMap(_ => s"Invalid payment id: '$raw'")

  def fromUuid(value: UUID): PaymentId = value

  def uuid(value: PaymentId): UUID = value

  def render(value: PaymentId): String = value.toString

  given Codec[String, PaymentId, CodecFormat.TextPlain] =
    Codec.string.mapDecode(raw =>
      parse(raw) match {
        case Right(id) => DecodeResult.Value(id)
        case Left(msg) =>
          DecodeResult.Error(raw, new IllegalArgumentException(msg))
      }
    )(render)

  given Schema[PaymentId] = Schema.string[PaymentId]
}

/** Opaque payment-method identifier. Distinct from [[PaymentId]] so the two can
  * never be passed for one another.
  */
opaque type PaymentMethodId = UUID

object PaymentMethodId {

  def parse(raw: String): Either[String, PaymentMethodId] =
    Either
      .catchOnly[IllegalArgumentException](UUID.fromString(raw))
      .leftMap(_ => s"Invalid payment method id: '$raw'")

  def fromUuid(value: UUID): PaymentMethodId = value

  def uuid(value: PaymentMethodId): UUID = value

  def render(value: PaymentMethodId): String = value.toString
}
