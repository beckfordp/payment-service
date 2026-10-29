package paymentservice.domain

import cats.syntax.all._
import io.circe.{Decoder, Encoder}
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

  given Decoder[PaymentMethodId] = Decoder.decodeString.emap(parse)
  given Encoder[PaymentMethodId] = Encoder.encodeString.contramap(render)
  given Schema[PaymentMethodId] = Schema.string[PaymentMethodId]
}

/** order-service's order id as this service references it (no FK - a different
  * database). Opaque over the raw string so the idempotency key, outbox
  * aggregate id and Kafka key can't be confused with any other string; the only
  * rule this side enforces is non-blank.
  */
opaque type OrderId = String

object OrderId {

  def parse(raw: String): Either[String, OrderId] =
    Either.cond(raw.trim.nonEmpty, raw, "orderId is required")

  /** For values this service already validated - its own database rows. */
  def trusted(raw: String): OrderId = raw

  def render(value: OrderId): String = value
}
