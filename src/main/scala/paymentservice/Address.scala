package paymentservice

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

/** Shared shipping/billing shape pinned in `gluon/docs/system-design.md`'s
  * "REST contracts" (`Address`). payment-service only uses it as the billing
  * address on a `PaymentMethod` (US-12.1).
  */
@description("Billing address.")
final case class Address(
    @description("Street address.") @encodedExample("1 High Street")
    line1: String,
    @description("Optional second address line.") @encodedExample("Flat 2")
    line2: Option[String],
    @description("City or town.") @encodedExample("London")
    city: String,
    @description("Postal code.") @encodedExample("SW1A 1AA")
    postalCode: String,
    @description("ISO 3166-1 alpha-2 country code.") @encodedExample("GB")
    country: String
)

object Address {
  implicit val codec: Codec[Address] = deriveCodec
  implicit val schema: Schema[Address] = Schema.derived

  private val alpha2 = "^[A-Z]{2}$".r

  def validate(address: Address): Either[String, Address] =
    for {
      _ <- required("line1", address.line1)
      _ <- required("city", address.city)
      _ <- required("postalCode", address.postalCode)
      _ <- Either.cond(
        alpha2.matches(address.country),
        (),
        "country must be an ISO 3166-1 alpha-2 code"
      )
    } yield address

  private def required(field: String, value: String): Either[String, Unit] =
    Either.cond(value.trim.nonEmpty, (), s"$field is required")
}
