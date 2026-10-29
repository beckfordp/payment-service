package paymentservice

import paymentservice.TestConversions.given
import scala.language.implicitConversions
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import paymentservice.domain.{PaymentId, PaymentMethodId}

class PaymentIdPropertySuite extends ScalaCheckSuite {

  property("PaymentId.parse then render is the identity for any UUID") {
    forAll(Gen.uuid) { uuid =>
      val raw = uuid.toString
      assertEquals(PaymentId.parse(raw).map(PaymentId.render), Right(raw))
    }
  }

  property(
    "PaymentMethodId.parse then render is the identity for any UUID"
  ) {
    forAll(Gen.uuid) { uuid =>
      val raw = uuid.toString
      assertEquals(
        PaymentMethodId.parse(raw).map(PaymentMethodId.render),
        Right(raw)
      )
    }
  }

  property("every PaymentStatus string round-trips") {
    val statuses = List(
      PaymentStatus.Pending,
      PaymentStatus.Authorized,
      PaymentStatus.Captured,
      PaymentStatus.Failed,
      PaymentStatus.Voided
    )
    forAll(Gen.oneOf(statuses)) { status =>
      assertEquals(
        PaymentStatus.fromString(status.asString),
        Right(status)
      )
    }
  }
}
