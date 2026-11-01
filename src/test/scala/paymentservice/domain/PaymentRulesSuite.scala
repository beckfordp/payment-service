package paymentservice.domain

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class PaymentRulesSuite extends ScalaCheckSuite {

  test("a non-blank orderId and positive amount parse to the OrderId") {
    assertEquals(
      PaymentRules.validate("order-1", 4999).map(OrderId.render),
      Right("order-1")
    )
  }

  test("a blank orderId is rejected before the amount") {
    assertEquals(
      PaymentRules.validate("  ", 0),
      Left("orderId is required")
    )
  }

  property("a non-positive amount is rejected") {
    forAll(Gen.choose(Int.MinValue, 0)) { amount =>
      PaymentRules.validate("order-1", amount) ==
        Left("amountCents must be positive")
    }
  }
}
