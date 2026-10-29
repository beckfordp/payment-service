package paymentservice

import munit.FunSuite
import paymentservice.domain.{PaymentId, PaymentMethodId}

class PaymentIdSuite extends FunSuite {

  test("PaymentId.parse accepts a canonical UUID and render round-trips it") {
    val raw = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
    assertEquals(PaymentId.parse(raw).map(PaymentId.render), Right(raw))
  }

  test("PaymentId.parse rejects a malformed id") {
    assert(PaymentId.parse("not-a-uuid").isLeft)
  }

  test(
    "PaymentMethodId.parse accepts a canonical UUID and render round-trips it"
  ) {
    val raw = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
    assertEquals(
      PaymentMethodId.parse(raw).map(PaymentMethodId.render),
      Right(raw)
    )
  }

  test("PaymentMethodId.parse rejects a malformed id") {
    assert(PaymentMethodId.parse("not-a-uuid").isLeft)
  }
}
