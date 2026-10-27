package paymentservice

import munit.CatsEffectSuite

class PaymentStatusSuite extends CatsEffectSuite {

  test("fromString parses \"pending\" as Pending") {
    assertEquals(
      PaymentStatus.fromString("pending"),
      Right(PaymentStatus.Pending)
    )
  }

  test("fromString parses \"authorized\" as Authorized") {
    assertEquals(
      PaymentStatus.fromString("authorized"),
      Right(PaymentStatus.Authorized)
    )
  }

  test("fromString parses \"captured\" as Captured") {
    assertEquals(
      PaymentStatus.fromString("captured"),
      Right(PaymentStatus.Captured)
    )
  }

  test("fromString parses \"failed\" as Failed") {
    assertEquals(
      PaymentStatus.fromString("failed"),
      Right(PaymentStatus.Failed)
    )
  }

  test("fromString rejects the retired \"settled\" value") {
    assert(PaymentStatus.fromString("settled").isLeft)
  }

  test("fromString rejects an unrecognized value") {
    assert(PaymentStatus.fromString("bogus").isLeft)
  }

  test("asString round-trips through fromString for every status") {
    List(
      PaymentStatus.Pending,
      PaymentStatus.Authorized,
      PaymentStatus.Captured,
      PaymentStatus.Failed
    ).foreach { status =>
      assertEquals(PaymentStatus.fromString(status.asString), Right(status))
    }
  }
}
