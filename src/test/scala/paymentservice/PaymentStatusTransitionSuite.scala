package paymentservice

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class PaymentStatusTransitionSuite extends ScalaCheckSuite {

  import PaymentStatus._

  private val all: List[PaymentStatus] =
    List(Pending, Authorized, Captured, Failed, Voided)

  private val legal: Set[(PaymentStatus, PaymentStatus)] = Set(
    Pending -> Authorized,
    Pending -> Failed,
    Authorized -> Captured,
    Authorized -> Voided,
    Authorized -> Failed
  )

  private val status: Gen[PaymentStatus] = Gen.oneOf(all)

  test("every legal transition is accepted and yields the target status") {
    legal.foreach { case (from, to) =>
      assertEquals(PaymentStatus.transition(from, to), Right(to))
    }
  }

  property("every other pair is rejected with IllegalTransition") {
    forAll(status, status) { (from, to) =>
      if (!legal.contains(from -> to))
        assertEquals(
          PaymentStatus.transition(from, to),
          Left(IllegalTransition(from, to))
        )
    }
  }

  property("captured, voided and failed are terminal") {
    forAll(Gen.oneOf(Captured, Voided, Failed), status) { (from, to) =>
      assert(PaymentStatus.transition(from, to).isLeft)
    }
  }
}
