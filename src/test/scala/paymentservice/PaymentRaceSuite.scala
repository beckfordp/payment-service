package paymentservice

import munit.FunSuite
import paymentservice.domain.{PaymentRace, PaymentStatus}

class PaymentRaceSuite extends FunSuite {

  import PaymentRace.Outcome

  test("the requested terminal state is a duplicate") {
    assertEquals(
      PaymentRace.classify(PaymentStatus.Voided, PaymentStatus.Voided),
      Outcome.Duplicate
    )
    assertEquals(
      PaymentRace.classify(PaymentStatus.Captured, PaymentStatus.Captured),
      Outcome.Duplicate
    )
  }

  test("contradictory terminal states are flagged losers") {
    assertEquals(
      PaymentRace.classify(PaymentStatus.Voided, PaymentStatus.Captured),
      Outcome.Lost("void_after_capture")
    )
    assertEquals(
      PaymentRace.classify(PaymentStatus.Captured, PaymentStatus.Voided),
      Outcome.Lost("capture_after_void")
    )
    assertEquals(
      PaymentRace.classify(PaymentStatus.Captured, PaymentStatus.Failed),
      Outcome.Lost("capture_after_failed")
    )
  }

  test("a failed authorization losing to a void/capture is benign or flagged") {
    assertEquals(
      PaymentRace.classify(PaymentStatus.Failed, PaymentStatus.Voided),
      Outcome.Benign
    )
    assertEquals(
      PaymentRace.classify(PaymentStatus.Voided, PaymentStatus.Failed),
      Outcome.Benign
    )
    assertEquals(
      PaymentRace.classify(PaymentStatus.Failed, PaymentStatus.Captured),
      Outcome.Lost("capture_failure_after_capture")
    )
  }

  test("an unexpected state needs the DLQ") {
    assertEquals(
      PaymentRace.classify(PaymentStatus.Voided, PaymentStatus.Pending),
      Outcome.Unexpected
    )
    assertEquals(
      PaymentRace.classify(PaymentStatus.Captured, PaymentStatus.Pending),
      Outcome.Unexpected
    )
  }
}
