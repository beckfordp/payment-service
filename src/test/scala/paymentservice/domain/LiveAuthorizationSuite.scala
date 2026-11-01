package paymentservice.domain

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import java.time.Instant

class LiveAuthorizationSuite extends ScalaCheckSuite {

  private val now = Instant.parse("2026-11-01T12:00:00Z")

  private val statuses = Gen.oneOf(
    PaymentStatus.Pending,
    PaymentStatus.Authorized,
    PaymentStatus.Captured,
    PaymentStatus.Failed,
    PaymentStatus.Voided
  )

  test("only a capture needs a live authorization") {
    assert(LiveAuthorization.required(PaymentStatus.Captured))
    List(
      PaymentStatus.Pending,
      PaymentStatus.Authorized,
      PaymentStatus.Failed,
      PaymentStatus.Voided
    ).foreach(s => assert(!LiveAuthorization.required(s), s))
  }

  test("a missing expiry is never live") {
    assert(!LiveAuthorization.isLive(None, now))
  }

  property("live exactly when the expiry is strictly after now") {
    forAll(Gen.choose(-86400L, 86400L)) { seconds =>
      LiveAuthorization.isLive(Some(now.plusSeconds(seconds)), now) ==
        (seconds > 0)
    }
  }

  property("permits blocks only an expired capture") {
    forAll(statuses, Gen.choose(-86400L, 86400L)) { (target, seconds) =>
      LiveAuthorization.permits(
        target,
        Some(now.plusSeconds(seconds)),
        now
      ) == (target != PaymentStatus.Captured || seconds > 0)
    }
  }
}
