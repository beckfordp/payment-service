package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import paymentservice.TestConversions.given
import paymentservice.domain.{PaymentMethodId, PaymentStatus}
import scala.language.implicitConversions

import java.time.Instant
import java.util.UUID

class ExpirySweepSuite extends CatsEffectSuite {

  private val methodId = PaymentMethodId.fromUuid(UUID.randomUUID())

  private val expiredAuthorization =
    Authorization("auth_expired", Instant.parse("2020-01-01T00:00:00Z"))

  test("sweepOnce expires due authorizations and reports the count") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      orderId = UUID.randomUUID().toString
      _ <- store
        .recordAuthorization(orderId, 4999, methodId, expiredAuthorization)
        .map(_.getOrElse(fail("expected the authorization to be inserted")))
      count <- ExpirySweep.sweepOnce(store, NoOpLogger[IO], 10)
      payment <- store.findByOrderId(orderId)
      events <- outbox.get
    } yield {
      assertEquals(count, 1)
      assertEquals(payment.map(_.status), Some(PaymentStatus.Voided))
      assertEquals(
        events.count(_._2.eventType == "PaymentAuthorizationExpired"),
        1
      )
    }
  }

  test("sweepOnce does nothing when no authorization is due") {
    for {
      store <- PaymentStore.inMemory[IO]
      count <- ExpirySweep.sweepOnce(store, NoOpLogger[IO], 10)
    } yield assertEquals(count, 0)
  }
}
