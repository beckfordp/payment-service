package paymentservice

import cats.effect.IO
import paymentservice.domain.{OrderId, PaymentMethodId}

import scala.language.implicitConversions

/** Test-only literals: lets suites pass `"order-1"` (or a rendered method id)
  * where the typed id is expected. Never imported from main code, which parses
  * at its boundaries instead.
  */
object TestConversions {
  given Conversion[String, OrderId] = OrderId.trusted

  given Conversion[String, PaymentMethodId] = raw =>
    PaymentMethodId
      .parse(raw)
      .fold(e => throw new IllegalArgumentException(e), identity)

  /** A create the test expects to succeed - fails the test on a conflict. */
  extension (store: PaymentStore[IO])
    def createOrFail(orderId: OrderId, amountCents: Int): IO[Payment] =
      store
        .create(orderId, amountCents)
        .flatMap(
          _.fold(
            conflict => IO.raiseError(new AssertionError(conflict.message)),
            IO.pure
          )
        )
}
