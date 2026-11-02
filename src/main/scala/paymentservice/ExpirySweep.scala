package paymentservice

import cats.Monad
import cats.effect.{Async, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import org.typelevel.log4cats.StructuredLogger
import paymentservice.domain.OrderId

/** US-20.1: the in-process authorization-expiry sweep.
  *
  * A fixed-delay loop, not a CronJob (decided 2026-11-02): each tick moves due
  * `Authorized` payments to `Voided` and writes their
  * `PaymentAuthorizationExpired` outbox rows in one store transaction, so a
  * tick is idempotent and a failure is retried on the next tick. One failed
  * tick is logged and swallowed - the loop must not die with it.
  */
object ExpirySweep {

  /** One tick, exposed for tests: expires up to `limit` due authorizations and
    * returns how many moved.
    */
  def sweepOnce[F[_]: Monad](
      store: PaymentStore[F],
      logger: StructuredLogger[F],
      limit: Int
  ): F[Int] =
    store
      .expireDueAuthorizations(limit)
      .flatMap { expired =>
        expired
          .traverse_ { payment =>
            logger.info(
              Map("order_id" -> OrderId.render(payment.orderId))
            )("Authorization expired - payment voided")
          }
          .as(expired.size)
      }

  def run[F[_]: Async](
      config: ExpirySweepConfig,
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): Stream[F, Unit] =
    Stream
      .fixedDelay[F](config.interval)
      .evalMap { _ =>
        sweepOnce(store, logger, config.batchSize).void.handleErrorWith {
          error =>
            logger.error(Map.empty, error)(
              "Expiry sweep tick failed - retrying next tick"
            )
        }
      }

  /** Backgrounded as one Resource line in Main. */
  def resource[F[_]: Async](
      config: ExpirySweepConfig,
      store: PaymentStore[F],
      logger: StructuredLogger[F]
  ): Resource[F, Unit] =
    run(config, store, logger).compile.drain.background.void
}
