package paymentservice

import cats.Monad
import cats.effect.{Async, Clock, Resource}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import paymentservice.domain.{OrderId, PaymentMethodId, PaymentStatus}
import purerest.logging.Milestones
import skunk.Session
import skunk.codec.all._
import skunk.implicits._

import java.time.Instant

/** Payment-journey business telemetry (gluon PERF-4.5, 4.11), as a
  * `PaymentStore` decorator — every authorize, decline, capture, void and
  * expiry goes through the store, so wrapping it sees them all:
  *
  *   - `payments.authorize.outcome{outcome=authorized|declined}`
  *   - `payments.capture.outcome{outcome=captured|failed}`
  *   - `payments.voided`, `payments.authorizations.expired`
  *
  * Plus a `Milestones` log line per step. Order ids go on logs, never labels.
  */
object PaymentJourneyMetrics {

  def instrument[F[_]: Monad](
      store: PaymentStore[F],
      meter: Meter[F],
      logger: StructuredLogger[F]
  ): F[PaymentStore[F]] =
    (
      meter.counter[Long]("payments.authorize.outcome").create,
      meter.counter[Long]("payments.capture.outcome").create,
      meter.counter[Long]("payments.voided").create,
      meter.counter[Long]("payments.authorizations.expired").create
    ).mapN { (authorize, capture, voided, expired) =>
      def milestone(name: String, orderId: OrderId): F[Unit] =
        Milestones.log(logger)(name, OrderId.render(orderId), None)

      new PaymentStore[F] {
        export store.{
          recordAuthorization => _,
          recordDecline => _,
          recordCapture => _,
          recordCaptureFailure => _,
          recordVoid => _,
          expireDueAuthorizations => _,
          expireAuthorizationNow => _,
          *
        }

        def recordAuthorization(
            orderId: OrderId,
            amountCents: Int,
            paymentMethodId: PaymentMethodId,
            authorization: Authorization
        ): F[Option[Payment]] =
          store
            .recordAuthorization(
              orderId,
              amountCents,
              paymentMethodId,
              authorization
            )
            .flatTap(
              _.traverse_(_ =>
                authorize.inc(Attribute("outcome", "authorized")) *>
                  milestone("payment_authorized", orderId)
              )
            )

        def recordDecline(
            orderId: OrderId,
            amountCents: Int,
            paymentMethodId: PaymentMethodId,
            reason: DeclineReason
        ): F[Option[Payment]] =
          store
            .recordDecline(orderId, amountCents, paymentMethodId, reason)
            .flatTap(
              _.traverse_(_ =>
                authorize.inc(Attribute("outcome", "declined")) *>
                  milestone("payment_declined", orderId)
              )
            )

        def recordCapture(orderId: OrderId): F[Option[Payment]] =
          store
            .recordCapture(orderId)
            .flatTap(
              _.traverse_(_ =>
                capture.inc(Attribute("outcome", "captured")) *>
                  milestone("payment_captured", orderId)
              )
            )

        def recordCaptureFailure(
            orderId: OrderId,
            reason: String
        ): F[Option[Payment]] =
          store
            .recordCaptureFailure(orderId, reason)
            .flatTap(
              _.traverse_(_ =>
                capture.inc(Attribute("outcome", "failed")) *>
                  milestone("payment_capture_failed", orderId)
              )
            )

        def recordVoid(orderId: OrderId): F[Option[Payment]] =
          store
            .recordVoid(orderId)
            .flatTap(
              _.traverse_(_ =>
                voided.inc() *> milestone("payment_voided", orderId)
              )
            )

        def expireDueAuthorizations(limit: Int): F[List[Payment]] =
          store
            .expireDueAuthorizations(limit)
            .flatTap(
              _.traverse_(p =>
                expired.inc() *> milestone("payment_expired", p.orderId)
              )
            )

        def expireAuthorizationNow(orderId: OrderId): F[Option[Payment]] =
          store.expireAuthorizationNow(orderId)
      }
    }
}

/** Count and oldest authorization time of payments still `Authorized` — not yet
  * captured, voided or expired.
  */
final case class UncapturedAuthorizations(
    count: Long,
    oldestAuthorizedAt: Option[Instant]
)

/** Uncaptured-authorization gauges (gluon PERF-4.5, Q15): how many orders are
  * holding a customer's funds and how long the oldest has waited — the number
  * that grows when dispatch falls behind, ahead of the US-20 expiry sweep.
  * Observed at scrape time: each gauge runs the query once per scrape.
  */
object UncapturedAuthorizationGauges {

  def register[F[_]: Monad: Clock](meter: Meter[F])(
      query: F[UncapturedAuthorizations]
  ): Resource[F, Unit] =
    meter
      .observableGauge[Long]("payments.uncaptured.count")
      .createWithCallback(cb => query.flatMap(stats => cb.record(stats.count)))
      .flatMap(_ =>
        meter
          .observableGauge[Double]("payments.uncaptured.oldest_age")
          .withUnit("s")
          .createWithCallback(cb =>
            (query, Clock[F].realTimeInstant).flatMapN { (stats, now) =>
              cb.record(
                stats.oldestAuthorizedAt.fold(0.0)(at =>
                  math.max(0L, now.toEpochMilli - at.toEpochMilli) / 1000.0
                )
              )
            }
          )
      )
      .void

  /** The same view over in-memory payments, for tests. */
  def fromPayments(payments: List[Payment]): UncapturedAuthorizations = {
    val authorized = payments.filter(_.status == PaymentStatus.Authorized)
    UncapturedAuthorizations(
      authorized.size.toLong,
      authorized.map(_.updatedAt).minOption
    )
  }

  /** The Postgres query, on its own one-connection pool so scrapes never
    * compete with request traffic for the store's connections.
    */
  def postgresQuery[F[_]: Async: Console: Network](
      config: PostgresConfig
  ): Resource[F, F[UncapturedAuthorizations]] = {
    import org.typelevel.otel4s.trace.Tracer.Implicits.noop
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    val query = sql"""
      SELECT count(*), min(updated_at) FROM "payment" WHERE status = 'authorized'
    """.query(int8 *: timestamptz.opt)
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .pooled(max = 1)
      .map { pool =>
        pool.use(_.unique(query)).map { case (count, oldest) =>
          UncapturedAuthorizations(count, oldest.map(_.toInstant))
        }
      }
  }
}
