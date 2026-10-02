package paymentservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import skunk.{Codec, Session}
import skunk.codec.all._
import skunk.implicits._

import java.time.OffsetDateTime
import java.util.UUID
import scala.concurrent.duration.SECONDS

final case class Payment(
    id: String,
    orderId: String,
    amountCents: Int,
    status: PaymentStatus,
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

trait PaymentStore[F[_]] {
  def create(orderId: String, amountCents: Int): F[Payment]
  def get(id: String): F[Option[Payment]]
  def update(id: String, status: PaymentStatus): F[Option[Payment]]
  def delete(id: String): F[Boolean]
  def ping: F[Boolean]
}

object PaymentStore {

  private val defaultStatus = PaymentStatus.Pending

  // Column stays plain `text` at the DB level (hardened by a CHECK constraint,
  // not a native Postgres enum type - see V2__add_payment_status_check.sql);
  // this eimap handles the Scala<->text mapping and surfaces a decode failure
  // for any value outside the closed PaymentStatus set.
  private val paymentStatus: Codec[PaymentStatus] =
    text.eimap(PaymentStatus.fromString)(_.asString)

  def inMemory[F[_]: Sync]: F[PaymentStore[F]] =
    Ref.of[F, Map[String, Payment]](Map.empty).map { ref =>
      new PaymentStore[F] {
        def create(orderId: String, amountCents: Int): F[Payment] =
          for {
            id <- Sync[F].delay(java.util.UUID.randomUUID().toString)
            now <- Sync[F].realTimeInstant
            entity = Payment(id, orderId, amountCents, defaultStatus, now, now)
            _ <- ref.update(_ + (id -> entity))
          } yield entity

        def get(id: String): F[Option[Payment]] = ref.get.map(_.get(id))

        def update(
            id: String,
            status: PaymentStatus
        ): F[Option[Payment]] =
          for {
            now <- Sync[F].realTimeInstant
            updated <- ref.modify { entities =>
              entities.get(id) match {
                case None           => (entities, None)
                case Some(existing) =>
                  val next =
                    existing.copy(
                      status = status,
                      updatedAt = now
                    )
                  (entities + (id -> next), Some(next))
              }
            }
          } yield updated

        def delete(id: String): F[Boolean] =
          ref.modify { entities =>
            if (entities.contains(id)) (entities - id, true)
            else (entities, false)
          }

        def ping: F[Boolean] = Sync[F].pure(true)
      }
    }

  private val insertPayment: skunk.Query[
    (UUID, String, Int, PaymentStatus),
    (OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      INSERT INTO "payment" (id, order_id, amount_cents, status)
      VALUES ($uuid, $text, $int4, $paymentStatus)
      RETURNING created_at, updated_at
    """.query(timestamptz *: timestamptz)

  private val selectPayment: skunk.Query[
    UUID,
    (String, Int, PaymentStatus, OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      SELECT order_id, amount_cents, status, created_at, updated_at
      FROM "payment"
      WHERE id = $uuid
    """.query(text *: int4 *: paymentStatus *: timestamptz *: timestamptz)

  private val updatePayment: skunk.Query[
    (PaymentStatus, UUID),
    (String, Int, PaymentStatus, OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      UPDATE "payment"
      SET status = $paymentStatus, updated_at = now()
      WHERE id = $uuid
      RETURNING order_id, amount_cents, status, created_at, updated_at
    """.query(text *: int4 *: paymentStatus *: timestamptz *: timestamptz)

  private val deletePayment: skunk.Query[UUID, UUID] =
    sql"""
      DELETE FROM "payment"
      WHERE id = $uuid
      RETURNING id
    """.query(uuid)

  private val pingQuery: skunk.Query[skunk.Void, Int] = sql"SELECT 1".query(
    int4
  )

  def postgres[F[_]: Async: Console: Network](
      config: PostgresConfig,
      meter: Meter[F]
  ): Resource[F, PaymentStore[F]] = {
    import org.typelevel.otel4s.trace.Tracer.Implicits.noop
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .pooled(max = 10)
      .evalMap { pool =>
        meter
          .histogram[Double]("db.client.operation.duration")
          .withUnit("s")
          .create
          .map { histogram =>
            /** Times a Skunk query, recording a `db.client.operation.duration`
              * measurement tagged with `db.system`/`db.operation` (OTel
              * semantic-convention names), plus `error.type` if it fails - this
              * is this service's only Postgres consumer, so it's instrumented
              * directly here rather than via a new purerest combinator.
              */
            def timed[A](operation: String)(fa: F[A]): F[A] =
              for {
                start <- Async[F].monotonic
                result <- fa.attempt
                end <- Async[F].monotonic
                outcomeAttributes = result match {
                  case Right(_)    => Nil
                  case Left(error) =>
                    List(Attribute("error.type", error.getClass.getName))
                }
                _ <- histogram.record(
                  (end - start).toUnit(SECONDS),
                  List(
                    Attribute("db.system", "postgresql"),
                    Attribute("db.operation", operation)
                  ) ++ outcomeAttributes
                )
                a <- result.liftTo[F]
              } yield a

            new PaymentStore[F] {
              def create(orderId: String, amountCents: Int): F[Payment] =
                for {
                  id <- Sync[F].delay(UUID.randomUUID())
                  timestamps <- timed("insert") {
                    pool.use { session =>
                      session
                        .prepare(insertPayment)
                        .flatMap(
                          _.unique((id, orderId, amountCents, defaultStatus))
                        )
                    }
                  }
                } yield {
                  val (createdAt, updatedAt) = timestamps
                  Payment(
                    id.toString,
                    orderId,
                    amountCents,
                    defaultStatus,
                    createdAt.toInstant,
                    updatedAt.toInstant
                  )
                }

              def get(id: String): F[Option[Payment]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    for {
                      row <- timed("select") {
                        pool.use { session =>
                          session.prepare(selectPayment).flatMap(_.option(uuid))
                        }
                      }
                    } yield row.map {
                      case (
                            orderId,
                            amountCents,
                            status,
                            createdAt,
                            updatedAt
                          ) =>
                        Payment(
                          id,
                          orderId,
                          amountCents,
                          status,
                          createdAt.toInstant,
                          updatedAt.toInstant
                        )
                    }
                }

              def update(
                  id: String,
                  status: PaymentStatus
              ): F[Option[Payment]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    for {
                      row <- timed("update") {
                        pool.use { session =>
                          session
                            .prepare(updatePayment)
                            .flatMap(_.option((status, uuid)))
                        }
                      }
                    } yield row.map {
                      case (
                            orderId,
                            amountCents,
                            status,
                            createdAt,
                            updatedAt
                          ) =>
                        Payment(
                          id,
                          orderId,
                          amountCents,
                          status,
                          createdAt.toInstant,
                          updatedAt.toInstant
                        )
                    }
                }

              def delete(id: String): F[Boolean] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(false)
                  case Some(uuid) =>
                    timed("delete") {
                      pool.use { session =>
                        session
                          .prepare(deletePayment)
                          .flatMap(_.option(uuid))
                          .map(_.isDefined)
                      }
                    }
                }

              def ping: F[Boolean] =
                timed("ping") {
                  pool.use(_.unique(pingQuery))
                }.attempt.map(_.isRight)
            }
          }
      }
  }
}
