package paymentservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import io.circe.syntax._
import org.typelevel.otel4s.metrics.Meter
import skunk.{Codec, Session}
import skunk.circe.codec.all.jsonb
import skunk.codec.all._
import skunk.implicits._

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

/** `paymentMethodId`/`authId`/`expiresAt`/`declineReason` are only set by
  * `POST /payments/authorize` (US-14.1) - rows created through the generated
  * CRUD routes leave them empty.
  */
final case class Payment(
    id: String,
    orderId: String,
    amountCents: Int,
    status: PaymentStatus,
    createdAt: Instant,
    updatedAt: Instant,
    paymentMethodId: Option[String] = None,
    authId: Option[String] = None,
    expiresAt: Option[Instant] = None,
    declineReason: Option[String] = None
)

trait PaymentStore[F[_]] {
  def create(orderId: String, amountCents: Int): F[Payment]
  def get(id: String): F[Option[Payment]]
  def update(id: String, status: PaymentStatus): F[Option[Payment]]
  def delete(id: String): F[Boolean]
  def ping: F[Boolean]

  /** Persists an `Authorized` payment and its `payment-events:
    * PaymentAuthorized` outbox row in one transaction (ADR 0008/0011).
    */
  def recordAuthorization(
      orderId: String,
      amountCents: Int,
      paymentMethodId: String,
      authorization: Authorization
  ): F[Payment]

  /** Persists a `Failed` payment with its decline reason. A decline stays
    * synchronous - no outbox row, nothing published.
    */
  def recordDecline(
      orderId: String,
      amountCents: Int,
      paymentMethodId: String,
      reason: DeclineReason
  ): F[Payment]
}

object PaymentStore {

  private val defaultStatus = PaymentStatus.Pending
  private val eventSource = "payment-service"

  /** Envelope for `PaymentAuthorized` (ADR 0009), keyed and correlated by
    * `orderId` - the same choice order-service makes for its own events.
    * Built once here for both interpreters.
    */
  private def authorizedEnvelope[F[_]: Sync](
      orderId: String,
      authorization: Authorization
  ): F[EventEnvelope] =
    for {
      eventId <- Sync[F].delay(UUID.randomUUID().toString)
      timestamp <- Sync[F].realTimeInstant
    } yield EventEnvelope(
      eventId = eventId,
      eventType = "PaymentAuthorized",
      timestamp = timestamp,
      source = eventSource,
      correlationId = orderId,
      payload = PaymentAuthorizedEvent(
        orderId,
        authorization.authId,
        authorization.expiresAt,
        timestamp
      ).asJson
    )

  // Column stays plain `text` at the DB level (hardened by a CHECK constraint,
  // not a native Postgres enum type - see V2__add_payment_status_check.sql);
  // this eimap handles the Scala<->text mapping and surfaces a decode failure
  // for any value outside the closed PaymentStatus set.
  private val paymentStatus: Codec[PaymentStatus] =
    text.eimap(PaymentStatus.fromString)(_.asString)

  def inMemory[F[_]: Sync]: F[PaymentStore[F]] =
    inMemoryWithOutbox[F].map(_._1)

  /** Same as `inMemory`, but also returns a `Ref` capturing every outbox write
    * (aggregateId, envelope) - mirrors order-service's `inMemoryWithOutbox`.
    */
  def inMemoryWithOutbox[F[_]: Sync]
      : F[(PaymentStore[F], Ref[F, List[(String, EventEnvelope)]])] =
    for {
      ref <- Ref.of[F, Map[String, Payment]](Map.empty)
      outboxRef <- Ref.of[F, List[(String, EventEnvelope)]](Nil)
    } yield {
      def insert(
          orderId: String,
          amountCents: Int,
          status: PaymentStatus,
          fill: Payment => Payment
      ): F[Payment] =
        for {
          id <- Sync[F].delay(java.util.UUID.randomUUID().toString)
          now <- Sync[F].realTimeInstant
          entity = fill(Payment(id, orderId, amountCents, status, now, now))
          _ <- ref.update(_ + (id -> entity))
        } yield entity

      val store = new PaymentStore[F] {
        def create(orderId: String, amountCents: Int): F[Payment] =
          insert(orderId, amountCents, defaultStatus, identity)

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

        def recordAuthorization(
            orderId: String,
            amountCents: Int,
            paymentMethodId: String,
            authorization: Authorization
        ): F[Payment] =
          for {
            envelope <- authorizedEnvelope[F](orderId, authorization)
            payment <- insert(
              orderId,
              amountCents,
              PaymentStatus.Authorized,
              _.copy(
                paymentMethodId = Some(paymentMethodId),
                authId = Some(authorization.authId),
                expiresAt = Some(authorization.expiresAt)
              )
            )
            _ <- outboxRef.update(_ :+ (orderId -> envelope))
          } yield payment

        def recordDecline(
            orderId: String,
            amountCents: Int,
            paymentMethodId: String,
            reason: DeclineReason
        ): F[Payment] =
          insert(
            orderId,
            amountCents,
            PaymentStatus.Failed,
            _.copy(
              paymentMethodId = Some(paymentMethodId),
              declineReason = Some(reason.value)
            )
          )
      }
      (store, outboxRef)
    }

  private type PaymentRow = (
      String,
      Int,
      PaymentStatus,
      OffsetDateTime,
      OffsetDateTime,
      Option[UUID],
      Option[String],
      Option[OffsetDateTime],
      Option[String]
  )

  private val paymentRow: skunk.Decoder[PaymentRow] =
    text *: int4 *: paymentStatus *: timestamptz *: timestamptz *:
      uuid.opt *: text.opt *: timestamptz.opt *: text.opt

  private def toPayment(id: String, row: PaymentRow): Payment = {
    val (
      orderId,
      amountCents,
      status,
      createdAt,
      updatedAt,
      paymentMethodId,
      authId,
      expiresAt,
      declineReason
    ) = row
    Payment(
      id,
      orderId,
      amountCents,
      status,
      createdAt.toInstant,
      updatedAt.toInstant,
      paymentMethodId.map(_.toString),
      authId,
      expiresAt.map(_.toInstant),
      declineReason
    )
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

  private val insertAuthorizeOutcome: skunk.Query[
    (
        UUID,
        String,
        Int,
        PaymentStatus,
        UUID,
        Option[String],
        Option[OffsetDateTime],
        Option[String]
    ),
    PaymentRow
  ] =
    sql"""
      INSERT INTO "payment"
        (id, order_id, amount_cents, status, payment_method_id, auth_id,
         expires_at, decline_reason)
      VALUES ($uuid, $text, $int4, $paymentStatus, $uuid, ${text.opt},
              ${timestamptz.opt}, ${text.opt})
      RETURNING order_id, amount_cents, status, created_at, updated_at,
                payment_method_id, auth_id, expires_at, decline_reason
    """.query(paymentRow)

  private val selectPayment: skunk.Query[UUID, PaymentRow] =
    sql"""
      SELECT order_id, amount_cents, status, created_at, updated_at,
             payment_method_id, auth_id, expires_at, decline_reason
      FROM "payment"
      WHERE id = $uuid
    """.query(paymentRow)

  private val updatePayment: skunk.Query[(PaymentStatus, UUID), PaymentRow] =
    sql"""
      UPDATE "payment"
      SET status = $paymentStatus, updated_at = now()
      WHERE id = $uuid
      RETURNING order_id, amount_cents, status, created_at, updated_at,
                payment_method_id, auth_id, expires_at, decline_reason
    """.query(paymentRow)

  private val deletePayment: skunk.Query[UUID, UUID] =
    sql"""
      DELETE FROM "payment"
      WHERE id = $uuid
      RETURNING id
    """.query(uuid)

  // `trace_parent` stays NULL - nothing in this service propagates trace
  // context into Kafka headers yet (same as order-service, ADR 0010's note).
  private val insertOutboxEvent
      : skunk.Command[(UUID, String, EventEnvelope, String)] =
    sql"""
      INSERT INTO outbox_events (id, aggregate_id, payload, correlation_id)
      VALUES ($uuid, $text, ${jsonb[EventEnvelope]}, $text)
    """.command

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
        DbTimer
          .create(meter)
          .map { timer =>
            import timer.timed

            def insertOutcome(
                orderId: String,
                amountCents: Int,
                status: PaymentStatus,
                paymentMethodId: String,
                authId: Option[String],
                expiresAt: Option[Instant],
                declineReason: Option[String],
                outbox: Option[EventEnvelope]
            ): F[Payment] =
              for {
                id <- Sync[F].delay(UUID.randomUUID())
                methodId <- Sync[F].delay(UUID.fromString(paymentMethodId))
                row <- timed("insert") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      for {
                        row <- session
                          .prepare(insertAuthorizeOutcome)
                          .flatMap(
                            _.unique(
                              (
                                id,
                                orderId,
                                amountCents,
                                status,
                                methodId,
                                authId,
                                expiresAt.map(_.atOffset(ZoneOffset.UTC)),
                                declineReason
                              )
                            )
                          )
                        _ <- outbox.traverse_ { envelope =>
                          Sync[F]
                            .delay(UUID.randomUUID())
                            .flatMap { outboxId =>
                              session
                                .prepare(insertOutboxEvent)
                                .flatMap(
                                  _.execute(
                                    (
                                      outboxId,
                                      orderId,
                                      envelope,
                                      envelope.correlationId
                                    )
                                  )
                                )
                            }
                        }
                      } yield row
                    }
                  }
                }
              } yield toPayment(id.toString, row)

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
                    timed("select") {
                      pool.use { session =>
                        session.prepare(selectPayment).flatMap(_.option(uuid))
                      }
                    }.map(_.map(toPayment(id, _)))
                }

              def update(
                  id: String,
                  status: PaymentStatus
              ): F[Option[Payment]] =
                scala.util.Try(UUID.fromString(id)).toOption match {
                  case None       => Sync[F].pure(None)
                  case Some(uuid) =>
                    timed("update") {
                      pool.use { session =>
                        session
                          .prepare(updatePayment)
                          .flatMap(_.option((status, uuid)))
                      }
                    }.map(_.map(toPayment(id, _)))
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

              def recordAuthorization(
                  orderId: String,
                  amountCents: Int,
                  paymentMethodId: String,
                  authorization: Authorization
              ): F[Payment] =
                authorizedEnvelope[F](orderId, authorization).flatMap {
                  envelope =>
                    insertOutcome(
                      orderId,
                      amountCents,
                      PaymentStatus.Authorized,
                      paymentMethodId,
                      Some(authorization.authId),
                      Some(authorization.expiresAt),
                      None,
                      Some(envelope)
                    )
                }

              def recordDecline(
                  orderId: String,
                  amountCents: Int,
                  paymentMethodId: String,
                  reason: DeclineReason
              ): F[Payment] =
                insertOutcome(
                  orderId,
                  amountCents,
                  PaymentStatus.Failed,
                  paymentMethodId,
                  None,
                  None,
                  Some(reason.value),
                  None
                )
            }
          }
      }
  }
}
