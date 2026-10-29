package paymentservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import paymentservice.domain.{PaymentId, PaymentMethodId}
import io.circe.syntax._
import org.typelevel.otel4s.metrics.Meter
import skunk.{Codec, Session}
import skunk.circe.codec.all.jsonb
import skunk.codec.all._
import skunk.implicits._

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

/** `paymentMethodId`/`authId`/`expiresAt` are only set by `POST
  * /payments/authorize` (US-14.1); `declineReason` is set by an authorize
  * decline or a capture failure (US-19.1). Rows created through the generated
  * CRUD routes leave them empty.
  */
final case class Payment(
    id: PaymentId,
    orderId: String,
    amountCents: Int,
    status: PaymentStatus,
    createdAt: Instant,
    updatedAt: Instant,
    paymentMethodId: Option[PaymentMethodId] = None,
    authId: Option[String] = None,
    expiresAt: Option[Instant] = None,
    declineReason: Option[String] = None
)

trait PaymentStore[F[_]] {
  def create(orderId: String, amountCents: Int): F[Payment]
  def get(id: PaymentId): F[Option[Payment]]
  def update(id: PaymentId, status: PaymentStatus): F[Option[Payment]]
  def delete(id: PaymentId): F[Boolean]
  def ping: F[Boolean]

  /** The single payment row for an order (TD-10.1) — used by
    * `POST /payments/authorize` to replay a prior outcome instead of placing a
    * second hold.
    */
  def findByOrderId(orderId: String): F[Option[Payment]]

  /** Persists an `Authorized` payment and its `payment-events:
    * PaymentAuthorized` outbox row in one transaction (ADR 0008/0011). `None`
    * when the order already has a payment (unique conflict): nothing is written
    * and no outbox row is emitted.
    */
  def recordAuthorization(
      orderId: String,
      amountCents: Int,
      paymentMethodId: PaymentMethodId,
      authorization: Authorization
  ): F[Option[Payment]]

  /** Persists a `Failed` payment with its decline reason. A decline stays
    * synchronous - no outbox row, nothing published. `None` on a unique
    * conflict, same as `recordAuthorization`.
    */
  def recordDecline(
      orderId: String,
      amountCents: Int,
      paymentMethodId: PaymentMethodId,
      reason: DeclineReason
  ): F[Option[Payment]]

  /** Atomically transitions an `Authorized` payment to `Captured` and writes
    * its `PaymentCaptured` outbox row (US-19.1). The conditional update is the
    * idempotency guard: `None` when the order has no authorized payment (a
    * duplicate `DispatchConfirmed` or an unknown order), and nothing is
    * written.
    */
  def recordCapture(orderId: String): F[Option[Payment]]

  /** Atomically transitions an `Authorized` payment to `Failed`, stores the
    * reason, and writes its `PaymentFailed` outbox row (US-19.1). Same `None`
    * semantics as `recordCapture`.
    */
  def recordCaptureFailure(orderId: String, reason: String): F[Option[Payment]]
}

object PaymentStore {

  private val defaultStatus = PaymentStatus.Pending
  private val eventSource = "payment-service"

  /** Envelope for `PaymentAuthorized` (ADR 0009), keyed and correlated by
    * `orderId` - the same choice order-service makes for its own events. Built
    * once here for both interpreters.
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

  private def capturedEnvelope[F[_]: Sync](
      payment: Payment
  ): F[EventEnvelope] =
    for {
      eventId <- Sync[F].delay(UUID.randomUUID().toString)
      timestamp <- Sync[F].realTimeInstant
    } yield EventEnvelope(
      eventId = eventId,
      eventType = "PaymentCaptured",
      timestamp = timestamp,
      source = eventSource,
      correlationId = payment.orderId,
      payload = PaymentCapturedEvent(
        payment.orderId,
        payment.amountCents,
        timestamp
      ).asJson
    )

  private def captureFailedEnvelope[F[_]: Sync](
      payment: Payment,
      reason: String
  ): F[EventEnvelope] =
    for {
      eventId <- Sync[F].delay(UUID.randomUUID().toString)
      timestamp <- Sync[F].realTimeInstant
    } yield EventEnvelope(
      eventId = eventId,
      eventType = "PaymentFailed",
      timestamp = timestamp,
      source = eventSource,
      correlationId = payment.orderId,
      payload = PaymentFailedEvent(
        payment.orderId,
        PaymentId.render(payment.id),
        payment.amountCents,
        reason,
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
      ref <- Ref.of[F, Map[PaymentId, Payment]](Map.empty)
      outboxRef <- Ref.of[F, List[(String, EventEnvelope)]](Nil)
    } yield {
      def insert(
          orderId: String,
          amountCents: Int,
          status: PaymentStatus,
          fill: Payment => Payment
      ): F[Payment] =
        for {
          id <- Sync[F].delay(PaymentId.fromUuid(java.util.UUID.randomUUID()))
          now <- Sync[F].realTimeInstant
          entity = fill(Payment(id, orderId, amountCents, status, now, now))
          _ <- ref.update(_ + (id -> entity))
        } yield entity

      val store = new PaymentStore[F] {
        def create(orderId: String, amountCents: Int): F[Payment] =
          insert(orderId, amountCents, defaultStatus, identity)

        def get(id: PaymentId): F[Option[Payment]] = ref.get.map(_.get(id))

        def update(
            id: PaymentId,
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

        def delete(id: PaymentId): F[Boolean] =
          ref.modify { entities =>
            if (entities.contains(id)) (entities - id, true)
            else (entities, false)
          }

        def ping: F[Boolean] = Sync[F].pure(true)

        def findByOrderId(orderId: String): F[Option[Payment]] =
          ref.get.map(_.values.find(_.orderId == orderId))

        def recordAuthorization(
            orderId: String,
            amountCents: Int,
            paymentMethodId: PaymentMethodId,
            authorization: Authorization
        ): F[Option[Payment]] =
          findByOrderId(orderId).flatMap {
            case Some(_) => Sync[F].pure(None)
            case None    =>
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
              } yield Some(payment)
          }

        def recordDecline(
            orderId: String,
            amountCents: Int,
            paymentMethodId: PaymentMethodId,
            reason: DeclineReason
        ): F[Option[Payment]] =
          findByOrderId(orderId).flatMap {
            case Some(_) => Sync[F].pure(None)
            case None    =>
              insert(
                orderId,
                amountCents,
                PaymentStatus.Failed,
                _.copy(
                  paymentMethodId = Some(paymentMethodId),
                  declineReason = Some(reason.value)
                )
              ).map(Some(_))
          }

        private def transitionFromAuthorized(
            orderId: String,
            status: PaymentStatus,
            reason: Option[String]
        ): F[Option[Payment]] =
          for {
            now <- Sync[F].realTimeInstant
            transitioned <- ref.modify { entities =>
              entities.values.find(_.orderId == orderId) match {
                case Some(existing)
                    if existing.status == PaymentStatus.Authorized =>
                  val next = existing.copy(
                    status = status,
                    updatedAt = now,
                    declineReason = reason.orElse(existing.declineReason)
                  )
                  (entities + (existing.id -> next), Some(next))
                case _ => (entities, None)
              }
            }
            result <- transitioned.traverse { payment =>
              val envelope =
                if (status == PaymentStatus.Captured)
                  capturedEnvelope[F](payment)
                else
                  captureFailedEnvelope[F](
                    payment,
                    reason.getOrElse("capture_failed")
                  )
              envelope.flatMap(env =>
                outboxRef.update(_ :+ (orderId -> env)).as(payment)
              )
            }
          } yield result

        def recordCapture(orderId: String): F[Option[Payment]] =
          transitionFromAuthorized(orderId, PaymentStatus.Captured, None)

        def recordCaptureFailure(
            orderId: String,
            reason: String
        ): F[Option[Payment]] =
          transitionFromAuthorized(orderId, PaymentStatus.Failed, Some(reason))
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

  private def toPayment(id: PaymentId, row: PaymentRow): Payment = {
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
      paymentMethodId.map(PaymentMethodId.fromUuid),
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
      ON CONFLICT (order_id) DO NOTHING
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

  private val paymentWithIdRow: skunk.Decoder[(UUID, PaymentRow)] =
    (uuid *: text *: int4 *: paymentStatus *: timestamptz *: timestamptz *:
      uuid.opt *: text.opt *: timestamptz.opt *: text.opt)
      .map {
        case (
              id,
              orderId,
              amountCents,
              status,
              createdAt,
              updatedAt,
              paymentMethodId,
              authId,
              expiresAt,
              declineReason
            ) =>
          (
            id,
            (
              orderId,
              amountCents,
              status,
              createdAt,
              updatedAt,
              paymentMethodId,
              authId,
              expiresAt,
              declineReason
            )
          )
      }

  private val selectPaymentByOrderId: skunk.Query[String, (UUID, PaymentRow)] =
    sql"""
      SELECT id, order_id, amount_cents, status, created_at, updated_at,
             payment_method_id, auth_id, expires_at, decline_reason
      FROM "payment"
      WHERE order_id = $text
    """.query(paymentWithIdRow)

  /** Conditional `authorized -> captured|failed` transition. The `WHERE status
    * = 'authorized'` guard is the idempotency check: a duplicate
    * `DispatchConfirmed` (or an unknown order) updates nothing and returns no
    * row, so no outbox event is written.
    */
  private val capturePayment: skunk.Query[
    (PaymentStatus, Option[String], String),
    (UUID, PaymentRow)
  ] =
    sql"""
      UPDATE "payment"
      SET status = $paymentStatus, decline_reason = ${text.opt}, updated_at = now()
      WHERE order_id = $text AND status = 'authorized'
      RETURNING id, order_id, amount_cents, status, created_at, updated_at,
                payment_method_id, auth_id, expires_at, decline_reason
    """.query(paymentWithIdRow)

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
                paymentMethodId: PaymentMethodId,
                authId: Option[String],
                expiresAt: Option[Instant],
                declineReason: Option[String],
                outbox: Option[EventEnvelope]
            ): F[Option[Payment]] =
              for {
                id <- Sync[F].delay(UUID.randomUUID())
                methodId = PaymentMethodId.uuid(paymentMethodId)
                row <- timed("insert") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      session
                        .prepare(insertAuthorizeOutcome)
                        .flatMap(
                          _.option(
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
                        .flatMap {
                          case None           => Sync[F].pure(None)
                          case Some(inserted) =>
                            outbox
                              .traverse_ { envelope =>
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
                              .as(Some(inserted))
                        }
                    }
                  }
                }
              } yield row.map(toPayment(PaymentId.fromUuid(id), _))

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
                    PaymentId.fromUuid(id),
                    orderId,
                    amountCents,
                    defaultStatus,
                    createdAt.toInstant,
                    updatedAt.toInstant
                  )
                }

              def get(id: PaymentId): F[Option[Payment]] =
                timed("select") {
                  pool.use { session =>
                    session
                      .prepare(selectPayment)
                      .flatMap(_.option(PaymentId.uuid(id)))
                  }
                }.map(_.map(toPayment(id, _)))

              def update(
                  id: PaymentId,
                  status: PaymentStatus
              ): F[Option[Payment]] =
                timed("update") {
                  pool.use { session =>
                    session
                      .prepare(updatePayment)
                      .flatMap(_.option((status, PaymentId.uuid(id))))
                  }
                }.map(_.map(toPayment(id, _)))

              def delete(id: PaymentId): F[Boolean] =
                timed("delete") {
                  pool.use { session =>
                    session
                      .prepare(deletePayment)
                      .flatMap(_.option(PaymentId.uuid(id)))
                      .map(_.isDefined)
                  }
                }

              def ping: F[Boolean] =
                timed("ping") {
                  pool.use(_.unique(pingQuery))
                }.attempt.map(_.isRight)

              def findByOrderId(orderId: String): F[Option[Payment]] =
                timed("select") {
                  pool.use { session =>
                    session
                      .prepare(selectPaymentByOrderId)
                      .flatMap(_.option(orderId))
                  }
                }.map(_.map { case (id, row) =>
                  toPayment(PaymentId.fromUuid(id), row)
                })

              def recordAuthorization(
                  orderId: String,
                  amountCents: Int,
                  paymentMethodId: PaymentMethodId,
                  authorization: Authorization
              ): F[Option[Payment]] =
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
                  paymentMethodId: PaymentMethodId,
                  reason: DeclineReason
              ): F[Option[Payment]] =
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

              private def transitionFromAuthorized(
                  orderId: String,
                  status: PaymentStatus,
                  reason: Option[String],
                  envelope: Payment => F[EventEnvelope]
              ): F[Option[Payment]] =
                timed("update") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      session
                        .prepare(capturePayment)
                        .flatMap(_.option((status, reason, orderId)))
                        .flatMap {
                          case None            => Sync[F].pure(None)
                          case Some((id, row)) =>
                            val payment = toPayment(PaymentId.fromUuid(id), row)
                            for {
                              event <- envelope(payment)
                              outboxId <- Sync[F].delay(UUID.randomUUID())
                              _ <- session
                                .prepare(insertOutboxEvent)
                                .flatMap(
                                  _.execute(
                                    (
                                      outboxId,
                                      orderId,
                                      event,
                                      event.correlationId
                                    )
                                  )
                                )
                            } yield Some(payment)
                        }
                    }
                  }
                }

              def recordCapture(orderId: String): F[Option[Payment]] =
                transitionFromAuthorized(
                  orderId,
                  PaymentStatus.Captured,
                  None,
                  capturedEnvelope[F]
                )

              def recordCaptureFailure(
                  orderId: String,
                  reason: String
              ): F[Option[Payment]] =
                transitionFromAuthorized(
                  orderId,
                  PaymentStatus.Failed,
                  Some(reason),
                  payment => captureFailedEnvelope[F](payment, reason)
                )
            }
          }
      }
  }
}
