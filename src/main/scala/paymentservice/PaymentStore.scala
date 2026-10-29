package paymentservice

import cats.Monad
import cats.effect.{Async, Clock, Ref, Resource, Sync}
import cats.effect.std.UUIDGen
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import paymentservice.domain.{OrderId, PaymentId, PaymentMethodId}
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
    orderId: OrderId,
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

  /** Raises [[PaymentAlreadyExists]] when the order already has a payment. */
  def create(orderId: OrderId, amountCents: Int): F[Payment]
  def get(id: PaymentId): F[Option[Payment]]
  def delete(id: PaymentId): F[Boolean]
  def ping: F[Boolean]

  /** The single payment row for an order (TD-10.1) — used by
    * `POST /payments/authorize` to replay a prior outcome instead of placing a
    * second hold.
    */
  def findByOrderId(orderId: OrderId): F[Option[Payment]]

  /** Persists an `Authorized` payment and its `payment-events:
    * PaymentAuthorized` outbox row in one transaction (ADR 0008/0011). `None`
    * when the order already has a payment (unique conflict): nothing is written
    * and no outbox row is emitted.
    */
  def recordAuthorization(
      orderId: OrderId,
      amountCents: Int,
      paymentMethodId: PaymentMethodId,
      authorization: Authorization
  ): F[Option[Payment]]

  /** Persists a `Failed` payment with its decline reason. A decline stays
    * synchronous - no outbox row, nothing published. `None` on a unique
    * conflict, same as `recordAuthorization`.
    */
  def recordDecline(
      orderId: OrderId,
      amountCents: Int,
      paymentMethodId: PaymentMethodId,
      reason: DeclineReason
  ): F[Option[Payment]]

  /** Atomically transitions an `Authorized` payment to `Captured` and writes
    * its `PaymentCaptured` outbox row (US-19.1). The conditional update is the
    * idempotency guard: `None` when the order has no authorized payment (a
    * duplicate `DispatchConfirmed` or an unknown order) or the authorization
    * has expired, and nothing is written.
    */
  def recordCapture(orderId: OrderId): F[Option[Payment]]

  /** Atomically transitions an `Authorized` payment to `Failed`, stores the
    * reason, and writes its `PaymentFailed` outbox row (US-19.1). Same `None`
    * semantics as `recordCapture`.
    */
  def recordCaptureFailure(orderId: OrderId, reason: String): F[Option[Payment]]

  /** Atomically transitions an `Authorized` payment to `Voided` (US-18.2). A
    * void publishes nothing, so no outbox row. Same `None` semantics as
    * `recordCapture`.
    */
  def recordVoid(orderId: OrderId): F[Option[Payment]]
}

object PaymentStore {

  private val defaultStatus = PaymentStatus.Pending
  private val eventSource = "payment-service"

  /** Envelope for `PaymentAuthorized` (ADR 0009), keyed and correlated by
    * `orderId` - the same choice order-service makes for its own events. Built
    * once here for both interpreters.
    */
  private def authorizedEnvelope[F[_]: Monad: Clock: UUIDGen](
      orderId: OrderId,
      authorization: Authorization
  ): F[EventEnvelope] =
    for {
      eventId <- UUIDGen[F].randomUUID.map(_.toString)
      timestamp <- Clock[F].realTimeInstant
    } yield EventEnvelope(
      eventId = eventId,
      eventType = "PaymentAuthorized",
      timestamp = timestamp,
      source = eventSource,
      correlationId = OrderId.render(orderId),
      payload = PaymentAuthorizedEvent(
        OrderId.render(orderId),
        authorization.authId,
        authorization.expiresAt,
        timestamp
      ).asJson
    )

  private def capturedEnvelope[F[_]: Monad: Clock: UUIDGen](
      payment: Payment
  ): F[EventEnvelope] =
    for {
      eventId <- UUIDGen[F].randomUUID.map(_.toString)
      timestamp <- Clock[F].realTimeInstant
    } yield EventEnvelope(
      eventId = eventId,
      eventType = "PaymentCaptured",
      timestamp = timestamp,
      source = eventSource,
      correlationId = OrderId.render(payment.orderId),
      payload = PaymentCapturedEvent(
        OrderId.render(payment.orderId),
        payment.amountCents,
        timestamp
      ).asJson
    )

  private def captureFailedEnvelope[F[_]: Monad: Clock: UUIDGen](
      payment: Payment,
      reason: String
  ): F[EventEnvelope] =
    for {
      eventId <- UUIDGen[F].randomUUID.map(_.toString)
      timestamp <- Clock[F].realTimeInstant
    } yield EventEnvelope(
      eventId = eventId,
      eventType = "PaymentFailed",
      timestamp = timestamp,
      source = eventSource,
      correlationId = OrderId.render(payment.orderId),
      payload = PaymentFailedEvent(
        OrderId.render(payment.orderId),
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

  private val orderId: Codec[OrderId] =
    text.imap(OrderId.trusted)(OrderId.render)

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
          orderId: OrderId,
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
        def create(orderId: OrderId, amountCents: Int): F[Payment] =
          for {
            id <- Sync[F].delay(PaymentId.fromUuid(java.util.UUID.randomUUID()))
            now <- Sync[F].realTimeInstant
            entity = Payment(id, orderId, amountCents, defaultStatus, now, now)
            inserted <- ref.modify { entities =>
              if (entities.values.exists(_.orderId == orderId))
                (entities, false)
              else (entities + (id -> entity), true)
            }
            _ <- Sync[F].raiseUnless(inserted)(PaymentAlreadyExists(orderId))
          } yield entity

        def get(id: PaymentId): F[Option[Payment]] = ref.get.map(_.get(id))

        def delete(id: PaymentId): F[Boolean] =
          ref.modify { entities =>
            if (entities.contains(id)) (entities - id, true)
            else (entities, false)
          }

        def ping: F[Boolean] = Sync[F].pure(true)

        def findByOrderId(orderId: OrderId): F[Option[Payment]] =
          ref.get.map(_.values.find(_.orderId == orderId))

        def recordAuthorization(
            orderId: OrderId,
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
                _ <- outboxRef.update(
                  _ :+ (OrderId.render(orderId) -> envelope)
                )
              } yield Some(payment)
          }

        def recordDecline(
            orderId: OrderId,
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

        /** Same contract as the Postgres conditional update: only a payment
          * still in `PaymentStatus.Authorized` moves, and only to a target
          * `PaymentStatus.transition` allows.
          */
        private def transitionFromAuthorized(
            orderId: OrderId,
            status: PaymentStatus,
            reason: Option[String],
            envelope: Option[Payment => F[EventEnvelope]]
        ): F[Option[Payment]] =
          for {
            _ <- Sync[F].fromEither(
              PaymentStatus.transition(PaymentStatus.Authorized, status)
            )
            now <- Sync[F].realTimeInstant
            transitioned <- ref.modify { entities =>
              entities.values.find(_.orderId == orderId) match {
                case Some(existing)
                    if existing.status == PaymentStatus.Authorized &&
                      (status != PaymentStatus.Captured ||
                        existing.expiresAt.exists(_.isAfter(now))) =>
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
              envelope
                .traverse_(
                  _(payment)
                    .flatMap(env =>
                      outboxRef.update(_ :+ (OrderId.render(orderId) -> env))
                    )
                )
                .as(payment)
            }
          } yield result

        def recordCapture(orderId: OrderId): F[Option[Payment]] =
          transitionFromAuthorized(
            orderId,
            PaymentStatus.Captured,
            None,
            Some(capturedEnvelope[F])
          )

        def recordCaptureFailure(
            orderId: OrderId,
            reason: String
        ): F[Option[Payment]] =
          transitionFromAuthorized(
            orderId,
            PaymentStatus.Failed,
            Some(reason),
            Some(captureFailedEnvelope[F](_, reason))
          )

        def recordVoid(orderId: OrderId): F[Option[Payment]] =
          transitionFromAuthorized(orderId, PaymentStatus.Voided, None, None)
      }
      (store, outboxRef)
    }

  private type PaymentRow = (
      OrderId,
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
    orderId *: int4 *: paymentStatus *: timestamptz *: timestamptz *:
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
    (UUID, OrderId, Int, PaymentStatus),
    (OffsetDateTime, OffsetDateTime)
  ] =
    sql"""
      INSERT INTO "payment" (id, order_id, amount_cents, status)
      VALUES ($uuid, $orderId, $int4, $paymentStatus)
      ON CONFLICT (order_id) DO NOTHING
      RETURNING created_at, updated_at
    """.query(timestamptz *: timestamptz)

  private val insertAuthorizeOutcome: skunk.Query[
    (
        UUID,
        OrderId,
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
      VALUES ($uuid, $orderId, $int4, $paymentStatus, $uuid, ${text.opt},
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
    (uuid *: orderId *: int4 *: paymentStatus *: timestamptz *: timestamptz *:
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

  private val selectPaymentByOrderId: skunk.Query[OrderId, (UUID, PaymentRow)] =
    sql"""
      SELECT id, order_id, amount_cents, status, created_at, updated_at,
             payment_method_id, auth_id, expires_at, decline_reason
      FROM "payment"
      WHERE order_id = $orderId
    """.query(paymentWithIdRow)

  /** Conditional `expected -> status` transition. The `WHERE status = expected`
    * guard is the idempotency check: a duplicate `DispatchConfirmed` (or an
    * unknown order) updates nothing and returns no row, so no outbox event is
    * written. With `requireLive` an expired authorization doesn't match either
    *   - captures only.
    */
  private val transitionPayment: skunk.Query[
    (PaymentStatus, Option[String], OrderId, PaymentStatus, Boolean),
    (UUID, PaymentRow)
  ] =
    sql"""
      UPDATE "payment"
      SET status = $paymentStatus,
          decline_reason = COALESCE(${text.opt}, decline_reason),
          updated_at = now()
      WHERE order_id = $orderId AND status = $paymentStatus
        AND (NOT $bool OR expires_at > now())
      RETURNING id, order_id, amount_cents, status, created_at, updated_at,
                payment_method_id, auth_id, expires_at, decline_reason
    """.query(paymentWithIdRow)

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
                orderId: OrderId,
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
                                            OrderId.render(orderId),
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
              def create(orderId: OrderId, amountCents: Int): F[Payment] =
                for {
                  id <- Sync[F].delay(UUID.randomUUID())
                  inserted <- timed("insert") {
                    pool.use { session =>
                      session
                        .prepare(insertPayment)
                        .flatMap(
                          _.option((id, orderId, amountCents, defaultStatus))
                        )
                    }
                  }
                  timestamps <- Sync[F].fromOption(
                    inserted,
                    PaymentAlreadyExists(orderId)
                  )
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

              def findByOrderId(orderId: OrderId): F[Option[Payment]] =
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
                  orderId: OrderId,
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
                  orderId: OrderId,
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
                  orderId: OrderId,
                  status: PaymentStatus,
                  reason: Option[String],
                  envelope: Option[Payment => F[EventEnvelope]]
              ): F[Option[Payment]] =
                Sync[F].fromEither(
                  PaymentStatus.transition(PaymentStatus.Authorized, status)
                ) *> timed("update") {
                  pool.use { session =>
                    session.transaction.use { _ =>
                      session
                        .prepare(transitionPayment)
                        .flatMap(
                          _.option(
                            (
                              status,
                              reason,
                              orderId,
                              PaymentStatus.Authorized,
                              status == PaymentStatus.Captured
                            )
                          )
                        )
                        .flatMap {
                          case None            => Sync[F].pure(None)
                          case Some((id, row)) =>
                            val payment = toPayment(PaymentId.fromUuid(id), row)
                            envelope
                              .traverse_ { build =>
                                for {
                                  event <- build(payment)
                                  outboxId <- Sync[F].delay(UUID.randomUUID())
                                  _ <- session
                                    .prepare(insertOutboxEvent)
                                    .flatMap(
                                      _.execute(
                                        (
                                          outboxId,
                                          OrderId.render(orderId),
                                          event,
                                          event.correlationId
                                        )
                                      )
                                    )
                                } yield ()
                              }
                              .as(Some(payment))
                        }
                    }
                  }
                }

              def recordCapture(orderId: OrderId): F[Option[Payment]] =
                transitionFromAuthorized(
                  orderId,
                  PaymentStatus.Captured,
                  None,
                  Some(capturedEnvelope[F])
                )

              def recordCaptureFailure(
                  orderId: OrderId,
                  reason: String
              ): F[Option[Payment]] =
                transitionFromAuthorized(
                  orderId,
                  PaymentStatus.Failed,
                  Some(reason),
                  Some(captureFailedEnvelope[F](_, reason))
                )

              def recordVoid(orderId: OrderId): F[Option[Payment]] =
                transitionFromAuthorized(
                  orderId,
                  PaymentStatus.Voided,
                  None,
                  None
                )
            }
          }
      }
  }
}
