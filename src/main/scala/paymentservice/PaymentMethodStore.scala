package paymentservice

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.std.Console
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.otel4s.metrics.Meter
import skunk.{Codec, Session}
import skunk.circe.codec.all.jsonb
import skunk.codec.all._
import skunk.implicits._

import java.time.{Instant, OffsetDateTime}
import java.util.UUID

/** A tokenized card plus its billing address (US-13.1/US-12.1). `cardToken`
  * stays server-side - only `id` is handed back to gshop, and order-service
  * passes that id to `POST /payments/authorize`.
  */
final case class PaymentMethod(
    id: String,
    customerId: String,
    cardToken: String,
    last4: String,
    brand: CardBrand,
    expiry: String,
    billingAddress: Address,
    createdAt: Instant
)

trait PaymentMethodStore[F[_]] {
  def create(
      customerId: String,
      card: Card,
      billingAddress: Address
  ): F[PaymentMethod]
  def get(id: String): F[Option[PaymentMethod]]
}

object PaymentMethodStore {

  def inMemory[F[_]: Sync]: F[PaymentMethodStore[F]] =
    Ref.of[F, Map[String, PaymentMethod]](Map.empty).map { ref =>
      new PaymentMethodStore[F] {
        def create(
            customerId: String,
            card: Card,
            billingAddress: Address
        ): F[PaymentMethod] =
          for {
            id <- Sync[F].delay(UUID.randomUUID().toString)
            now <- Sync[F].realTimeInstant
            method = fromCard(id, customerId, card, billingAddress, now)
            _ <- ref.update(_ + (id -> method))
          } yield method

        def get(id: String): F[Option[PaymentMethod]] = ref.get.map(_.get(id))
      }
    }

  private def fromCard(
      id: String,
      customerId: String,
      card: Card,
      billingAddress: Address,
      createdAt: Instant
  ): PaymentMethod =
    PaymentMethod(
      id,
      customerId,
      card.token,
      card.last4,
      card.brand,
      card.expiry,
      billingAddress,
      createdAt
    )

  private val cardBrand: Codec[CardBrand] =
    text.eimap(CardBrand.fromString)(_.asString)

  private val insertMethod: skunk.Query[
    (UUID, String, String, String, CardBrand, String, Address),
    OffsetDateTime
  ] =
    sql"""
      INSERT INTO "payment_methods"
        (id, customer_id, card_token, last4, brand, expiry, billing_address)
      VALUES ($uuid, $text, $text, $text, $cardBrand, $text, ${jsonb[Address]})
      RETURNING created_at
    """.query(timestamptz)

  private val selectMethod: skunk.Query[
    UUID,
    (String, String, String, CardBrand, String, Address, OffsetDateTime)
  ] =
    sql"""
      SELECT customer_id, card_token, last4, brand, expiry, billing_address,
             created_at
      FROM "payment_methods"
      WHERE id = $uuid
    """.query(
      text *: text *: text *: cardBrand *: text *: jsonb[Address] *: timestamptz
    )

  def postgres[F[_]: Async: Console: Network](
      config: PostgresConfig,
      meter: Meter[F]
  ): Resource[F, PaymentMethodStore[F]] = {
    import org.typelevel.otel4s.trace.Tracer.Implicits.noop
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .pooled(max = 5)
      .evalMap { pool =>
        DbTimer.create(meter).map { timer =>
          new PaymentMethodStore[F] {
            def create(
                customerId: String,
                card: Card,
                billingAddress: Address
            ): F[PaymentMethod] =
              for {
                id <- Sync[F].delay(UUID.randomUUID())
                createdAt <- timer.timed("insert") {
                  pool.use { session =>
                    session
                      .prepare(insertMethod)
                      .flatMap(
                        _.unique(
                          (
                            id,
                            customerId,
                            card.token,
                            card.last4,
                            card.brand,
                            card.expiry,
                            billingAddress
                          )
                        )
                      )
                  }
                }
              } yield fromCard(
                id.toString,
                customerId,
                card,
                billingAddress,
                createdAt.toInstant
              )

            def get(id: String): F[Option[PaymentMethod]] =
              scala.util.Try(UUID.fromString(id)).toOption match {
                case None       => Sync[F].pure(None)
                case Some(uuid) =>
                  timer
                    .timed("select") {
                      pool.use(_.prepare(selectMethod).flatMap(_.option(uuid)))
                    }
                    .map(_.map {
                      case (
                            customerId,
                            cardToken,
                            last4,
                            brand,
                            expiry,
                            billingAddress,
                            createdAt
                          ) =>
                        PaymentMethod(
                          id,
                          customerId,
                          cardToken,
                          last4,
                          brand,
                          expiry,
                          billingAddress,
                          createdAt.toInstant
                        )
                    })
              }
          }
        }
      }
  }
}
