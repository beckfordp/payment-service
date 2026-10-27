package paymentservice

import cats.effect.Sync
import cats.syntax.all._
import pureconfig.ConfigReader

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final case class DeclineReason(value: String)

final case class Authorization(authId: String, expiresAt: Instant)

/** ADR 0011's provider seam. Only `simulated` exists - a real provider
  * (Stripe or otherwise) is a future ADR's decision. `capture`/`void` have no
  * callers until Checkout Journeys 3/4.
  */
trait PaymentGatewayClient[F[_]] {
  def authorize(
      cardToken: String,
      amountCents: Int
  ): F[Either[DeclineReason, Authorization]]
  def capture(authId: String): F[Unit]
  def void(authId: String): F[Unit]
}

sealed trait GatewayOutcome

object GatewayOutcome {
  case object Approve extends GatewayOutcome
  final case class Decline(reason: String) extends GatewayOutcome

  private val declinePattern = "^decline:(.+)$".r

  def parse(raw: String): Either[String, GatewayOutcome] = raw match {
    case "approve"              => Right(Approve)
    case declinePattern(reason) => Right(Decline(reason))
    case other                  =>
      Left(s"Invalid gateway outcome '$other', expected approve or decline:<reason>")
  }

  implicit val reader: ConfigReader[GatewayOutcome] =
    ConfigReader[String].emap(raw =>
      parse(raw).leftMap(pureconfig.error.CannotConvert(raw, "GatewayOutcome", _))
    )
}

/** `payment-gateway.simulated` - outcome keyed by simulated card token
  * (`tok_<brand>_<last4>`), same convention as real gateways' test-card
  * sandboxes (`gluon/docs/system-design.md`, "Local payment simulation").
  */
final case class SimulatedGatewayConfig(
    defaultOutcome: GatewayOutcome,
    outcomes: Map[String, GatewayOutcome],
    authorizationTtl: FiniteDuration
) derives ConfigReader

final case class PaymentGatewayConfig(simulated: SimulatedGatewayConfig)
    derives ConfigReader

object PaymentGatewayClient {

  def simulated[F[_]: Sync](
      config: SimulatedGatewayConfig
  ): PaymentGatewayClient[F] =
    new PaymentGatewayClient[F] {
      def authorize(
          cardToken: String,
          amountCents: Int
      ): F[Either[DeclineReason, Authorization]] =
        config.outcomes.getOrElse(cardToken, config.defaultOutcome) match {
          case GatewayOutcome.Decline(reason) =>
            DeclineReason(reason).asLeft[Authorization].pure[F]
          case GatewayOutcome.Approve =>
            for {
              id <- Sync[F].delay(UUID.randomUUID().toString)
              now <- Sync[F].realTimeInstant
            } yield Authorization(
              s"auth_$id",
              now.plusNanos(config.authorizationTtl.toNanos)
            ).asRight[DeclineReason]
        }

      def capture(authId: String): F[Unit] = Sync[F].unit

      def void(authId: String): F[Unit] = Sync[F].unit
    }
}
