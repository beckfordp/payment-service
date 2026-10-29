package paymentservice

import cats.effect.Sync
import cats.syntax.all._
import pureconfig.ConfigReader

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final case class DeclineReason(value: String)

final case class Authorization(authId: String, expiresAt: Instant)

/** ADR 0011's provider seam. Only `simulated` exists - a real provider (Stripe
  * or otherwise) is a future ADR's decision. `void` is used by US-18.2's
  * dispatch-failed path and `capture` by US-19.1's capture-on-dispatch; a
  * capture decline is returned as a value so it never retries through the DLQ.
  * `cardToken` is only consulted by the simulator (per-test-card capture
  * outcomes); a real provider captures by `authId` alone.
  *
  * `capture` and `void` must be idempotent by `authId` (a real provider via its
  * idempotency key): the fulfilment-events consumer retries the whole handler
  * when the store write after a gateway call fails. The simulator satisfies
  * this trivially - its outcome is a pure function of the card token.
  */
trait PaymentGatewayClient[F[_]] {
  def authorize(
      cardToken: String,
      amountCents: Int
  ): F[Either[DeclineReason, Authorization]]
  def capture(
      authId: String,
      cardToken: String
  ): F[Either[DeclineReason, Unit]]
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
      Left(
        s"Invalid gateway outcome '$other', expected approve or decline:<reason>"
      )
  }

  implicit val reader: ConfigReader[GatewayOutcome] =
    ConfigReader[String].emap(raw =>
      parse(raw).leftMap(
        pureconfig.error.CannotConvert(raw, "GatewayOutcome", _)
      )
    )
}

/** `payment-gateway.simulated` - outcome keyed by simulated card token
  * (`tok_<brand>_<last4>`), same convention as real gateways' test-card
  * sandboxes (`gluon/docs/system-design.md`, "Local payment simulation").
  * `captureOutcomes`/`defaultCaptureOutcome` are the capture-time equivalent: a
  * token can authorize successfully yet decline when captured (US-19.1).
  */
final case class SimulatedGatewayConfig(
    defaultOutcome: GatewayOutcome,
    outcomes: Map[String, GatewayOutcome],
    defaultCaptureOutcome: GatewayOutcome,
    captureOutcomes: Map[String, GatewayOutcome],
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

      def capture(
          authId: String,
          cardToken: String
      ): F[Either[DeclineReason, Unit]] =
        config.captureOutcomes
          .getOrElse(cardToken, config.defaultCaptureOutcome) match {
          case GatewayOutcome.Decline(reason) =>
            DeclineReason(reason).asLeft[Unit].pure[F]
          case GatewayOutcome.Approve =>
            ().asRight[DeclineReason].pure[F]
        }

      def void(authId: String): F[Unit] = Sync[F].unit
    }
}
