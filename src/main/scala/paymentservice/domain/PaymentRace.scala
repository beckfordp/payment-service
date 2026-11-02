package paymentservice.domain

import cats.MonadThrow
import cats.syntax.all._
import org.typelevel.log4cats.StructuredLogger
import paymentservice.{PaymentStore, UnrecordedTransition}

/** Classification of a payment whose conditional transition returned `None`
  * (US-21.2/21.4). The conditional update is the arbiter; this decides whether
  * the caller saw a duplicate, a benign late event, a genuine race loser that
  * needs a reconciliation flag, or an unexpected state.
  */
object PaymentRace {

  sealed trait Outcome

  object Outcome {
    case object Duplicate extends Outcome
    case object Benign extends Outcome
    final case class Lost(reason: String) extends Outcome
    case object Unexpected extends Outcome
  }

  import Outcome._

  def classify(
      target: PaymentStatus,
      current: PaymentStatus
  ): Outcome =
    if (target == current) Duplicate
    else
      (target, current) match {
        case (PaymentStatus.Voided, PaymentStatus.Captured) =>
          Lost("void_after_capture")
        case (PaymentStatus.Captured, PaymentStatus.Voided) =>
          Lost("capture_after_void")
        case (PaymentStatus.Captured, PaymentStatus.Failed) =>
          Lost("capture_after_failed")
        case (PaymentStatus.Failed, PaymentStatus.Captured) =>
          Lost("capture_failure_after_capture")
        // No money moved from these attempts; a late cancel/dispatch after a
        // failed authorization is benign.
        case (PaymentStatus.Failed, PaymentStatus.Voided) => Benign
        case (PaymentStatus.Voided, PaymentStatus.Failed) => Benign
        case _                                            => Unexpected
      }

  /** Re-reads the payment after a `None` transition and records/commits the
    * outcome. Only an unexpected state raises `UnrecordedTransition` (DLQ).
    */
  def afterNone[F[_]: MonadThrow](
      store: PaymentStore[F],
      logger: StructuredLogger[F],
      orderId: OrderId,
      target: PaymentStatus,
      eventLabel: String
  ): F[Unit] =
    store.findByOrderId(orderId).flatMap {
      case None =>
        MonadThrow[F].raiseError(UnrecordedTransition(orderId, target))
      case Some(payment) =>
        val context = Map(
          "order_id" -> OrderId.render(orderId),
          "event" -> eventLabel,
          "current_status" -> payment.status.asString,
          "target" -> target.asString
        )
        classify(target, payment.status) match {
          case Duplicate =>
            logger.info(context)(
              "Payment already in the requested state - duplicate"
            )
          case Benign =>
            logger.info(context)(
              "Late event for a terminal non-capture state - ignoring"
            )
          case Lost(reason) =>
            logger.warn(context + ("reconciliation" -> reason))(
              "Payment transition lost - reconciliation flagged"
            ) *> store.flagReconciliation(orderId, reason).void
          case Unexpected =>
            MonadThrow[F].raiseError(UnrecordedTransition(orderId, target))
        }
    }
}
