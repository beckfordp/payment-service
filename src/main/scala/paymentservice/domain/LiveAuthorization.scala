package paymentservice.domain

import java.time.Instant

/** "Capture needs a live authorization", defined once (TD-16.1). A hold that
  * has passed `expiresAt` can no longer move money, so only a capture checks it -
  * a void or a capture failure releases or records the hold regardless. The
  * fulfilment-events pre-check, the authorize replay and the in-memory store
  * call this directly; the Postgres conditional update takes [[required]] as
  * its parameter and compares `expires_at > now()` atomically.
  */
object LiveAuthorization {

  def required(target: PaymentStatus): Boolean = target match {
    case PaymentStatus.Captured => true
    case PaymentStatus.Pending | PaymentStatus.Authorized |
        PaymentStatus.Failed | PaymentStatus.Voided =>
      false
  }

  def isLive(expiresAt: Option[Instant], now: Instant): Boolean =
    expiresAt.exists(_.isAfter(now))

  /** The sweep's own predicate: a hold whose `expiresAt` is now or earlier. */
  def isExpired(expiresAt: Option[Instant], now: Instant): Boolean =
    expiresAt.exists(!_.isAfter(now))

  /** Whether moving an authorized payment to `target` is allowed at `now`. */
  def permits(
      target: PaymentStatus,
      expiresAt: Option[Instant],
      now: Instant
  ): Boolean =
    !required(target) || isLive(expiresAt, now)
}
