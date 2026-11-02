package paymentservice

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Dev/demo operator gate for the reconciliation resolve action (ADM-4, ADR
  * 0024/0026). Unlike order-service's cancel path there is no customer variant:
  * a missing `Authorization` header is rejected too.
  */
final case class OperatorAuth(token: Option[String], operatorId: String) {

  def requireOperator(
      authorization: Option[String]
  ): Either[OperatorUnauthorized.type, String] =
    authorization match {
      case Some(raw) =>
        token match {
          case Some(expected) if bearerMatches(raw, expected) =>
            Right(operatorId)
          case _ => Left(OperatorUnauthorized)
        }
      case None => Left(OperatorUnauthorized)
    }

  private def bearerMatches(raw: String, expected: String): Boolean = {
    val scheme = "bearer "
    if (raw.length <= scheme.length) false
    else if (!raw.substring(0, scheme.length).equalsIgnoreCase(scheme)) false
    else {
      val presented = raw.substring(scheme.length).trim
      MessageDigest.isEqual(
        presented.getBytes(StandardCharsets.UTF_8),
        expected.getBytes(StandardCharsets.UTF_8)
      )
    }
  }
}

object OperatorAuth {
  val disabled: OperatorAuth = OperatorAuth(None, "dev-operator")

  def fromConfig(config: OperatorAuthConfig): OperatorAuth =
    OperatorAuth(
      config.token.map(_.trim).filter(_.nonEmpty),
      config.operatorId.trim
    )
}

case object OperatorUnauthorized
