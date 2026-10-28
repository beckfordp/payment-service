package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite

import scala.concurrent.duration._

class PaymentGatewayClientSuite extends CatsEffectSuite {

  private val config = SimulatedGatewayConfig(
    defaultOutcome = GatewayOutcome.Approve,
    outcomes = Map(
      "tok_visa_0002" -> GatewayOutcome.Decline("insufficient_funds"),
      "tok_visa_4242" -> GatewayOutcome.Approve
    ),
    defaultCaptureOutcome = GatewayOutcome.Approve,
    captureOutcomes = Map(
      "tok_mastercard_4444" -> GatewayOutcome.Decline("capture_declined"),
      "tok_visa_4242" -> GatewayOutcome.Approve
    ),
    authorizationTtl = 7.days
  )

  private val gateway = PaymentGatewayClient.simulated[IO](config)

  test("authorize approves a token mapped to approve, expiring after the TTL") {
    for {
      before <- IO.realTimeInstant
      result <- gateway.authorize("tok_visa_4242", 1000)
      after <- IO.realTimeInstant
    } yield result match {
      case Right(auth) =>
        assert(auth.authId.startsWith("auth_"), auth.authId)
        assert(!auth.expiresAt.isBefore(before.plusSeconds(7 * 24 * 3600)))
        assert(!auth.expiresAt.isAfter(after.plusSeconds(7 * 24 * 3600)))
      case Left(reason) => fail(s"expected approval, got $reason")
    }
  }

  test("authorize declines a token mapped to decline:<reason>") {
    gateway
      .authorize("tok_visa_0002", 1000)
      .assertEquals(Left(DeclineReason("insufficient_funds")))
  }

  test("authorize falls back to default-outcome for an unlisted token") {
    val declineByDefault = PaymentGatewayClient.simulated[IO](
      config.copy(defaultOutcome = GatewayOutcome.Decline("do_not_honor"))
    )
    for {
      unlisted <- declineByDefault.authorize("tok_amex_0005", 1000)
      listed <- declineByDefault.authorize("tok_visa_4242", 1000)
    } yield {
      assertEquals(unlisted, Left(DeclineReason("do_not_honor")))
      assert(listed.isRight)
    }
  }

  test("authorize issues a fresh authId per call") {
    for {
      a <- gateway.authorize("tok_visa_4242", 1000)
      b <- gateway.authorize("tok_visa_4242", 1000)
    } yield assertNotEquals(a.map(_.authId), b.map(_.authId))
  }

  test("void always succeeds in the simulator") {
    gateway.void("auth_x")
  }

  test("capture approves a token with no capture decline configured") {
    gateway.capture("auth_x", "tok_visa_4242").assertEquals(Right(()))
  }

  test("capture declines a token mapped to decline:<reason>") {
    gateway
      .capture("auth_x", "tok_mastercard_4444")
      .assertEquals(Left(DeclineReason("capture_declined")))
  }

  test("capture falls back to default-capture-outcome for an unlisted token") {
    val declineByDefault = PaymentGatewayClient.simulated[IO](
      config.copy(
        defaultCaptureOutcome = GatewayOutcome.Decline("do_not_honor")
      )
    )
    for {
      unlisted <- declineByDefault.capture("auth_x", "tok_amex_0005")
      listed <- declineByDefault.capture("auth_x", "tok_visa_4242")
    } yield {
      assertEquals(unlisted, Left(DeclineReason("do_not_honor")))
      assertEquals(listed, Right(()))
    }
  }

  test("GatewayOutcome.parse reads approve and decline:<reason>") {
    assertEquals(GatewayOutcome.parse("approve"), Right(GatewayOutcome.Approve))
    assertEquals(
      GatewayOutcome.parse("decline:card_expired"),
      Right(GatewayOutcome.Decline("card_expired"))
    )
  }

  test("GatewayOutcome.parse rejects anything else") {
    List("approved", "decline", "decline:", "").foreach { raw =>
      assert(GatewayOutcome.parse(raw).isLeft, raw)
    }
  }
}
