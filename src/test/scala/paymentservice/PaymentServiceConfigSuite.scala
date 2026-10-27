package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import pureconfig.ConfigSource

import scala.concurrent.duration._

class PaymentServiceConfigSuite extends CatsEffectSuite {

  private val validHocon =
    """
      |port = 8080
      |metrics-port = 9090
      |service-name = "payment-service"
      |postgres {
      |  host = "localhost"
      |  port = 5432
      |  database = "payment"
      |  user = "payment"
      |  password = "payment"
      |}
      |payment-gateway.simulated {
      |  default-outcome = "approve"
      |  outcomes {
      |    "tok_visa_0002" = "decline:insufficient_funds"
      |  }
      |  authorization-ttl = 7d
      |}
      |""".stripMargin

  test("loads a fully-specified config") {
    val result =
      ConfigSource.string(validHocon).load[PaymentServiceConfig]
    assertEquals(
      result,
      Right(
        PaymentServiceConfig(
          port = 8080,
          metricsPort = 9090,
          serviceName = "payment-service",
          postgres = PostgresConfig(
            host = "localhost",
            port = 5432,
            database = "payment",
            user = "payment",
            password = "payment"
          ),
          paymentGateway = PaymentGatewayConfig(
            SimulatedGatewayConfig(
              defaultOutcome = GatewayOutcome.Approve,
              outcomes = Map(
                "tok_visa_0002" -> GatewayOutcome.Decline("insufficient_funds")
              ),
              authorizationTtl = 7.days
            )
          )
        )
      )
    )
  }

  test("fails to load when a gateway outcome string is invalid") {
    val badOutcome =
      validHocon + "payment-gateway.simulated.outcomes.\"tok_visa_4242\" = \"maybe\"\n"
    assert(
      ConfigSource.string(badOutcome).load[PaymentServiceConfig].isLeft
    )
  }

  test("fails to load when a required field is missing") {
    val missingPassword =
      """
        |port = 8080
        |metrics-port = 9090
        |postgres {
        |  host = "localhost"
        |  port = 5432
        |  database = "payment"
        |  user = "payment"
        |}
        |""".stripMargin

    assert(
      ConfigSource
        .string(missingPassword)
        .load[PaymentServiceConfig]
        .isLeft
    )
  }

  test("load[F] reads the shipped application.conf defaults") {
    PaymentServiceConfig.load[IO].map { config =>
      assertEquals(config.port, 8080)
      assertEquals(config.metricsPort, 9090)
      assertEquals(config.serviceName, "payment-service")
      assertEquals(
        config.postgres,
        PostgresConfig(
          "localhost",
          5432,
          "payment",
          "payment",
          "payment"
        )
      )
      assertEquals(
        config.paymentGateway.simulated,
        SimulatedGatewayConfig(
          defaultOutcome = GatewayOutcome.Approve,
          outcomes = Map(
            "tok_visa_0002" -> GatewayOutcome.Decline("insufficient_funds")
          ),
          authorizationTtl = 7.days
        )
      )
    }
  }
}
