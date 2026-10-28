package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import pureconfig.ConfigSource
import purekafka.consumer.StreamSupervisorConfig
import purekafka.resilience.{
  CircuitBreakerConfig => KafkaCircuitBreakerConfig,
  ResilienceConfig => KafkaResilienceConfig,
  RetryConfig => KafkaRetryConfig
}

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
      |  default-capture-outcome = "approve"
      |  capture-outcomes {
      |    "tok_mastercard_4444" = "decline:capture_declined"
      |  }
      |  authorization-ttl = 7d
      |}
      |kafka {
      |  bootstrap-servers = "localhost:9092"
      |  dlq-retry { max-retries = 3, base-delay = 200ms }
      |  dlq-publish-resilience {
      |    retry { max-retries = 3, base-delay = 100ms }
      |    circuit-breaker { failure-threshold = 5, reset-timeout = 30s }
      |  }
      |  stream-supervisor { base-delay = 1s, max-delay = 30s }
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
              defaultCaptureOutcome = GatewayOutcome.Approve,
              captureOutcomes = Map(
                "tok_mastercard_4444" -> GatewayOutcome.Decline(
                  "capture_declined"
                )
              ),
              authorizationTtl = 7.days
            )
          ),
          kafka = KafkaConfig(
            bootstrapServers = "localhost:9092",
            dlqRetry = KafkaRetryConfig(3, 200.millis),
            dlqPublishResilience = KafkaResilienceConfig(
              retry = KafkaRetryConfig(3, 100.millis),
              circuitBreaker = KafkaCircuitBreakerConfig(5, 30.seconds)
            ),
            streamSupervisor = StreamSupervisorConfig(1.second, 30.seconds)
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

  test("fails to load when a capture outcome string is invalid") {
    val badOutcome =
      validHocon + "payment-gateway.simulated.capture-outcomes.\"tok_visa_4242\" = \"maybe\"\n"
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
      assertEquals(config.kafka.bootstrapServers, "localhost:9092")
      assertEquals(
        config.paymentGateway.simulated,
        SimulatedGatewayConfig(
          defaultOutcome = GatewayOutcome.Approve,
          outcomes = Map(
            "tok_visa_0002" -> GatewayOutcome.Decline("insufficient_funds")
          ),
          defaultCaptureOutcome = GatewayOutcome.Approve,
          captureOutcomes = Map(
            "tok_mastercard_4444" -> GatewayOutcome.Decline("capture_declined")
          ),
          authorizationTtl = 7.days
        )
      )
    }
  }
}
