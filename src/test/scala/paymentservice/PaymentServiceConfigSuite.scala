package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
import pureconfig.ConfigSource

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
      |kafka {
      |  bootstrap-servers = "localhost:9092"
      |  dlq-retry {
      |    max-retries = 3
      |    base-delay = 200ms
      |  }
      |  dlq-publish-resilience {
      |    retry {
      |      max-retries = 3
      |      base-delay = 100ms
      |    }
      |    circuit-breaker {
      |      failure-threshold = 5
      |      reset-timeout = 30s
      |    }
      |  }
      |  stream-supervisor {
      |    base-delay = 1s
      |    max-delay = 30s
      |  }
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
          kafka = KafkaConfig(bootstrapServers = "localhost:9092")
        )
      )
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
        config.kafka,
        KafkaConfig(bootstrapServers = "localhost:9092")
      )
    }
  }
}
