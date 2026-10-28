package paymentservice

import cats.effect.Sync
import pureconfig.{ConfigReader, ConfigSource}

import scala.concurrent.duration._
import purekafka.consumer.StreamSupervisorConfig
import purekafka.resilience.{
  CircuitBreakerConfig => KafkaCircuitBreakerConfig,
  ResilienceConfig => KafkaResilienceConfig,
  RetryConfig => KafkaRetryConfig
}

final case class PostgresConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) derives ConfigReader

given ConfigReader[KafkaCircuitBreakerConfig] = ConfigReader.derived
given ConfigReader[KafkaResilienceConfig] = ConfigReader.derived
given ConfigReader[StreamSupervisorConfig] = ConfigReader.derived

final case class KafkaConfig(
    bootstrapServers: String,
    dlqRetry: KafkaRetryConfig =
      KafkaRetryConfig(maxRetries = 3, baseDelay = 200.millis),
    dlqPublishResilience: KafkaResilienceConfig = KafkaResilienceConfig(
      retry = KafkaRetryConfig(maxRetries = 3, baseDelay = 100.millis),
      circuitBreaker = KafkaCircuitBreakerConfig(
        failureThreshold = 5,
        resetTimeout = 30.seconds
      )
    ),
    streamSupervisor: StreamSupervisorConfig =
      StreamSupervisorConfig(baseDelay = 1.second, maxDelay = 30.seconds)
) derives ConfigReader

final case class PaymentServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    postgres: PostgresConfig,
    paymentGateway: PaymentGatewayConfig,
    kafka: KafkaConfig
) derives ConfigReader

object PaymentServiceConfig {
  def load[F[_]: Sync]: F[PaymentServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[PaymentServiceConfig])
}
