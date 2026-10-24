package paymentservice

import cats.effect.Sync
import pureconfig.{ConfigReader, ConfigSource}
import purekafka.consumer.StreamSupervisorConfig
import purekafka.resilience.{
  CircuitBreakerConfig => KafkaCircuitBreakerConfig,
  ResilienceConfig => KafkaResilienceConfig,
  RetryConfig => KafkaRetryConfig
}

import scala.concurrent.duration._

final case class PostgresConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) derives ConfigReader

// purekafka's own resilience/consumer config case classes don't derive
// ConfigReader themselves (the library has no PureConfig dependency) -
// instances are derived here, mirroring order-service's OrderServiceConfig.
given ConfigReader[KafkaRetryConfig] = ConfigReader.derived
given ConfigReader[KafkaCircuitBreakerConfig] = ConfigReader.derived
given ConfigReader[KafkaResilienceConfig] = ConfigReader.derived
given ConfigReader[StreamSupervisorConfig] = ConfigReader.derived

// Defaults below are for direct Scala construction only (e.g. test helpers
// that build a KafkaConfig without caring about these three fields) -
// confirmed empirically (order-service's own OrderServiceConfig) that this
// version's ConfigReader.derived does *not* fall back to them when a key is
// missing from HOCON (KeyNotFound instead), so application.conf always
// specifies all three explicitly.
final case class KafkaConfig(
    bootstrapServers: String,
    // Message-processing retry before a message is routed to the DLQ - see
    // KafkaConsumers.scala's DeadLetter usage.
    dlqRetry: KafkaRetryConfig =
      KafkaRetryConfig(maxRetries = 3, baseDelay = 200.millis),
    // Guards the DLQ's own publish call. Defaults match order-service's own
    // KafkaConfig, for consistency across services.
    dlqPublishResilience: KafkaResilienceConfig = KafkaResilienceConfig(
      retry = KafkaRetryConfig(maxRetries = 3, baseDelay = 100.millis),
      circuitBreaker = KafkaCircuitBreakerConfig(
        failureThreshold = 5,
        resetTimeout = 30.seconds
      )
    ),
    // Restart-with-backoff for a crashed consumer stream (TD-2.4, ADR 0010) -
    // see KafkaConsumers.scala's StreamSupervisor usage.
    streamSupervisor: StreamSupervisorConfig =
      StreamSupervisorConfig(baseDelay = 1.second, maxDelay = 30.seconds)
) derives ConfigReader

final case class PaymentServiceConfig(
    port: Int,
    metricsPort: Int,
    serviceName: String,
    postgres: PostgresConfig,
    kafka: KafkaConfig
) derives ConfigReader

object PaymentServiceConfig {
  def load[F[_]: Sync]: F[PaymentServiceConfig] =
    Sync[F].delay(ConfigSource.default.loadOrThrow[PaymentServiceConfig])
}
