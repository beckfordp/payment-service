package paymentservice

import cats.effect.Sync
import pureconfig.{ConfigReader, ConfigSource}

final case class PostgresConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) derives ConfigReader

final case class KafkaConfig(
    bootstrapServers: String
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
