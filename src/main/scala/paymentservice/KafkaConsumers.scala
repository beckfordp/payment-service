package paymentservice

import cats.effect.{Async, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.kafka.ProducerSettings
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.{DeadLetter, StreamSupervisor}

/** Supervises the `DispatchFailed` consumer (US-18.2) independently, with its
  * own DLQ (`fulfilment-events-dlq`).
  */
object KafkaConsumers {

  def supervised[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Stream[F, Unit] = {
    def dlqProducerSettings: ProducerSettings[F, String, String] =
      ProducerSettings[F, String, String]
        .withBootstrapServers(config.bootstrapServers)

    for {
      deadLetter <- Stream.resource(
        DeadLetter.resource[F, String, String](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      result <- StreamSupervisor.supervise[F, Unit](config.streamSupervisor)(
        logger
      )(meter)(
        Async[F].pure(
          FulfilmentEventConsumer.run[F](
            config,
            store,
            gateway,
            logger,
            deadLetter
          )
        )
      )
    } yield result
  }

  def resource[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Resource[F, Unit] =
    supervised[F](
      config,
      store,
      gateway,
      logger,
      meter
    ).compile.drain.background.void
}
