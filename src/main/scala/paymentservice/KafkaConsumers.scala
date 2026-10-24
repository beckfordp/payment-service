package paymentservice

import cats.effect.{Async, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.kafka.ProducerSettings
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.{DeadLetter, StreamSupervisor}

/** Wires `OrderReservedConsumer` with `purekafka`'s `StreamSupervisor`
  * (restart-with-backoff) and `DeadLetter` (bounded-retry-then-DLQ) - TD-2.4,
  * ADR 0010. Single-consumer version of order-service's own
  * `KafkaConsumers.scala` (which wraps two consumers individually, then
  * merges). Extracted out of `Main.scala`'s own `val run` so this wiring is
  * directly testable, not just exercised incidentally by running the whole
  * service. `config.streamSupervisor`/`config.dlqRetry`/
  * `config.dlqPublishResilience` are all `application.conf`-driven (see
  * `PaymentServiceConfig.KafkaConfig`) rather than hardcoded here.
  */
object KafkaConsumers {

  def supervised[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
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
          OrderReservedConsumer.run[F](config, store, logger, deadLetter)
        )
      )
    } yield result
  }

  /** `supervised`, backgrounded as a fire-and-forget fiber - so `Main` wires
    * this in as one `Resource[F, Unit]` line, the same shape as every other
    * dependency, with no fs2 streaming mechanics visible there.
    */
  def resource[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      logger: StructuredLogger[F],
      meter: Meter[F]
  ): Resource[F, Unit] =
    supervised[F](config, store, logger, meter).compile.drain.background.void
}
