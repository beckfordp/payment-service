package paymentservice

import cats.effect.{Async, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.kafka.ProducerSettings
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import purekafka.consumer.{DeadLetter, EventAge, StreamSupervisor}

/** Supervises the `fulfilment-events` and `order-events` consumers
  * independently, each with its own DLQ. The first handles `DispatchFailed`
  * (US-18.2, void) and `DispatchConfirmed` (US-19.1, capture); the second
  * handles `OrderCancelled` (US-9.3, void).
  */
object KafkaConsumers {

  def supervised[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      meter: Meter[F],
      tracer: Tracer[F]
  ): Stream[F, Unit] = {
    def dlqProducerSettings: ProducerSettings[F, String, Option[String]] =
      ProducerSettings[F, String, Option[String]]
        .withBootstrapServers(config.bootstrapServers)

    for {
      eventAge <- Stream.eval(EventAge.create[F](meter))
      fulfilmentDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, Option[String]](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      orderDeadLetter <- Stream.resource(
        DeadLetter.resource[F, String, Option[String]](
          config.dlqRetry,
          config.dlqPublishResilience,
          dlqProducerSettings
        )(logger)(meter)
      )
      result <- StreamSupervisor
        .supervise[F, Unit](config.streamSupervisor)(logger)(meter)(
          Async[F].pure(
            FulfilmentEventConsumer.run[F](
              config,
              store,
              methods,
              gateway,
              logger,
              fulfilmentDeadLetter,
              eventAge,
              tracer
            )
          )
        )
        .merge(
          StreamSupervisor.supervise[F, Unit](config.streamSupervisor)(
            logger
          )(meter)(
            Async[F].pure(
              OrderCancelledConsumer.run[F](
                config,
                store,
                gateway,
                logger,
                orderDeadLetter,
                eventAge,
                tracer
              )
            )
          )
        )
    } yield result
  }

  def resource[F[_]: Async](
      config: KafkaConfig,
      store: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      logger: StructuredLogger[F],
      meter: Meter[F],
      tracer: Tracer[F]
  ): Resource[F, Unit] =
    supervised[F](
      config,
      store,
      methods,
      gateway,
      logger,
      meter,
      tracer
    ).compile.drain.background.void
}
