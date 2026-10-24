package paymentservice

import cats.effect.{IO, IOApp, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import com.comcast.ip4s._
import org.http4s.ember.server.EmberServerBuilder

/** Acquires every dependency payment-service needs, builds the routes, and
  * serves them — a single flat `Resource` for-comprehension, one `<-` per
  * dependency, per the composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake: bundling dependencies into `Resource`-built
  * modules"). Mirrors order-service's own `Main.scala`. `OrderReservedConsumer`
  * is backgrounded inline here for now - extracted into `KafkaConsumers` once
  * it adopts `StreamSupervisor`/`DeadLetter` (TD-2.4).
  */
object Main extends IOApp.Simple {

  val run: IO[Unit] = (for {
    config <- Resource.eval(PaymentServiceConfig.load[IO])
    port <- Resource.eval(
      IO.fromOption(Port.fromInt(config.port))(
        new IllegalArgumentException(
          s"Invalid payment-service port: ${config.port}"
        )
      )
    )
    _ <- Resource.eval(Migrations.run[IO](config.postgres))
    telemetry <- Telemetry.resource[IO](config.serviceName, config.metricsPort)
    _ <- Resource.eval(
      telemetry.logger.info(
        Map(
          "port" -> config.port.toString,
          "metrics_port" -> config.metricsPort.toString
        )
      )("payment-service starting")
    )
    store <- PaymentStore.postgres[IO](config.postgres, telemetry.meter)
    _ <- OrderReservedConsumer
      .run[IO](config.kafka, store, telemetry.logger)
      .compile
      .drain
      .background
      .void
    routes = AppRoutes.all[IO](store, telemetry)
    _ <- EmberServerBuilder
      .default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port)
      .withHttpApp(routes.orNotFound)
      .build
  } yield ()).useForever
}
