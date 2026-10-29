package paymentservice

import cats.effect.{IO, IOApp, Resource}
import com.comcast.ip4s._
import org.http4s.ember.server.EmberServerBuilder

/** Acquires every dependency payment-service needs, builds the routes, and
  * serves them — a single flat `Resource` for-comprehension, one `<-` per
  * dependency, per the composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake: bundling dependencies into `Resource`-built
  * modules"). Mirrors order-service's own `Main.scala`.
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
    methods <- PaymentMethodStore.postgres[IO](config.postgres, telemetry.meter)
    gateway = PaymentGatewayClient.simulated[IO](
      config.paymentGateway.simulated
    )
    _ <- KafkaConsumers.resource[IO](
      config.kafka,
      store,
      methods,
      gateway,
      telemetry.logger,
      telemetry.meter
    )
    routes = AppRoutes.all[IO](
      store,
      methods,
      gateway,
      telemetry,
      AsyncApiChannels.channels,
      config.asyncApiHtmlDir
    )
    _ <- EmberServerBuilder
      .default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port)
      .withHttpApp(routes.orNotFound)
      .build
  } yield ()).useForever
}
