package paymentservice

import cats.effect.{IO, IOApp}
import cats.effect.syntax.all._
import com.comcast.ip4s._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import purerest.docs.Docs
import purerest.logging.Logging
import purerest.metrics.{Metrics, ServerMetrics}
import purerest.tracing.{ServerTracing, Tracing}

object Main extends IOApp.Simple {

  val run: IO[Unit] =
    for {
      config <- PaymentServiceConfig.load[IO]
      port <- IO.fromOption(Port.fromInt(config.port))(
        new IllegalArgumentException(
          s"Invalid payment-service port: ${config.port}"
        )
      )
      _ <- Migrations.run[IO](config.postgres)
      _ <- Tracing.console[IO](config.serviceName).use { tracer =>
        Metrics.oteljava[IO](config.serviceName, config.metricsPort).use {
          meter =>
            for {
              logger <- Logging.create[IO](tracer, config.serviceName)
              _ <- logger.info(
                Map(
                  "port" -> config.port.toString,
                  "metrics_port" -> config.metricsPort.toString
                )
              )("payment-service starting")
              _ <- PaymentStore.postgres[IO](config.postgres, meter).use {
                store =>
                  PaymentEventPublisher.resource[IO](config.kafka, logger).use {
                    publisher =>
                      OrderReservedConsumer
                        .run[IO](config.kafka, store, publisher, logger)
                        .compile
                        .drain
                        .background
                        .use { _ =>
                          val docsRoutes = Docs.routes[IO](
                            "Payment Service",
                            "1.0",
                            List(
                              PaymentRoutes.serverEndpoint[IO](store, logger),
                              PaymentRoutes
                                .getPaymentServerEndpoint[IO](store, logger),
                              PaymentRoutes
                                .updatePaymentServerEndpoint[IO](store, logger),
                              PaymentRoutes
                                .replacePaymentServerEndpoint[IO](
                                  store,
                                  logger
                                ),
                              PaymentRoutes
                                .deletePaymentServerEndpoint[IO](
                                  store,
                                  logger
                                ),
                              HealthRoutes.healthServerEndpoint[IO],
                              HealthRoutes.readyServerEndpoint[IO](store)
                            )
                          )
                          val tracedRoutes =
                            ServerTracing.middleware(tracer)(docsRoutes)
                          val routes =
                            ServerMetrics.middleware[IO](meter)(tracedRoutes)
                          EmberServerBuilder
                            .default[IO]
                            .withHost(host"0.0.0.0")
                            .withPort(port)
                            .withHttpApp(routes.orNotFound)
                            .build
                            .useForever
                        }
                  }
              }
            } yield ()
        }
      }
    } yield ()
}
