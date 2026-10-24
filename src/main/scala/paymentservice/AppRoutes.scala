package paymentservice

import cats.effect.Async
import org.http4s.HttpRoutes
import purerest.docs.Docs
import purerest.metrics.ServerMetrics
import purerest.tracing.ServerTracing

/** Assembles payment-service's full set of `HttpRoutes[F]` — business routes,
  * OpenAPI/Swagger docs — and applies this service's full middleware stack
  * (server tracing, server metrics, CORS), in one call. Extracted out of `Main`
  * per the `Resource`-built-module composition pattern documented in
  * `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake") — `Main` should just acquire dependencies and
  * call this once, not inline the route construction itself. Mirrors
  * order-service's own `AppRoutes.scala`.
  */
object AppRoutes {

  def all[F[_]: Async](
      store: PaymentStore[F],
      telemetry: Telemetry[F]
  ): HttpRoutes[F] = {
    val docsRoutes = Docs.routes[F](
      "Payment Service",
      "1.0",
      List(
        PaymentRoutes.serverEndpoint[F](store, telemetry.logger),
        PaymentRoutes.getPaymentServerEndpoint[F](store, telemetry.logger),
        PaymentRoutes
          .updatePaymentServerEndpoint[F](store, telemetry.logger),
        PaymentRoutes
          .replacePaymentServerEndpoint[F](store, telemetry.logger),
        PaymentRoutes
          .deletePaymentServerEndpoint[F](store, telemetry.logger),
        HealthRoutes.healthServerEndpoint[F],
        HealthRoutes.readyServerEndpoint[F](store)
      )
    )
    val tracedRoutes = ServerTracing.middleware(telemetry.tracer)(docsRoutes)
    val meteredRoutes =
      ServerMetrics.middleware[F](telemetry.meter)(tracedRoutes)
    Cors.middleware[F](meteredRoutes)
  }
}
