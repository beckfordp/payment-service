package paymentservice

import cats.effect.Async
import cats.syntax.semigroupk._
import org.http4s.HttpRoutes
import org.http4s.server.staticcontent.{FileService, fileService}
import purekafka.docs.{AsyncApiChannel, Docs => AsyncApiDocs}
import purerest.docs.Docs
import purerest.metrics.ServerMetrics
import purerest.tracing.ServerTracing

/** Assembles payment-service's full set of `HttpRoutes[F]` — business routes,
  * OpenAPI/Swagger docs, AsyncAPI docs (raw spec + rendered static HTML) — and
  * applies this service's full middleware stack (server tracing, server
  * metrics, CORS), in one call. Extracted out of `Main` per the
  * `Resource`-built-module composition pattern documented in `purerest`'s
  * `docs/functional-design-patterns-scala-haskell-category-theory.md` ("Scaling
  * tagless final without Cake") — `Main` should just acquire dependencies and
  * call this once, not inline the route construction itself. Mirrors
  * order-service's/inventory-service's own `AppRoutes.scala`.
  */
object AppRoutes {

  def all[F[_]: Async](
      store: PaymentStore[F],
      methods: PaymentMethodStore[F],
      gateway: PaymentGatewayClient[F],
      telemetry: Telemetry[F],
      channels: List[AsyncApiChannel[?]]
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
        PaymentMethodRoutes
          .createMethodServerEndpoint[F](methods, telemetry.logger),
        AuthorizeRoutes.authorizeServerEndpoint[F](
          store,
          methods,
          gateway,
          telemetry.logger
        ),
        HealthRoutes.healthServerEndpoint[F],
        HealthRoutes.readyServerEndpoint[F](store)
      )
    )
    val asyncApiRoutes =
      AsyncApiDocs.routes[F]("Payment Service", "1.0", channels)
    // Serves the sbt-purekafka-docs plugin's generated static HTML at
    // /docs/asyncapi/* - sibling to asyncApiRoutes' raw-spec route, same
    // split as Swagger's /docs vs /docs/docs.yaml. payment-service runs as
    // a plain `sbt run` JVM process (no docker bind mount), so this reads
    // straight from the local filesystem. Mirrors order-service's/
    // inventory-service's own AppRoutes.scala.
    val staticAsyncApiDocsDir =
      sys.env.getOrElse("ASYNCAPI_HTML_DIR", "target/asyncapi-docs")
    val staticAsyncApiRoutes = fileService[F](
      FileService.Config(
        systemPath = staticAsyncApiDocsDir,
        pathPrefix = "/docs/asyncapi"
      )
    )
    // asyncApiRoutes/staticAsyncApiRoutes must come before docsRoutes:
    // docsRoutes' Swagger UI bundle serves a wildcard /docs/{resource}
    // route for its own static assets, which returns its own real 404 (not
    // a http4s fallthrough None) for any /docs/* path it doesn't recognize
    // — including /docs/asyncapi.json — short-circuiting `<+>` before
    // asyncApiRoutes ever sees the request (found the hard way in
    // order-service's own asyncapi-docs_20261017 track).
    val tracedRoutes = ServerTracing.middleware(telemetry.tracer)(
      asyncApiRoutes <+> staticAsyncApiRoutes <+> docsRoutes
    )
    val meteredRoutes =
      ServerMetrics.middleware[F](telemetry.meter)(tracedRoutes)
    Cors.middleware[F](meteredRoutes)
  }
}
