package paymentservice

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.{Span, Tracer}
import paymentservice.domain.{OrderId, PaymentMethodId}

/** Payment-journey business spans (gluon PERF-5.9), as decorators: the
  * `PaymentStore` steps `PaymentJourneyMetrics` wraps (each one Postgres
  * transaction, so its span is that step's DB transaction) and the gateway
  * calls. Wired outermost, so milestone log lines and the `outbox.write` span
  * nest inside. A raised error marks the span Error (otel4s's default
  * finalization).
  *
  *   - `payment.record_*{order.id}`: `outcome=no_match` when the store's
  *     conditional update matched nothing
  *   - `payment.expire{payment.expired}`: one sweep batch
  *   - `payment.gateway.authorize|capture|void{outcome}`
  */
object PaymentStoreTracing {

  def instrument[F[_]: MonadCancelThrow](
      store: PaymentStore[F],
      tracer: Tracer[F]
  ): PaymentStore[F] =
    new PaymentStore[F] {
      export store.{
        recordAuthorization => _,
        recordDecline => _,
        recordCapture => _,
        recordCaptureFailure => _,
        recordVoid => _,
        expireDueAuthorizations => _,
        *
      }

      private def step(name: String, orderId: OrderId)(
          fa: F[Option[Payment]]
      ): F[Option[Payment]] =
        tracer
          .span(name, Attribute("order.id", OrderId.render(orderId)))
          .use(span =>
            fa.flatTap(result =>
              span
                .addAttribute(Attribute("outcome", "no_match"))
                .whenA(result.isEmpty)
            )
          )

      def recordAuthorization(
          orderId: OrderId,
          amountCents: Int,
          paymentMethodId: PaymentMethodId,
          authorization: Authorization
      ): F[Option[Payment]] =
        step("payment.record_authorization", orderId)(
          store.recordAuthorization(
            orderId,
            amountCents,
            paymentMethodId,
            authorization
          )
        )

      def recordDecline(
          orderId: OrderId,
          amountCents: Int,
          paymentMethodId: PaymentMethodId,
          reason: DeclineReason
      ): F[Option[Payment]] =
        step("payment.record_decline", orderId)(
          store.recordDecline(orderId, amountCents, paymentMethodId, reason)
        )

      def recordCapture(orderId: OrderId): F[Option[Payment]] =
        step("payment.record_capture", orderId)(store.recordCapture(orderId))

      def recordCaptureFailure(
          orderId: OrderId,
          reason: String
      ): F[Option[Payment]] =
        step("payment.record_capture_failure", orderId)(
          store.recordCaptureFailure(orderId, reason)
        )

      def recordVoid(orderId: OrderId): F[Option[Payment]] =
        step("payment.record_void", orderId)(store.recordVoid(orderId))

      def expireDueAuthorizations(limit: Int): F[List[Payment]] =
        tracer
          .span("payment.expire")
          .use(span =>
            store
              .expireDueAuthorizations(limit)
              .flatTap(expired =>
                span.addAttribute(
                  Attribute("payment.expired", expired.size.toLong)
                )
              )
          )
    }
}

object PaymentGatewayTracing {

  def instrument[F[_]: MonadCancelThrow](
      gateway: PaymentGatewayClient[F],
      tracer: Tracer[F]
  ): PaymentGatewayClient[F] =
    new PaymentGatewayClient[F] {
      private def outcome[A](span: Span[F])(
          result: Either[DeclineReason, A]
      ): F[Unit] =
        span.addAttribute(
          Attribute("outcome", result.fold(_ => "declined", _ => "approved"))
        )

      def authorize(
          cardToken: String,
          amountCents: Int
      ): F[Either[DeclineReason, Authorization]] =
        tracer
          .span("payment.gateway.authorize")
          .use(span =>
            gateway.authorize(cardToken, amountCents).flatTap(outcome(span))
          )

      def capture(
          authId: String,
          cardToken: String
      ): F[Either[DeclineReason, Unit]] =
        tracer
          .span("payment.gateway.capture")
          .use(span =>
            gateway.capture(authId, cardToken).flatTap(outcome(span))
          )

      def void(authId: String): F[Unit] =
        tracer.span("payment.gateway.void").surround(gateway.void(authId))
    }
}
