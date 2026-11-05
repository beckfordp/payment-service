package paymentservice

import cats.effect.IO
import cats.syntax.all._
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.trace.data.SpanData
import munit.CatsEffectSuite
import paymentservice.TestConversions.given
import paymentservice.domain.{OrderId, PaymentMethodId}
import purerest.tracing.Tracing

import java.time.Instant
import java.util.UUID
import scala.language.implicitConversions

class PaymentTracingSuite extends CatsEffectSuite {

  private val methodId = PaymentMethodId.fromUuid(UUID.randomUUID())
  private val authorization =
    Authorization("auth_1", Instant.parse("2099-11-03T12:00:00Z"))

  private def attr(span: SpanData, key: String): Option[String] =
    Option(span.getAttributes.get(AttributeKey.stringKey(key)))

  test(
    "store steps run in business spans under the caller's span, with order.id and no_match outcome"
  ) {
    Tracing.test[IO]("payment-store-tracing").use { tt =>
      for {
        underlying <- PaymentStore.inMemory[IO]
        store = PaymentStoreTracing.instrument[IO](underlying, tt.tracer)
        parentId <- tt.tracer.span("request").use { span =>
          store.recordAuthorization("o-1", 100, methodId, authorization) *>
            store.recordDecline("o-2", 100, methodId, DeclineReason("card")) *>
            store.recordCapture("o-1") *>
            store.recordCaptureFailure("o-1", "gateway") *>
            store.recordVoid("o-9") *>
            store.expireDueAuthorizations(10) *>
            IO.pure(span.context.spanIdHex)
        }
        spans <- tt.finishedSpans
      } yield {
        val byName = spans.filter(_.getParentSpanId == parentId).map { s =>
          (s.getName, attr(s, "order.id"), attr(s, "outcome"))
        }
        assertEquals(
          byName.toSet,
          Set[(String, Option[String], Option[String])](
            ("payment.record_authorization", Some("o-1"), None),
            ("payment.record_decline", Some("o-2"), None),
            ("payment.record_capture", Some("o-1"), None),
            // already captured: the conditional update matches nothing
            ("payment.record_capture_failure", Some("o-1"), Some("no_match")),
            ("payment.record_void", Some("o-9"), Some("no_match")),
            ("payment.expire", None, None)
          )
        )
      }
    }
  }

  test("a store error marks the span Error") {
    Tracing.test[IO]("payment-store-tracing-error").use { tt =>
      for {
        underlying <- PaymentStore.inMemory[IO]
        failing = new PaymentStore[IO] {
          export underlying.{recordCapture => _, *}
          def recordCapture(orderId: OrderId): IO[Option[Payment]] =
            IO.raiseError(new RuntimeException("db down"))
        }
        result <- PaymentStoreTracing
          .instrument[IO](failing, tt.tracer)
          .recordCapture("o-1")
          .attempt
        spans <- tt.finishedSpans
      } yield {
        assert(result.isLeft)
        assertEquals(
          spans
            .filter(_.getName == "payment.record_capture")
            .map(_.getStatus.getStatusCode),
          List(StatusCode.ERROR)
        )
      }
    }
  }

  test("gateway calls run in spans with an approved/declined outcome") {
    val gateway = new PaymentGatewayClient[IO] {
      def authorize(
          cardToken: String,
          amountCents: Int
      ): IO[Either[DeclineReason, Authorization]] =
        IO.pure(
          if (cardToken == "bad") DeclineReason("card").asLeft
          else authorization.asRight
        )
      def capture(
          authId: String,
          cardToken: String
      ): IO[Either[DeclineReason, Unit]] = IO.pure(().asRight)
      def void(authId: String): IO[Unit] = IO.unit
    }
    Tracing.test[IO]("payment-gateway-tracing").use { tt =>
      val traced = PaymentGatewayTracing.instrument[IO](gateway, tt.tracer)
      for {
        _ <- traced.authorize("good", 100)
        _ <- traced.authorize("bad", 100)
        _ <- traced.capture("auth_1", "good")
        _ <- traced.void("auth_1")
        spans <- tt.finishedSpans
      } yield assertEquals(
        spans.map(s => (s.getName, attr(s, "outcome"))),
        List[(String, Option[String])](
          ("payment.gateway.authorize", Some("approved")),
          ("payment.gateway.authorize", Some("declined")),
          ("payment.gateway.capture", Some("approved")),
          ("payment.gateway.void", None)
        )
      )
    }
  }
}
