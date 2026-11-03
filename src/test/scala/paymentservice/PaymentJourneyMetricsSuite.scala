package paymentservice

import cats.effect.IO
import io.opentelemetry.sdk.metrics.data.MetricData
import munit.CatsEffectSuite
import org.typelevel.log4cats.testing.StructuredTestingLogger
import paymentservice.TestConversions.given
import paymentservice.domain.{
  OrderId,
  PaymentId,
  PaymentMethodId,
  PaymentStatus
}
import purerest.metrics.Metrics

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

class PaymentJourneyMetricsSuite extends CatsEffectSuite {

  private val methodId = PaymentMethodId.fromUuid(UUID.randomUUID())
  private val authorization =
    Authorization("auth_1", Instant.parse("2099-11-03T12:00:00Z"))

  private def counter(
      metrics: List[MetricData],
      name: String
  ): Map[Map[String, String], Long] =
    metrics
      .find(_.getName == name)
      .map(
        _.getLongSumData.getPoints.asScala
          .map { p =>
            p.getAttributes.asMap.asScala
              .map((k, v) => k.getKey -> v.toString)
              .toMap -> p.getValue
          }
          .toMap
      )
      .getOrElse(Map.empty)

  test(
    "counts authorize, capture, void and expiry outcomes and logs milestones"
  ) {
    Metrics.test[IO]("payment-journey").use { tm =>
      val logger = StructuredTestingLogger.impl[IO]()
      for {
        underlying <- PaymentStore.inMemory[IO]
        store <- PaymentJourneyMetrics
          .instrument[IO](underlying, tm.meter, logger)
        _ <- store.recordAuthorization("o-1", 100, methodId, authorization)
        _ <- store.recordDecline("o-2", 100, methodId, DeclineReason("card"))
        _ <- store.recordCapture("o-1")
        _ <- store.recordAuthorization("o-3", 100, methodId, authorization)
        _ <- store.recordCaptureFailure("o-3", "gateway")
        _ <- store.recordAuthorization("o-4", 100, methodId, authorization)
        _ <- store.recordVoid("o-4")
        _ <- store.recordAuthorization("o-5", 100, methodId, authorization)
        _ <- store.expireAuthorizationNow("o-5")
        _ <- store.expireDueAuthorizations(10)
        missing <- store.recordCapture("o-unknown")
        metrics <- tm.collectMetrics
        logged <- logger.logged
      } yield {
        assertEquals(missing, None)
        assertEquals(
          counter(metrics, "payments.authorize.outcome"),
          Map(
            Map("outcome" -> "authorized") -> 4L,
            Map("outcome" -> "declined") -> 1L
          )
        )
        assertEquals(
          counter(metrics, "payments.capture.outcome"),
          Map(
            Map("outcome" -> "captured") -> 1L,
            Map("outcome" -> "failed") -> 1L
          )
        )
        assertEquals(counter(metrics, "payments.voided").values.sum, 1L)
        assertEquals(
          counter(metrics, "payments.authorizations.expired").values.sum,
          1L
        )
        val milestones = logged.toList.flatMap(_.ctx.get("milestone"))
        assert(milestones.contains("payment_captured"), milestones)
        assert(milestones.contains("payment_expired"), milestones)
        assertEquals(
          logged.toList
            .find(_.ctx.get("milestone").contains("payment_declined"))
            .flatMap(_.ctx.get("order_id")),
          Some("o-2")
        )
      }
    }
  }

  test("uncaptured gauges report count and oldest authorization age") {
    Metrics.test[IO]("payment-gauges").use { tm =>
      for {
        now <- IO.realTimeInstant
        stats = UncapturedAuthorizations(3, Some(now.minusSeconds(600)))
        metrics <- UncapturedAuthorizationGauges
          .register[IO](tm.meter)(IO.pure(stats))
          .surround(tm.collectMetrics)
      } yield {
        val count = metrics.find(_.getName == "payments.uncaptured.count").get
        assertEquals(count.getLongGaugeData.getPoints.asScala.head.getValue, 3L)
        val age = metrics
          .find(_.getName == "payments.uncaptured.oldest_age")
          .get
          .getDoubleGaugeData
          .getPoints
          .asScala
          .head
          .getValue
        assert(age >= 600.0 && age < 610.0, s"age $age")
      }
    }
  }

  test("gauges report zero age when nothing is uncaptured") {
    Metrics.test[IO]("payment-gauges").use { tm =>
      UncapturedAuthorizationGauges
        .register[IO](tm.meter)(IO.pure(UncapturedAuthorizations(0, None)))
        .surround(tm.collectMetrics)
        .map { metrics =>
          val age = metrics
            .find(_.getName == "payments.uncaptured.oldest_age")
            .get
            .getDoubleGaugeData
            .getPoints
            .asScala
            .head
            .getValue
          assertEquals(age, 0.0)
        }
    }
  }

  test("fromPayments counts Authorized rows and takes the oldest updatedAt") {
    val t0 = Instant.parse("2026-01-01T00:00:00Z")
    def p(status: PaymentStatus, at: Instant) =
      Payment(
        PaymentId.fromUuid(UUID.randomUUID()),
        OrderId.trusted("o"),
        1,
        status,
        at,
        at
      )
    assertEquals(
      UncapturedAuthorizationGauges.fromPayments(
        List(
          p(PaymentStatus.Authorized, t0.plusSeconds(5)),
          p(PaymentStatus.Authorized, t0),
          p(PaymentStatus.Captured, t0.minusSeconds(5))
        )
      ),
      UncapturedAuthorizations(2, Some(t0))
    )
  }
}
