package paymentservice

import cats.effect.IO
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.parser.decode
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.WARN

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

class PaymentEventPublisherSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  /** All event types now share one `payment-events` topic (ADR 0009), so a
    * fresh "earliest" consumer can see another test's leftover message first —
    * filter for the envelope matching this test's own `correlationId` rather
    * than just taking the first record.
    */
  private def consumeEnvelope(
      config: KafkaConfig,
      topic: String,
      correlationId: String
  ): IO[String] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"test-${UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer.resource(consumerSettings).use { consumer =>
      for {
        _ <- consumer.subscribeTo(topic)
        record <- consumer.stream
          .map(_.record.value)
          .filter(raw =>
            decode[EventEnvelope](raw).exists(
              _.correlationId == correlationId
            )
          )
          .take(1)
          .compile
          .lastOrError
          .timeout(15.seconds)
      } yield record
    }
  }

  test(
    "publishSettled produces one message on payment-events, wrapped in an envelope"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val event = PaymentSettledEvent(
        orderId = "order-settled-1",
        paymentId = "payment-1",
        amountCents = 4999,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        consumed <- PaymentEventPublisher
          .resource[IO](config, NoOpLogger[IO])
          .use(_.publishSettled(event)) *> consumeEnvelope(
          config,
          PaymentEventPublisher.topic,
          event.orderId
        )
        envelope = decode[EventEnvelope](consumed).toOption.get
      } yield {
        assertEquals(envelope.eventType, "PaymentSettled")
        assertEquals(envelope.source, "payment-service")
        assertEquals(envelope.correlationId, "order-settled-1")
        assert(envelope.eventId.nonEmpty)
        assertEquals(envelope.payload.as[PaymentSettledEvent], Right(event))
      }
    }
  }

  test(
    "publishFailed produces one message on payment-events, wrapped in an envelope"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val event = PaymentFailedEvent(
        orderId = "order-failed-1",
        paymentId = "payment-1",
        amountCents = 4999,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        consumed <- PaymentEventPublisher
          .resource[IO](config, NoOpLogger[IO])
          .use(_.publishFailed(event)) *> consumeEnvelope(
          config,
          PaymentEventPublisher.topic,
          event.orderId
        )
        envelope = decode[EventEnvelope](consumed).toOption.get
      } yield {
        assertEquals(envelope.eventType, "PaymentFailed")
        assertEquals(envelope.source, "payment-service")
        assertEquals(envelope.correlationId, "order-failed-1")
        assert(envelope.eventId.nonEmpty)
        assertEquals(envelope.payload.as[PaymentFailedEvent], Right(event))
      }
    }
  }

  test(
    "publishSettled logs a WARN and does not raise once the bounded retry is exhausted against an unreachable broker"
  ) {
    val unreachableConfig = KafkaConfig(bootstrapServers = "localhost:1")
    val event = PaymentSettledEvent(
      orderId = "order-1",
      paymentId = "payment-1",
      amountCents = 4999,
      timestamp = Instant.parse("2026-01-01T00:00:00Z")
    )
    val testLogger = StructuredTestingLogger.impl[IO]()
    for {
      result <- PaymentEventPublisher
        .resource[IO](unreachableConfig, testLogger)
        .use(_.publishSettled(event))
        .attempt
      logged <- testLogger.logged
    } yield {
      assert(result.isRight, s"expected publish to not raise, got: $result")
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.nonEmpty,
        s"expected at least one WARN line, got: $logged"
      )
    }
  }
}
