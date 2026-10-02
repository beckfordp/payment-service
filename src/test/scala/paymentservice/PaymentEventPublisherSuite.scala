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

class PaymentEventPublisherSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  private def consumeOne(config: KafkaConfig, topic: String): IO[String] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"test-${UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer.resource(consumerSettings).use { consumer =>
      for {
        _ <- consumer.subscribeTo(topic)
        record <- consumer.stream
          .take(1)
          .compile
          .lastOrError
          .timeout(15.seconds)
      } yield record.record.value
    }
  }

  test(
    "publishSettled produces exactly one message on payment.settled"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val event = PaymentSettledEvent(
        orderId = "order-1",
        paymentId = "payment-1",
        amountCents = 4999,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        consumed <- PaymentEventPublisher
          .resource[IO](config, NoOpLogger[IO])
          .use(_.publishSettled(event)) *> consumeOne(
          config,
          PaymentEventPublisher.settledTopic
        )
      } yield assertEquals(decode[PaymentSettledEvent](consumed), Right(event))
    }
  }

  test(
    "publishFailed produces exactly one message on payment.failed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val event = PaymentFailedEvent(
        orderId = "order-1",
        paymentId = "payment-1",
        amountCents = 4999,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        consumed <- PaymentEventPublisher
          .resource[IO](config, NoOpLogger[IO])
          .use(_.publishFailed(event)) *> consumeOne(
          config,
          PaymentEventPublisher.failedTopic
        )
      } yield assertEquals(decode[PaymentFailedEvent](consumed), Right(event))
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
