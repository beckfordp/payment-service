package paymentservice

import cats.effect.{IO, Ref}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.Meter
import purekafka.consumer.DeadLetter
import purekafka.resilience.{
  CircuitBreakerConfig,
  ResilienceConfig,
  RetryConfig
}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

class FulfilmentEventConsumerSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  private def noOpDeadLetter: IO[DeadLetter[IO, String, String]] = {
    import org.typelevel.otel4s.metrics.Meter.Implicits.noop
    DeadLetter.fromPublish[IO, String, String](
      RetryConfig(maxRetries = 0, baseDelay = 1.milli),
      ResilienceConfig(
        retry = RetryConfig(maxRetries = 0, baseDelay = 1.milli),
        circuitBreaker =
          CircuitBreakerConfig(failureThreshold = 100, resetTimeout = 1.second)
      ),
      (_, _, _) => IO.unit
    )(NoOpLogger[IO])(summon[Meter[IO]])
  }

  private def produce(
      config: KafkaConfig,
      key: String,
      json: String
  ): IO[Unit] = {
    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(
        _.produceOne_(
          ProducerRecord(FulfilmentEventConsumer.topic, key, json)
        ).flatten.void
      )
  }

  private def envelopeJson(payload: Json): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString(),
      eventType = "DispatchFailed",
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "fulfilment-service",
      correlationId = "order-1",
      payload = payload
    ).asJson.noSpaces

  private def gateway(voids: Ref[IO, List[String]]): PaymentGatewayClient[IO] =
    new PaymentGatewayClient[IO] {
      def authorize(token: String, amount: Int): IO[Either[DeclineReason, Authorization]] =
        IO.raiseError(new NotImplementedError())
      def capture(authId: String): IO[Unit] =
        IO.raiseError(new NotImplementedError())
      def void(authId: String): IO[Unit] = voids.update(_ :+ authId)
    }

  private def pollStatus(
      store: PaymentStore[IO],
      orderId: String,
      expected: PaymentStatus
  ): IO[PaymentStatus] =
    store.findByOrderId(orderId).flatMap {
      case Some(payment) if payment.status == expected => IO.pure(expected)
      case _ => IO.sleep(200.millis) *> pollStatus(store, orderId, expected)
    }.timeout(15.seconds)

  private val authorization =
    Authorization("auth-1", Instant.parse("2026-11-03T12:00:00Z"))

  test("DispatchFailed voids an authorized payment exactly once") {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- PaymentStore.inMemory[IO]
        voids <- Ref.of[IO, List[String]](Nil)
        orderId = UUID.randomUUID().toString
        _ <- store.recordAuthorization(orderId, 4999, "pm-1", authorization)
        event = DispatchFailedEvent(orderId, "damaged")
        _ <- produce(config, orderId, envelopeJson(event.asJson))
        _ <- produce(config, orderId, envelopeJson(event.asJson))
        status <- IO.race(
          FulfilmentEventConsumer
            .run[IO](config, store, gateway(voids), NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          pollStatus(store, orderId, PaymentStatus.Voided)
        )
        _ <- IO.sleep(2.seconds)
        voided <- voids.get
      } yield {
        assertEquals(status, Right(PaymentStatus.Voided))
        assertEquals(voided, List("auth-1"))
      }
    }
  }

  test("DispatchFailed for a non-authorized payment is skipped") {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- PaymentStore.inMemory[IO]
        voids <- Ref.of[IO, List[String]](Nil)
        created <- store.create("order-pending", 1000)
        event = DispatchFailedEvent(created.orderId, "damaged")
        _ <- produce(config, created.orderId, envelopeJson(event.asJson))
        _ <- FulfilmentEventConsumer
          .run[IO](config, store, gateway(voids), NoOpLogger[IO], deadLetter)
          .compile
          .drain
          .background
          .use(_ => IO.sleep(3.seconds))
        status <- store.findByOrderId(created.orderId)
        voided <- voids.get
      } yield {
        assertEquals(status.map(_.status), Some(PaymentStatus.Pending))
        assertEquals(voided, Nil)
      }
    }
  }
}
