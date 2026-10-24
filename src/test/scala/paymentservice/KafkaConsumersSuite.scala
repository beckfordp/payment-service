package paymentservice

import cats.effect.{IO, Ref}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger
import purerest.metrics.Metrics

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Proves `KafkaConsumers.supervised`'s DLQ-routing property for
  * `OrderReservedConsumer` (TD-2.4, ADR 0010).
  *
  * Unlike order-service's own `KafkaConsumersSuite` (which also proves a
  * *merge-isolation* property — wrapping two consumers individually, then
  * merging, so a crash in one doesn't restart the other), payment-service
  * has only one consumer: there's no merge, so no merge-isolation property
  * to prove here. `StreamSupervisor`'s restart-with-backoff behavior itself
  * is already proven by `purekafka`'s own `StreamSupervisorSuite` against a
  * synthetic `Stream[F, O]` — re-proving that generic mechanism again here,
  * through this thin single-consumer wrapper, would add no wiring-specific
  * value. Only the DLQ-routing test is adapted.
  */
class KafkaConsumersSuite extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: KafkaContainer.Def = KafkaContainer.Def()

  private def configFor(kafka: KafkaContainer): KafkaConfig =
    KafkaConfig(bootstrapServers = kafka.bootstrapServers)

  private def produce(
      config: KafkaConfig,
      topic: String,
      key: String,
      json: String
  ): IO[Unit] = {
    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(_.produceOne_(ProducerRecord(topic, key, json)).flatten.void)
  }

  private def envelopeJson(eventType: String, correlationId: String, payload: io.circe.Json): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "order-service",
      correlationId = correlationId,
      payload = payload
    ).asJson.noSpaces

  private def pollUntil[A](check: IO[Option[A]]): IO[A] =
    check.flatMap {
      case Some(a) => IO.pure(a)
      case None    => IO.sleep(200.millis) *> pollUntil(check)
    }

  private def awaitSettled(settledRef: Ref[IO, Option[Payment]]): IO[Payment] =
    pollUntil(settledRef.get).timeout(20.seconds)

  /** Wraps a real `PaymentStore` with a side channel recording the one
    * `Settled` payment `update` produces - mirrors
    * `OrderReservedConsumerSuite`'s own `observingStore` helper. Only the
    * healthy order ever reaches `update` (the poison order fails at
    * `create`, below), so this ref only ever observes the healthy payment.
    */
  private def observingStore(
      underlying: PaymentStore[IO],
      settledRef: Ref[IO, Option[Payment]]
  ): PaymentStore[IO] =
    new PaymentStore[IO] {
      def create(orderId: String, amountCents: Int): IO[Payment] =
        underlying.create(orderId, amountCents)
      def get(id: String): IO[Option[Payment]] = underlying.get(id)
      def update(id: String, status: PaymentStatus): IO[Option[Payment]] =
        underlying.update(id, status).flatTap {
          case Some(payment) if status == PaymentStatus.Settled =>
            settledRef.set(Some(payment))
          case _ => IO.unit
        }
      def delete(id: String): IO[Boolean] = underlying.delete(id)
      def ping: IO[Boolean] = underlying.ping
    }

  /** Fails `create` every time for `failOnOrderId` - never recovers. Used to
    * prove `DeadLetter` actually exhausts its retry budget and routes to the
    * DLQ, rather than eventually succeeding.
    */
  private def alwaysFailOn(
      base: PaymentStore[IO],
      failOnOrderId: String
  ): PaymentStore[IO] = new PaymentStore[IO] {
    def create(orderId: String, amountCents: Int): IO[Payment] =
      if (orderId == failOnOrderId)
        IO.raiseError(
          new RuntimeException("synthetic permanent store failure")
        )
      else base.create(orderId, amountCents)
    def get(id: String): IO[Option[Payment]] = base.get(id)
    def update(id: String, status: PaymentStatus): IO[Option[Payment]] =
      base.update(id, status)
    def delete(id: String): IO[Boolean] = base.delete(id)
    def ping: IO[Boolean] = base.ping
  }

  /** Consumes whatever's on `topic` right now (from the beginning) within a
    * short window, for asserting on DLQ contents directly - a plain, one-shot
    * read, not a long-running consumer group.
    */
  private def consumeOneFrom(
      config: KafkaConfig,
      topic: String
  ): IO[List[String]] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(s"dlq-check-${UUID.randomUUID()}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(topic)
      .records
      .map(_.record.value)
      .interruptAfter(5.seconds)
      .compile
      .toList
  }

  test(
    "a message that fails processing every time is routed to order-events-dlq after exhausting retries, and the stream keeps consuming later messages (TD-2.4)"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      Metrics.test[IO]("payment-kafka-consumers-dlq-suite").use { testMeter =>
        for {
          underlying <- PaymentStore.inMemory[IO]
          settledRef <- Ref.of[IO, Option[Payment]](None)
          store = alwaysFailOn(
            observingStore(underlying, settledRef),
            "order-poison"
          )
          poisonEvent = OrderReservedEvent(
            orderId = "order-poison",
            customerId = "cust-poison",
            totalCents = 1000,
            timestamp = Instant.parse("2026-01-01T00:00:00Z")
          )
          healthyEvent = OrderReservedEvent(
            orderId = "order-healthy",
            customerId = "cust-healthy",
            totalCents = 2000,
            timestamp = Instant.parse("2026-01-01T00:00:00Z")
          )
          _ <- produce(
            config,
            OrderReservedConsumer.topic,
            poisonEvent.orderId,
            envelopeJson(
              "OrderReserved",
              poisonEvent.orderId,
              poisonEvent.asJson
            )
          )
          _ <- produce(
            config,
            OrderReservedConsumer.topic,
            healthyEvent.orderId,
            envelopeJson(
              "OrderReserved",
              healthyEvent.orderId,
              healthyEvent.asJson
            )
          )
          healthyFinal <-
            KafkaConsumers
              .supervised[IO](config, store, NoOpLogger[IO], testMeter.meter)
              .compile
              .drain
              .background
              .use { _ =>
                // Proves the stream didn't get stuck/killed on the poison
                // message - a later, healthy message on the same topic
                // still gets processed and settles normally.
                awaitSettled(settledRef)
              }
          dlqMessages <- consumeOneFrom(
            config,
            s"${OrderReservedConsumer.topic}-dlq"
          )
          metrics <- testMeter.collectMetrics
          dlqPublished = metrics
            .filter(_.getName == "purekafka.dlq.published")
            .flatMap(_.getLongSumData.getPoints.asScala)
            .map(_.getValue)
            .sum
        } yield {
          assertEquals(healthyFinal.orderId, "order-healthy")
          assertEquals(healthyFinal.status, PaymentStatus.Settled)
          assert(
            dlqMessages.exists(_.contains("order-poison")),
            s"expected a DLQ message mentioning order-poison, got $dlqMessages"
          )
          assert(
            dlqPublished >= 1L,
            s"expected at least 1 purekafka.dlq.published, got $dlqPublished"
          )
        }
      }
    }
  }
}
