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

class OrderReservedConsumerSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  /** A `DeadLetter` whose `guard` never actually triggers in these tests -
    * every `handleEvent` call here succeeds on its first attempt, so this is
    * only ever a transparent pass-through. `KafkaConsumersSuite` is where DLQ
    * routing itself (the failure path) is actually exercised.
    */
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

  /** Sends a record with a genuinely null key, bypassing fs2-kafka's typed
    * `String` serializer (which itself throws on a null key before ever
    * reaching the broker) - exactly what a real external producer (or a raw
    * `kafka-console-producer.sh` invocation with no key set) can send.
    */
  private def produceNullKey(
      config: KafkaConfig,
      topic: String,
      value: String
  ): IO[Unit] = {
    val producerSettings =
      ProducerSettings[IO, Array[Byte], Array[Byte]]
        .withBootstrapServers(config.bootstrapServers)
    KafkaProducer
      .resource(producerSettings)
      .use(
        _.produceOne_(
          ProducerRecord(topic, null, value.getBytes("UTF-8"))
        ).flatten.void
      )
  }

  private def envelopeJson(
      eventType: String,
      correlationId: String,
      payload: Json
  ): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString,
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "order-service",
      correlationId = correlationId,
      payload = payload
    ).asJson.noSpaces

  /** Wraps a real `PaymentStore` with a side channel recording every
    * `Settled` payment `update` produces - ADR 0008 moved the outbox write
    * itself inside `PaymentStore.update` (Phase 1, no live Kafka needed to
    * observe it), so these consumer-level tests no longer race against a
    * real `payment-events` message; they poll this instead.
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

  private def awaitSettled(settledRef: Ref[IO, Option[Payment]]): IO[Payment] =
    (IO.sleep(50.millis) *> settledRef.get)
      .iterateUntil(_.isDefined)
      .map(_.get)
      .timeout(15.seconds)

  test(
    "a synthetic OrderReserved envelope settles the Payment via the outbox-aware store"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val event = OrderReservedEvent(
        orderId = "order-1",
        customerId = "cust-1",
        totalCents = 4999,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        underlying <- PaymentStore.inMemory[IO]
        settledRef <- Ref.of[IO, Option[Payment]](None)
        store = observingStore(underlying, settledRef)
        deadLetter <- noOpDeadLetter
        _ <- produce(
          config,
          OrderReservedConsumer.topic,
          event.orderId,
          envelopeJson("OrderReserved", event.orderId, event.asJson)
        )
        raced <- IO.race(
          OrderReservedConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          awaitSettled(settledRef)
        )
        settled = raced match {
          case Right(payment) => payment
          case Left(_)         =>
            fail("consumer stream completed before the payment settled")
        }
      } yield {
        assertEquals(settled.orderId, event.orderId)
        assertEquals(settled.amountCents, event.totalCents)
        assertEquals(settled.status, PaymentStatus.Settled)
      }
    }
  }

  test(
    "an envelope with a different eventType on order-events is ignored, not treated as malformed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val goodEvent = OrderReservedEvent(
        orderId = "order-ignore-1",
        customerId = "cust-ignore",
        totalCents = 1200,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        underlying <- PaymentStore.inMemory[IO]
        settledRef <- Ref.of[IO, Option[Payment]](None)
        store = observingStore(underlying, settledRef)
        deadLetter <- noOpDeadLetter
        _ <- produce(
          config,
          OrderReservedConsumer.topic,
          "order-other-1",
          envelopeJson(
            "OrderStatusChanged",
            "order-other-1",
            Json.obj(
              "orderId" -> Json.fromString("order-other-1"),
              "customerId" -> Json.fromString("cust-x"),
              "status" -> Json.fromString("confirmed"),
              "timestamp" -> Json.fromString("2026-01-01T00:00:00Z")
            )
          )
        )
        _ <- produce(
          config,
          OrderReservedConsumer.topic,
          goodEvent.orderId,
          envelopeJson("OrderReserved", goodEvent.orderId, goodEvent.asJson)
        )
        raced <- IO.race(
          OrderReservedConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          awaitSettled(settledRef)
        )
      } yield assert(
        raced.isRight,
        s"expected the OrderReserved event to still settle the payment despite the earlier differently-typed one, got: $raced"
      )
    }
  }

  test(
    "a malformed envelope is logged and doesn't block later events"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val goodEvent = OrderReservedEvent(
        orderId = "order-2",
        customerId = "cust-2",
        totalCents = 1500,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        underlying <- PaymentStore.inMemory[IO]
        settledRef <- Ref.of[IO, Option[Payment]](None)
        store = observingStore(underlying, settledRef)
        deadLetter <- noOpDeadLetter
        _ <- produce(
          config,
          OrderReservedConsumer.topic,
          "bad-key",
          "not-json"
        )
        _ <- produce(
          config,
          OrderReservedConsumer.topic,
          goodEvent.orderId,
          envelopeJson("OrderReserved", goodEvent.orderId, goodEvent.asJson)
        )
        raced <- IO.race(
          OrderReservedConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          awaitSettled(settledRef)
        )
      } yield assert(
        raced.isRight,
        s"expected the good event to still settle the payment despite the earlier malformed one, got: $raced"
      )
    }
  }

  test(
    "a null-keyed record on order-events doesn't crash the stream, and a later event is still processed"
  ) {
    withContainers { kafka =>
      val config = configFor(kafka)
      val goodEvent = OrderReservedEvent(
        orderId = "order-3",
        customerId = "cust-3",
        totalCents = 2500,
        timestamp = Instant.parse("2026-01-01T00:00:00Z")
      )
      for {
        underlying <- PaymentStore.inMemory[IO]
        settledRef <- Ref.of[IO, Option[Payment]](None)
        store = observingStore(underlying, settledRef)
        deadLetter <- noOpDeadLetter
        _ <- produceNullKey(
          config,
          OrderReservedConsumer.topic,
          "{\"irrelevant\":\"null-key record\"}"
        )
        _ <- produce(
          config,
          OrderReservedConsumer.topic,
          goodEvent.orderId,
          envelopeJson("OrderReserved", goodEvent.orderId, goodEvent.asJson)
        )
        raced <- IO.race(
          OrderReservedConsumer
            .run[IO](config, store, NoOpLogger[IO], deadLetter)
            .compile
            .drain,
          awaitSettled(settledRef)
        )
      } yield assert(
        raced.isRight,
        s"expected the good event to still settle the payment despite the earlier null-keyed one, got: $raced"
      )
    }
  }
}
