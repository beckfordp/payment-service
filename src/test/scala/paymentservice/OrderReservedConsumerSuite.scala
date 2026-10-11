package paymentservice

import cats.effect.IO
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._
import munit.CatsEffectSuite
import org.typelevel.log4cats.noop.NoOpLogger

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

class OrderReservedConsumerSuite extends CatsEffectSuite with TestContainerForAll {

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
    "a synthetic OrderReserved envelope creates a settled Payment and publishes PaymentSettled"
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
        store <- PaymentStore.inMemory[IO]
        raced <- PaymentEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
          publisher =>
            for {
              _ <- produce(
                config,
                OrderReservedConsumer.topic,
                event.orderId,
                envelopeJson("OrderReserved", event.orderId, event.asJson)
              )
              raced <- IO.race(
                OrderReservedConsumer
                  .run[IO](config, store, publisher, NoOpLogger[IO])
                  .compile
                  .drain,
                consumeOne(config, PaymentEventPublisher.topic)
              )
            } yield raced
        }
        settledJson = raced match {
          case Right(json) => json
          case Left(_) =>
            fail(
              "consumer stream completed before a PaymentSettled message arrived"
            )
        }
        settledEnvelope <- IO.fromEither(decode[EventEnvelope](settledJson))
        settledEvent <- IO.fromEither(
          settledEnvelope.payload.as[PaymentSettledEvent]
        )
        stored <- store.get(settledEvent.paymentId)
      } yield {
        assertEquals(settledEnvelope.eventType, "PaymentSettled")
        assertEquals(settledEvent.orderId, event.orderId)
        assertEquals(settledEvent.amountCents, event.totalCents)
        assertEquals(stored.map(_.status), Some(PaymentStatus.Settled))
        assertEquals(stored.map(_.orderId), Some(event.orderId))
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
        store <- PaymentStore.inMemory[IO]
        raced <- PaymentEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
          publisher =>
            for {
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
                envelopeJson(
                  "OrderReserved",
                  goodEvent.orderId,
                  goodEvent.asJson
                )
              )
              raced <- IO.race(
                OrderReservedConsumer
                  .run[IO](config, store, publisher, NoOpLogger[IO])
                  .compile
                  .drain,
                consumeOne(config, PaymentEventPublisher.topic)
              )
            } yield raced
        }
      } yield assert(
        raced.isRight,
        s"expected the OrderReserved event to still produce a PaymentSettled message despite the earlier differently-typed one, got: $raced"
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
        store <- PaymentStore.inMemory[IO]
        raced <- PaymentEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
          publisher =>
            for {
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
                envelopeJson(
                  "OrderReserved",
                  goodEvent.orderId,
                  goodEvent.asJson
                )
              )
              raced <- IO.race(
                OrderReservedConsumer
                  .run[IO](config, store, publisher, NoOpLogger[IO])
                  .compile
                  .drain,
                consumeOne(config, PaymentEventPublisher.topic)
              )
            } yield raced
        }
      } yield assert(
        raced.isRight,
        s"expected the good event to still produce a PaymentSettled message despite the earlier malformed one, got: $raced"
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
        store <- PaymentStore.inMemory[IO]
        raced <- PaymentEventPublisher.resource[IO](config, NoOpLogger[IO]).use {
          publisher =>
            for {
              _ <- produceNullKey(
                config,
                OrderReservedConsumer.topic,
                "{\"irrelevant\":\"null-key record\"}"
              )
              _ <- produce(
                config,
                OrderReservedConsumer.topic,
                goodEvent.orderId,
                envelopeJson(
                  "OrderReserved",
                  goodEvent.orderId,
                  goodEvent.asJson
                )
              )
              raced <- IO.race(
                OrderReservedConsumer
                  .run[IO](config, store, publisher, NoOpLogger[IO])
                  .compile
                  .drain,
                consumeOne(config, PaymentEventPublisher.topic)
              )
            } yield raced
        }
      } yield assert(
        raced.isRight,
        s"expected the good event to still produce a PaymentSettled message despite the earlier null-keyed one, got: $raced"
      )
    }
  }
}
