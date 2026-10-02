package paymentservice

import cats.effect.IO
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import fs2.kafka._
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
    "a synthetic order.reserved event creates a settled Payment and publishes payment.settled"
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
                event.asJson.noSpaces
              )
              raced <- IO.race(
                OrderReservedConsumer
                  .run[IO](config, store, publisher, NoOpLogger[IO])
                  .compile
                  .drain,
                consumeOne(config, PaymentEventPublisher.settledTopic)
              )
            } yield raced
        }
        settledJson = raced match {
          case Right(json) => json
          case Left(_) =>
            fail(
              "consumer stream completed before a payment.settled message arrived"
            )
        }
        settledEvent <- IO.fromEither(decode[PaymentSettledEvent](settledJson))
        stored <- store.get(settledEvent.paymentId)
      } yield {
        assertEquals(settledEvent.orderId, event.orderId)
        assertEquals(settledEvent.amountCents, event.totalCents)
        assertEquals(stored.map(_.status), Some(PaymentStatus.Settled))
        assertEquals(stored.map(_.orderId), Some(event.orderId))
      }
    }
  }

  test(
    "a malformed order.reserved payload is logged and doesn't block later events"
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
                goodEvent.asJson.noSpaces
              )
              raced <- IO.race(
                OrderReservedConsumer
                  .run[IO](config, store, publisher, NoOpLogger[IO])
                  .compile
                  .drain,
                consumeOne(config, PaymentEventPublisher.settledTopic)
              )
            } yield raced
        }
      } yield assert(
        raced.isRight,
        s"expected the good event to still produce a payment.settled message despite the earlier malformed one, got: $raced"
      )
    }
  }

  test(
    "a null-keyed order.reserved record doesn't crash the stream, and a later event is still processed"
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
                goodEvent.asJson.noSpaces
              )
              raced <- IO.race(
                OrderReservedConsumer
                  .run[IO](config, store, publisher, NoOpLogger[IO])
                  .compile
                  .drain,
                consumeOne(config, PaymentEventPublisher.settledTopic)
              )
            } yield raced
        }
      } yield assert(
        raced.isRight,
        s"expected the good event to still produce a payment.settled message despite the earlier null-keyed one, got: $raced"
      )
    }
  }
}
