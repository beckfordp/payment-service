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

  private def envelopeJson(eventType: String, payload: Json): String =
    EventEnvelope(
      eventId = UUID.randomUUID().toString(),
      eventType = eventType,
      timestamp = Instant.parse("2026-01-01T00:00:00Z"),
      source = "fulfilment-service",
      correlationId = "order-1",
      payload = payload
    ).asJson.noSpaces

  private final case class Spy(
      voids: Ref[IO, List[String]],
      captures: Ref[IO, List[(String, String)]],
      client: PaymentGatewayClient[IO]
  )

  private def spy(
      captureOutcome: Either[DeclineReason, Unit] = Right(())
  ): IO[Spy] =
    for {
      voids <- Ref.of[IO, List[String]](Nil)
      captures <- Ref.of[IO, List[(String, String)]](Nil)
    } yield Spy(
      voids,
      captures,
      new PaymentGatewayClient[IO] {
        def authorize(
            token: String,
            amount: Int
        ): IO[Either[DeclineReason, Authorization]] =
          IO.raiseError(new NotImplementedError())
        def capture(
            authId: String,
            cardToken: String
        ): IO[Either[DeclineReason, Unit]] =
          captures.update(_ :+ (authId -> cardToken)) *> IO.pure(captureOutcome)
        def void(authId: String): IO[Unit] = voids.update(_ :+ authId)
      }
    )

  private def runConsumer(
      config: KafkaConfig,
      store: PaymentStore[IO],
      methods: PaymentMethodStore[IO],
      gateway: PaymentGatewayClient[IO],
      deadLetter: DeadLetter[IO, String, String]
  ): IO[Unit] =
    FulfilmentEventConsumer
      .run[IO](config, store, methods, gateway, NoOpLogger[IO], deadLetter)
      .compile
      .drain

  private def pollStatus(
      store: PaymentStore[IO],
      orderId: String,
      expected: PaymentStatus
  ): IO[PaymentStatus] =
    store
      .findByOrderId(orderId)
      .flatMap {
        case Some(payment) if payment.status == expected => IO.pure(expected)
        case _ => IO.sleep(200.millis) *> pollStatus(store, orderId, expected)
      }
      .timeout(15.seconds)

  private val authorization =
    Authorization("auth-1", Instant.parse("2026-11-03T12:00:00Z"))

  private val address = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private def dispatchConfirmed(orderId: String): DispatchConfirmedEvent =
    DispatchConfirmedEvent(
      orderId,
      "TRACK-1",
      List(FulfilmentItem("sku-1", "Widget", 1, 4999))
    )

  test("DispatchFailed voids an authorized payment exactly once") {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        store <- PaymentStore.inMemory[IO]
        methods <- PaymentMethodStore.inMemory[IO]
        s <- spy()
        orderId = UUID.randomUUID().toString
        _ <- store.recordAuthorization(orderId, 4999, "pm-1", authorization)
        event = DispatchFailedEvent(orderId, "damaged")
        _ <- produce(
          config,
          orderId,
          envelopeJson("DispatchFailed", event.asJson)
        )
        _ <- produce(
          config,
          orderId,
          envelopeJson("DispatchFailed", event.asJson)
        )
        status <- IO.race(
          runConsumer(config, store, methods, s.client, deadLetter),
          pollStatus(store, orderId, PaymentStatus.Voided)
        )
        _ <- IO.sleep(2.seconds)
        voided <- s.voids.get
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
        methods <- PaymentMethodStore.inMemory[IO]
        s <- spy()
        created <- store.create("order-pending", 1000)
        event = DispatchFailedEvent(created.orderId, "damaged")
        _ <-
          produce(
            config,
            created.orderId,
            envelopeJson("DispatchFailed", event.asJson)
          )
        _ <- runConsumer(
          config,
          store,
          methods,
          s.client,
          deadLetter
        ).background
          .use(_ => IO.sleep(3.seconds))
        status <- store.findByOrderId(created.orderId)
        voided <- s.voids.get
      } yield {
        assertEquals(status.map(_.status), Some(PaymentStatus.Pending))
        assertEquals(voided, Nil)
      }
    }
  }

  test("DispatchConfirmed captures an authorized payment exactly once") {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
        methods <- PaymentMethodStore.inMemory[IO]
        method <- methods.create(
          "customer-1",
          Card("4242", CardBrand.Visa, "12/99"),
          address
        )
        s <- spy()
        orderId = UUID.randomUUID().toString
        _ <- store.recordAuthorization(orderId, 4999, method.id, authorization)
        event = dispatchConfirmed(orderId)
        _ <- produce(
          config,
          orderId,
          envelopeJson("DispatchConfirmed", event.asJson)
        )
        _ <- produce(
          config,
          orderId,
          envelopeJson("DispatchConfirmed", event.asJson)
        )
        status <- IO.race(
          runConsumer(config, store, methods, s.client, deadLetter),
          pollStatus(store, orderId, PaymentStatus.Captured)
        )
        _ <- IO.sleep(2.seconds)
        captures <- s.captures.get
        events <- outbox.get
      } yield {
        assertEquals(status, Right(PaymentStatus.Captured))
        assertEquals(captures, List("auth-1" -> "tok_visa_4242"))
        assertEquals(events.count(_._2.eventType == "PaymentCaptured"), 1)
      }
    }
  }

  test("DispatchConfirmed records a capture decline as Failed") {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
        methods <- PaymentMethodStore.inMemory[IO]
        method <- methods.create(
          "customer-1",
          Card("4444", CardBrand.Mastercard, "12/99"),
          address
        )
        s <- spy(captureOutcome = Left(DeclineReason("capture_declined")))
        orderId = UUID.randomUUID().toString
        _ <- store.recordAuthorization(orderId, 4999, method.id, authorization)
        event = dispatchConfirmed(orderId)
        _ <- produce(
          config,
          orderId,
          envelopeJson("DispatchConfirmed", event.asJson)
        )
        status <- IO.race(
          runConsumer(config, store, methods, s.client, deadLetter),
          pollStatus(store, orderId, PaymentStatus.Failed)
        )
        _ <- IO.sleep(1.second)
        captures <- s.captures.get
        found <- store.findByOrderId(orderId)
        events <- outbox.get
      } yield {
        assertEquals(status, Right(PaymentStatus.Failed))
        assertEquals(captures, List("auth-1" -> "tok_mastercard_4444"))
        assertEquals(found.flatMap(_.declineReason), Some("capture_failed"))
        val failures = events.filter(_._2.eventType == "PaymentFailed")
        assertEquals(failures.size, 1)
        val payload =
          failures.head._2.payload.as[PaymentFailedEvent].toOption.get
        assertEquals(payload.reason, "capture_failed")
        assertEquals(payload.amountCents, 4999)
      }
    }
  }

  test("DispatchConfirmed for a non-authorized payment is skipped") {
    withContainers { kafka =>
      val config = configFor(kafka)
      for {
        deadLetter <- noOpDeadLetter
        (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
        methods <- PaymentMethodStore.inMemory[IO]
        s <- spy()
        created <- store.create("order-pending", 1000)
        event = dispatchConfirmed(created.orderId)
        _ <- produce(
          config,
          created.orderId,
          envelopeJson("DispatchConfirmed", event.asJson)
        )
        _ <- runConsumer(
          config,
          store,
          methods,
          s.client,
          deadLetter
        ).background
          .use(_ => IO.sleep(3.seconds))
        found <- store.findByOrderId(created.orderId)
        captures <- s.captures.get
        events <- outbox.get
      } yield {
        assertEquals(found.map(_.status), Some(PaymentStatus.Pending))
        assertEquals(captures, Nil)
        assertEquals(events, Nil)
      }
    }
  }
}
