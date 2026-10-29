package paymentservice

import paymentservice.TestConversions.given
import scala.language.implicitConversions
import cats.effect.IO
import munit.CatsEffectSuite
import paymentservice.domain.{OrderId, PaymentId, PaymentMethodId}

import java.time.Instant
import java.util.UUID

class PaymentStoreSuite extends CatsEffectSuite {

  private val paymentMethodId =
    PaymentMethodId.fromUuid(UUID.randomUUID())
  private def unknownPaymentId: PaymentId =
    PaymentId.fromUuid(UUID.randomUUID())

  test("create returns a persisted entity with a generated id") {
    for {
      store <- PaymentStore.inMemory[IO]
      entity <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
    } yield assert(PaymentId.render(entity.id).nonEmpty)
  }

  test("get returns the persisted entity") {
    for {
      store <- PaymentStore.inMemory[IO]
      created <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
      found <- store.get(created.id)
    } yield assertEquals(found, Some(created))
  }

  test("get returns None for an unknown id") {
    for {
      store <- PaymentStore.inMemory[IO]
      found <- store.get(unknownPaymentId)
    } yield assertEquals(found, None)
  }

  test("create produces distinct ids across calls") {
    for {
      store <- PaymentStore.inMemory[IO]
      first <- store.create("order-a", 4999)
      second <- store.create("order-b", 4999)
    } yield assertNotEquals(first.id, second.id)
  }

  test(
    "delete removes the entity and returns true, and get then returns None"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      created <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
      deleted <- store.delete(created.id)
      found <- store.get(created.id)
    } yield {
      assert(deleted)
      assertEquals(found, None)
    }
  }

  test("delete returns false for an unknown id") {
    for {
      store <- PaymentStore.inMemory[IO]
      deleted <- store.delete(unknownPaymentId)
    } yield assert(!deleted)
  }

  private val authorization =
    Authorization("auth_1", Instant.parse("2099-11-03T12:00:00Z"))

  test(
    "recordAuthorization stores an Authorized payment and one outbox event"
  ) {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      payment <- store
        .recordAuthorization(
          "order-1",
          4999,
          paymentMethodId,
          authorization
        )
        .map(_.getOrElse(fail("expected the authorization to be inserted")))
      found <- store.get(payment.id)
      events <- outbox.get
    } yield {
      assertEquals(payment.status, PaymentStatus.Authorized)
      assertEquals(payment.paymentMethodId, Some(paymentMethodId))
      assertEquals(payment.authId, Some("auth_1"))
      assertEquals(payment.expiresAt, Some(authorization.expiresAt))
      assertEquals(payment.declineReason, None)
      assertEquals(found, Some(payment))
      assertEquals(events.size, 1)
      val (aggregateId, envelope) = events.head
      assertEquals(aggregateId, "order-1")
      assertEquals(envelope.eventType, "PaymentAuthorized")
      assertEquals(envelope.source, "payment-service")
      assertEquals(envelope.correlationId, "order-1")
      val event = envelope.payload.as[PaymentAuthorizedEvent].toOption.get
      assertEquals(event.orderId, "order-1")
      assertEquals(event.authId, "auth_1")
      assertEquals(event.expiresAt, authorization.expiresAt)
    }
  }

  test(
    "recordDecline stores a Failed payment with its reason and no outbox event"
  ) {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      payment <- store
        .recordDecline(
          "order-1",
          4999,
          paymentMethodId,
          DeclineReason("insufficient_funds")
        )
        .map(_.getOrElse(fail("expected the decline to be inserted")))
      found <- store.get(payment.id)
      events <- outbox.get
    } yield {
      assertEquals(payment.status, PaymentStatus.Failed)
      assertEquals(payment.paymentMethodId, Some(paymentMethodId))
      assertEquals(payment.declineReason, Some("insufficient_funds"))
      assertEquals(payment.authId, None)
      assertEquals(found, Some(payment))
      assertEquals(events, Nil)
    }
  }

  test("findByOrderId returns the stored row and None for an unknown order") {
    for {
      store <- PaymentStore.inMemory[IO]
      _ <- store
        .recordAuthorization("order-1", 4999, paymentMethodId, authorization)
        .map(_.getOrElse(fail("expected the authorization to be inserted")))
      found <- store.findByOrderId("order-1")
      missing <- store.findByOrderId("order-unknown")
    } yield {
      assertEquals(found.map(_.orderId), Some(OrderId.trusted("order-1")))
      assertEquals(found.map(_.status), Some(PaymentStatus.Authorized))
      assertEquals(missing, None)
    }
  }

  test(
    "a second recordAuthorization for the same order returns None and adds no outbox row"
  ) {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      first <- store
        .recordAuthorization("order-1", 4999, paymentMethodId, authorization)
        .map(_.getOrElse(fail("expected the first insert")))
      second <- store.recordAuthorization(
        "order-1",
        4999,
        paymentMethodId,
        authorization
      )
      events <- outbox.get
      found <- store.findByOrderId("order-1")
    } yield {
      assertEquals(second, None)
      assertEquals(found.map(_.id), Some(first.id))
      assertEquals(events.size, 1)
    }
  }

  test("a second recordDecline for the same order returns None") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      _ <- store
        .recordDecline(
          "order-1",
          4999,
          paymentMethodId,
          DeclineReason("declined")
        )
        .map(_.getOrElse(fail("expected the first decline")))
      second <- store.recordDecline(
        "order-1",
        4999,
        paymentMethodId,
        DeclineReason("declined")
      )
      events <- outbox.get
    } yield {
      assertEquals(second, None)
      assertEquals(events, Nil)
    }
  }

  private def authorizedPayment(store: PaymentStore[IO]): IO[Payment] =
    store
      .recordAuthorization("order-1", 4999, paymentMethodId, authorization)
      .map(_.getOrElse(fail("expected the authorization to be inserted")))

  test(
    "recordCapture marks an Authorized payment Captured and emits PaymentCaptured"
  ) {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      authorized <- authorizedPayment(store)
      captured <- store
        .recordCapture("order-1")
        .map(_.getOrElse(fail("expected the capture to be recorded")))
      events <- outbox.get
    } yield {
      assertEquals(captured.id, authorized.id)
      assertEquals(captured.status, PaymentStatus.Captured)
      assertEquals(captured.declineReason, None)
      assertEquals(events.size, 2)
      val (aggregateId, envelope) = events.last
      assertEquals(aggregateId, "order-1")
      assertEquals(envelope.eventType, "PaymentCaptured")
      assertEquals(envelope.source, "payment-service")
      assertEquals(envelope.correlationId, "order-1")
      val event = envelope.payload.as[PaymentCapturedEvent].toOption.get
      assertEquals(event.orderId, "order-1")
      assertEquals(event.amountCents, 4999)
    }
  }

  test("a second recordCapture returns None and adds no outbox row") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      _ <- authorizedPayment(store)
      _ <- store
        .recordCapture("order-1")
        .map(_.getOrElse(fail("expected the first capture")))
      second <- store.recordCapture("order-1")
      events <- outbox.get
    } yield {
      assertEquals(second, None)
      assertEquals(events.size, 2)
    }
  }

  test("recordCapture returns None for an unknown order") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      result <- store.recordCapture("order-unknown")
      events <- outbox.get
    } yield {
      assertEquals(result, None)
      assertEquals(events, Nil)
    }
  }

  test("recordVoid marks an Authorized payment Voided and emits nothing") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      authorized <- authorizedPayment(store)
      voided <- store.recordVoid("order-1")
      events <- outbox.get
    } yield {
      assertEquals(voided.map(_.id), Some(authorized.id))
      assertEquals(voided.map(_.status), Some(PaymentStatus.Voided))
      assertEquals(events.map(_._2.eventType), List("PaymentAuthorized"))
    }
  }

  test("recordVoid returns None for a non-authorized payment, unchanged") {
    for {
      store <- PaymentStore.inMemory[IO]
      created <- store.create("order-1", 4999)
      result <- store.recordVoid("order-1")
      after <- store.get(created.id)
    } yield {
      assertEquals(result, None)
      assertEquals(after.map(_.status), Some(PaymentStatus.Pending))
    }
  }

  test("recordCapture after recordVoid returns None (voided is terminal)") {
    for {
      store <- PaymentStore.inMemory[IO]
      _ <- authorizedPayment(store)
      _ <- store.recordVoid("order-1")
      result <- store.recordCapture("order-1")
    } yield assertEquals(result, None)
  }

  test("recordCapture returns None for a non-authorized payment") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      _ <- store.create("order-1", 4999)
      result <- store.recordCapture("order-1")
      events <- outbox.get
    } yield {
      assertEquals(result, None)
      assertEquals(events, Nil)
    }
  }

  test(
    "recordCaptureFailure marks an Authorized payment Failed and emits PaymentFailed"
  ) {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      authorized <- authorizedPayment(store)
      failed <- store
        .recordCaptureFailure("order-1", "capture_failed")
        .map(_.getOrElse(fail("expected the capture failure to be recorded")))
      events <- outbox.get
    } yield {
      assertEquals(failed.id, authorized.id)
      assertEquals(failed.status, PaymentStatus.Failed)
      assertEquals(failed.declineReason, Some("capture_failed"))
      assertEquals(events.size, 2)
      val (aggregateId, envelope) = events.last
      assertEquals(aggregateId, "order-1")
      assertEquals(envelope.eventType, "PaymentFailed")
      assertEquals(envelope.source, "payment-service")
      assertEquals(envelope.correlationId, "order-1")
      val event = envelope.payload.as[PaymentFailedEvent].toOption.get
      assertEquals(event.orderId, "order-1")
      assertEquals(event.paymentId, PaymentId.render(failed.id))
      assertEquals(event.amountCents, 4999)
      assertEquals(event.reason, "capture_failed")
    }
  }

  test("recordCaptureFailure returns None for a non-authorized payment") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      _ <- store.create("order-1", 4999)
      result <- store.recordCaptureFailure("order-1", "capture_failed")
      events <- outbox.get
    } yield {
      assertEquals(result, None)
      assertEquals(events, Nil)
    }
  }

  test("create rejects a second payment for the same order") {
    for {
      store <- PaymentStore.inMemory[IO]
      _ <- store.create("order-dup", 4999)
      result <- store.create("order-dup", 4999).attempt
    } yield assertEquals(result, Left(PaymentAlreadyExists("order-dup")))
  }

  test("recordCapture leaves an expired authorization untouched") {
    for {
      store <- PaymentStore.inMemory[IO]
      _ <- store.recordAuthorization(
        "order-expired",
        4999,
        paymentMethodId,
        Authorization("auth-x", Instant.parse("2020-01-01T00:00:00Z"))
      )
      result <- store.recordCapture("order-expired")
      after <- store.findByOrderId("order-expired")
    } yield {
      assertEquals(result, None)
      assertEquals(after.map(_.status), Some(PaymentStatus.Authorized))
    }
  }
}
