package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite

import java.time.Instant

class PaymentStoreSuite extends CatsEffectSuite {

  test("create returns a persisted entity with a generated id") {
    for {
      store <- PaymentStore.inMemory[IO]
      entity <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
    } yield assert(entity.id.nonEmpty)
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
      found <- store.get("unknown-id")
    } yield assertEquals(found, None)
  }

  test("create produces distinct ids across calls") {
    for {
      store <- PaymentStore.inMemory[IO]
      first <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
      second <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
    } yield assertNotEquals(first.id, second.id)
  }

  test(
    "update returns the updated entity with updatedAt not moving backwards"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      created <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
      updated <- store.update(created.id, PaymentStatus.Pending)
    } yield {
      assertEquals(updated.map(_.id), Some(created.id))
      assert(
        updated.exists(!_.updatedAt.isBefore(created.updatedAt)),
        s"expected updatedAt not to move backwards, got: $updated"
      )
    }
  }

  test("update returns None for an unknown id") {
    for {
      store <- PaymentStore.inMemory[IO]
      result <- store.update("unknown-id", PaymentStatus.Pending)
    } yield assertEquals(result, None)
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
      deleted <- store.delete("unknown-id")
    } yield assert(!deleted)
  }

  private val authorization =
    Authorization("auth_1", Instant.parse("2026-11-03T12:00:00Z"))

  test("recordAuthorization stores an Authorized payment and one outbox event") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      payment <- store.recordAuthorization("order-1", 4999, "pm-1", authorization)
      found <- store.get(payment.id)
      events <- outbox.get
    } yield {
      assertEquals(payment.status, PaymentStatus.Authorized)
      assertEquals(payment.paymentMethodId, Some("pm-1"))
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

  test("recordDecline stores a Failed payment with its reason and no outbox event") {
    for {
      (store, outbox) <- PaymentStore.inMemoryWithOutbox[IO]
      payment <- store.recordDecline(
        "order-1",
        4999,
        "pm-1",
        DeclineReason("insufficient_funds")
      )
      found <- store.get(payment.id)
      events <- outbox.get
    } yield {
      assertEquals(payment.status, PaymentStatus.Failed)
      assertEquals(payment.paymentMethodId, Some("pm-1"))
      assertEquals(payment.declineReason, Some("insufficient_funds"))
      assertEquals(payment.authId, None)
      assertEquals(found, Some(payment))
      assertEquals(events, Nil)
    }
  }
}
