package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite

class PaymentMethodStoreSuite extends CatsEffectSuite {

  private val card = Card("4242", CardBrand.Visa, "12/28")
  private val billing =
    Address("1 High St", Some("Flat 2"), "London", "SW1A 1AA", "GB")

  test("create returns a method carrying the card's token and details") {
    for {
      store <- PaymentMethodStore.inMemory[IO]
      method <- store.create("customer-1", card, billing)
    } yield {
      assert(method.id.nonEmpty)
      assertEquals(method.customerId, "customer-1")
      assertEquals(method.cardToken, "tok_visa_4242")
      assertEquals(method.last4, "4242")
      assertEquals(method.brand, CardBrand.Visa)
      assertEquals(method.expiry, "12/28")
      assertEquals(method.billingAddress, billing)
    }
  }

  test("get returns the created method") {
    for {
      store <- PaymentMethodStore.inMemory[IO]
      created <- store.create("customer-1", card, billing)
      found <- store.get(created.id)
    } yield assertEquals(found, Some(created))
  }

  test("get returns None for an unknown id") {
    PaymentMethodStore
      .inMemory[IO]
      .flatMap(_.get("unknown"))
      .assertEquals(None)
  }

  test("create produces distinct ids across calls") {
    for {
      store <- PaymentMethodStore.inMemory[IO]
      a <- store.create("customer-1", card, billing)
      b <- store.create("customer-1", card, billing)
    } yield assertNotEquals(a.id, b.id)
  }
}
