package paymentservice

import cats.effect.IO
import io.circe.Json
import io.circe.syntax._
import munit.CatsEffectSuite
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Response, Status}
import org.typelevel.log4cats.noop.NoOpLogger

class PaymentMethodRoutesSuite extends CatsEffectSuite {

  private val billing =
    Address("1 High St", None, "London", "SW1A 1AA", "GB")

  private val validRequest = CreatePaymentMethodRequest(
    customerId = "customer-1",
    cardNumber = "4242 4242 4242 4242",
    expiry = "12/99",
    billingAddress = billing
  )

  private def post(
      store: PaymentMethodStore[IO],
      body: CreatePaymentMethodRequest
  ): IO[Response[IO]] =
    PaymentMethodRoutes
      .routes[IO](store, NoOpLogger[IO])
      .orNotFound
      .run(Request[IO](Method.POST, uri"/payments/methods").withEntity(body))

  test("POST /payments/methods returns 201 with id/last4/brand/expiry") {
    for {
      store <- PaymentMethodStore.inMemory[IO]
      response <- post(store, validRequest)
      body <- response.as[PaymentMethodResponse]
      stored <- store.get(body.id)
    } yield {
      assertEquals(response.status, Status.Created)
      assertEquals(body.last4, "4242")
      assertEquals(body.brand, "visa")
      assertEquals(body.expiry, "12/99")
      assertEquals(stored.map(_.cardToken), Some("tok_visa_4242"))
      assertEquals(stored.map(_.billingAddress), Some(billing))
    }
  }

  test("POST /payments/methods never returns the card token or PAN") {
    for {
      store <- PaymentMethodStore.inMemory[IO]
      response <- post(store, validRequest)
      json <- response.as[Json]
    } yield {
      assertEquals(
        json.asObject.map(_.keys.toSet),
        Some(Set("id", "last4", "brand", "expiry"))
      )
      assert(!json.noSpaces.contains("tok_"))
      assert(!json.noSpaces.contains("42424242"))
    }
  }

  private def assertBadRequest(
      label: String,
      request: CreatePaymentMethodRequest,
      expectedError: String
  ): Unit =
    test(s"POST /payments/methods returns 400 for $label") {
      for {
        store <- PaymentMethodStore.inMemory[IO]
        response <- post(store, request)
        body <- response.as[ErrorResponse]
      } yield {
        assertEquals(response.status, Status.BadRequest)
        assertEquals(body.error, expectedError)
      }
    }

  assertBadRequest(
    "an invalid card number",
    validRequest.copy(cardNumber = "4242424242424241"),
    "Invalid card number"
  )
  assertBadRequest(
    "a malformed expiry",
    validRequest.copy(expiry = "13/28"),
    "Invalid expiry, expected MM/YY"
  )
  assertBadRequest(
    "an expired card",
    validRequest.copy(expiry = "01/20"),
    "Card has expired"
  )
  assertBadRequest(
    "an invalid billing address",
    validRequest.copy(billingAddress = billing.copy(country = "gb")),
    "country must be an ISO 3166-1 alpha-2 code"
  )
  assertBadRequest(
    "a blank customerId",
    validRequest.copy(customerId = " "),
    "customerId is required"
  )

  test("POST /payments/methods stores nothing on a 400") {
    for {
      store <- PaymentMethodStore.inMemory[IO]
      recording = new PaymentMethodStore[IO] {
        def create(c: String, card: Card, a: Address): IO[PaymentMethod] =
          IO.raiseError(new AssertionError("create must not be called"))
        def get(id: String): IO[Option[PaymentMethod]] = store.get(id)
      }
      response <- post(recording, validRequest.copy(cardNumber = "1"))
    } yield assertEquals(response.status, Status.BadRequest)
  }

  test("POST /payments/methods returns 400 for a malformed body") {
    for {
      store <- PaymentMethodStore.inMemory[IO]
      response <- PaymentMethodRoutes
        .routes[IO](store, NoOpLogger[IO])
        .orNotFound
        .run(
          Request[IO](Method.POST, uri"/payments/methods")
            .withEntity(Json.obj("customerId" -> "c".asJson))
        )
    } yield assertEquals(response.status, Status.BadRequest)
  }
}
