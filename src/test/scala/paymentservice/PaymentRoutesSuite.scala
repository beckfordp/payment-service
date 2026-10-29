package paymentservice

import cats.effect.IO
import cats.syntax.all._
import munit.CatsEffectSuite
import paymentservice.domain.{PaymentId, PaymentMethodId}
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.{Method, Request, Status}
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger
import org.typelevel.log4cats.testing.StructuredTestingLogger.{
  ERROR,
  INFO,
  WARN
}
import purerest.tracing.{ServerTracing, Tracing}

class PaymentRoutesSuite extends CatsEffectSuite {

  private val unknownPaymentId = java.util.UUID.randomUUID().toString

  private def failingStore(error: Throwable): PaymentStore[IO] =
    new PaymentStore[IO] {
      def create(orderId: String, amountCents: Int): IO[Payment] =
        IO.raiseError(error)
      def get(id: PaymentId): IO[Option[Payment]] = IO.pure(None)
      def recordVoid(orderId: String): IO[Option[Payment]] =
        IO.raiseError(error)
      def delete(id: PaymentId): IO[Boolean] = IO.raiseError(error)
      def ping: IO[Boolean] = IO.raiseError(error)
      def findByOrderId(orderId: String): IO[Option[Payment]] =
        IO.raiseError(error)
      def recordAuthorization(
          orderId: String,
          amountCents: Int,
          paymentMethodId: PaymentMethodId,
          authorization: Authorization
      ): IO[Option[Payment]] = IO.raiseError(new NotImplementedError)
      def recordDecline(
          orderId: String,
          amountCents: Int,
          paymentMethodId: PaymentMethodId,
          reason: DeclineReason
      ): IO[Option[Payment]] = IO.raiseError(new NotImplementedError)
      def recordCapture(orderId: String): IO[Option[Payment]] =
        IO.raiseError(error)
      def recordCaptureFailure(
          orderId: String,
          reason: String
      ): IO[Option[Payment]] = IO.raiseError(error)
    }

  test("POST /payments returns 201 with the created entity") {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      request = Request[IO](Method.POST, uri"/payments")
        .withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      response <- routes.orNotFound.run(request)
      entity <- response.as[PaymentResponse]
    } yield {
      assertEquals(response.status, Status.Created)
      assert(entity.id.nonEmpty)
    }
  }

  test("GET /payments/{id} returns 200 with the persisted entity") {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/payments").withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      )
      created <- postResponse.as[PaymentResponse]
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/payments" / created.id)
      )
      fetched <- getResponse.as[PaymentResponse]
    } yield {
      assertEquals(getResponse.status, Status.Ok)
      assertEquals(fetched, created)
    }
  }

  test(
    "GET /payments/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/payments" / unknownPaymentId)
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "POST /payments logs a received-request line and a completed line with structured context"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = PaymentRoutes.routes[IO](store, testLogger)
      request = Request[IO](Method.POST, uri"/payments")
        .withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      response <- routes.orNotFound.run(request)
      entity <- response.as[PaymentResponse]
      logged <- testLogger.logged
    } yield {
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("POST")
        ),
        s"expected a received-request INFO line with method context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("payment_id").contains(entity.id)
        ),
        s"expected a completed INFO line with payment_id context, got: $infos"
      )
    }
  }

  test(
    "POST /payments logs an ERROR with the raised throwable when persisting the entity fails"
  ) {
    val boom = new RuntimeException("boom")
    for {
      testLogger <- IO.pure(StructuredTestingLogger.impl[IO]())
      routes = PaymentRoutes.routes[IO](failingStore(boom), testLogger)
      request = Request[IO](Method.POST, uri"/payments")
        .withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      response <- routes.orNotFound.run(request)
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.InternalServerError)
      val errors = logged.collect { case m: ERROR => m }
      assert(
        errors.exists(m => m.throwOpt.contains(boom)),
        s"expected an ERROR line with the raised throwable, got: $errors"
      )
    }
  }

  test("PATCH and PUT /payments/{id} are gone - status is server-owned") {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      created <- store.create("order-1", 4999)
      responses <- List(Method.PATCH, Method.PUT).traverse { method =>
        routes.orNotFound.run(
          Request[IO](method, uri"/payments" / PaymentId.render(created.id))
            .withEntity(
              io.circe.Json.obj(
                "status" -> io.circe.Json.fromString("captured")
              )
            )
        )
      }
      after <- store.get(created.id)
    } yield {
      responses.foreach { response =>
        assert(
          Set(Status.NotFound, Status.MethodNotAllowed)(response.status),
          s"expected 404/405, got ${response.status}"
        )
      }
      assertEquals(after.map(_.status), Some(PaymentStatus.Pending))
    }
  }

  test("GET /payments/{id} returns 400 for a malformed (non-UUID) id") {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/payments" / "not-a-uuid")
      )
    } yield assertEquals(response.status, Status.BadRequest)
  }

  test(
    "GET /payments/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = PaymentRoutes.routes[IO](store, testLogger)
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/payments").withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      )
      created <- postResponse.as[PaymentResponse]
      _ <- testLogger.logged // drain POST's own log lines before the GET
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/payments" / created.id)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(getResponse.status, Status.Ok)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("GET") &&
            m.ctx.get("payment_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("payment_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test(
    "GET /payments/{id} logs a WARN for an unknown id"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = PaymentRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/payments" / unknownPaymentId)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("payment_id").contains(unknownPaymentId)
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test(
    "DELETE /payments/{id} returns 204, and a subsequent GET returns 404"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/payments").withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      )
      created <- postResponse.as[PaymentResponse]
      deleteResponse <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/payments" / created.id)
      )
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/payments" / created.id)
      )
    } yield {
      assertEquals(deleteResponse.status, Status.NoContent)
      assertEquals(getResponse.status, Status.NotFound)
    }
  }

  test(
    "DELETE /payments/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/payments" / unknownPaymentId)
      )
      body <- response.as[io.circe.Json]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
    }
  }

  test(
    "DELETE /payments/{id} logs a received-request line and a completed line for a found entity"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = PaymentRoutes.routes[IO](store, testLogger)
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/payments").withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      )
      created <- postResponse.as[PaymentResponse]
      _ <- testLogger.logged // drain POST's own log lines before the DELETE
      deleteResponse <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/payments" / created.id)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(deleteResponse.status, Status.NoContent)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("DELETE") &&
            m.ctx.get("payment_id").contains(created.id)
        ),
        s"expected a received-request INFO line with method/id context, got: $infos"
      )
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("completed") &&
            m.ctx.get("payment_id").contains(created.id)
        ),
        s"expected a completed INFO line with id context, got: $infos"
      )
    }
  }

  test("DELETE /payments/{id} logs a WARN for an unknown id") {
    for {
      store <- PaymentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = PaymentRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.DELETE, uri"/payments" / unknownPaymentId)
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("payment_id").contains(unknownPaymentId)
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test(
    "wrapped routes (with tracing middleware) record a span for a handled request"
  ) {
    Tracing.test[IO]("payment-service-test").use { testTracer =>
      for {
        store <- PaymentStore.inMemory[IO]
        routes = ServerTracing.middleware(testTracer.tracer)(
          PaymentRoutes.routes[IO](store, NoOpLogger[IO])
        )
        request = Request[IO](Method.POST, uri"/payments")
          .withEntity(
            CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
          )
        response <- routes.orNotFound.run(request)
        spans <- testTracer.finishedSpans
      } yield {
        assertEquals(response.status, Status.Created)
        assertEquals(spans.map(_.getName), List("POST /payments"))
      }
    }
  }
}
