package paymentservice

import cats.effect.IO
import munit.CatsEffectSuite
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

  private def failingStore(error: Throwable): PaymentStore[IO] =
    new PaymentStore[IO] {
      def create(orderId: String, amountCents: Int): IO[Payment] =
        IO.raiseError(error)
      def get(id: String): IO[Option[Payment]] = IO.pure(None)
      def update(
          id: String,
          status: PaymentStatus
      ): IO[Option[Payment]] =
        IO.raiseError(error)
      def delete(id: String): IO[Boolean] = IO.raiseError(error)
      def ping: IO[Boolean] = IO.raiseError(error)
    }

  test("POST /payments returns 201 with the created entity") {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      request = Request[IO](Method.POST, uri"/payments")
        .withEntity(CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999))
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
        Request[IO](Method.GET, uri"/payments" / "unknown-id")
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
        .withEntity(CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999))
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
        .withEntity(CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999))
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
        Request[IO](Method.GET, uri"/payments" / "unknown-id")
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("payment_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test("PATCH /payments/{id} returns 200 with the updated entity") {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/payments").withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      )
      created <- postResponse.as[PaymentResponse]
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/payments" / created.id)
          .withEntity(UpdatePaymentRequest("pending"))
      )
      updated <- patchResponse.as[PaymentResponse]
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      assertEquals(updated.id, created.id)
    }
  }

  test(
    "PATCH /payments/{id} accepts \"settled\" and \"failed\" statuses"
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
      settledResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/payments" / created.id)
          .withEntity(UpdatePaymentRequest("settled"))
      )
      settled <- settledResponse.as[PaymentResponse]
      failedResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/payments" / created.id)
          .withEntity(UpdatePaymentRequest("failed"))
      )
      failed <- failedResponse.as[PaymentResponse]
    } yield {
      assertEquals(settledResponse.status, Status.Ok)
      assertEquals(settled.status, "settled")
      assertEquals(failedResponse.status, Status.Ok)
      assertEquals(failed.status, "failed")
    }
  }

  test(
    "PATCH /payments/{id} returns 400 with a JSON error body for an invalid status, and persists nothing"
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
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/payments" / created.id)
          .withEntity(UpdatePaymentRequest("bogus"))
      )
      body <- patchResponse.as[io.circe.Json]
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/payments" / created.id)
      )
      unchanged <- getResponse.as[PaymentResponse]
    } yield {
      assertEquals(patchResponse.status, Status.BadRequest)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
      assertEquals(unchanged.status, "pending")
    }
  }

  test(
    "PATCH /payments/{id} logs a WARN for an invalid status"
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
      _ <- testLogger.logged // drain POST's own log lines before the PATCH
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/payments" / created.id)
          .withEntity(UpdatePaymentRequest("bogus"))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(patchResponse.status, Status.BadRequest)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m => m.message.toLowerCase.contains("invalid status")),
        s"expected an 'invalid status' WARN line, got: $warns"
      )
    }
  }

  test(
    "PATCH /payments/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/payments" / "unknown-id")
          .withEntity(UpdatePaymentRequest("pending"))
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
    "PATCH /payments/{id} logs a received-request line and a completed line for a found entity"
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
      _ <- testLogger.logged // drain POST's own log lines before the PATCH
      patchResponse <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/payments" / created.id)
          .withEntity(UpdatePaymentRequest("pending"))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(patchResponse.status, Status.Ok)
      val infos = logged.collect { case m: INFO => m }
      assert(
        infos.exists(m =>
          m.message.toLowerCase.contains("received") &&
            m.ctx.get("method").contains("PATCH") &&
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

  test("PATCH /payments/{id} logs a WARN for an unknown id") {
    for {
      store <- PaymentStore.inMemory[IO]
      testLogger = StructuredTestingLogger.impl[IO]()
      routes = PaymentRoutes.routes[IO](store, testLogger)
      response <- routes.orNotFound.run(
        Request[IO](Method.PATCH, uri"/payments" / "unknown-id")
          .withEntity(UpdatePaymentRequest("pending"))
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("payment_id").contains("unknown-id")
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
        Request[IO](Method.DELETE, uri"/payments" / "unknown-id")
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
        Request[IO](Method.DELETE, uri"/payments" / "unknown-id")
      )
      logged <- testLogger.logged
    } yield {
      assertEquals(response.status, Status.NotFound)
      val warns = logged.collect { case m: WARN => m }
      assert(
        warns.exists(m =>
          m.message.toLowerCase.contains("not found") &&
            m.ctx.get("payment_id").contains("unknown-id")
        ),
        s"expected a 'not found' WARN line with id context, got: $warns"
      )
    }
  }

  test("PUT /payments/{id} returns 200 with the replaced entity") {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      postResponse <- routes.orNotFound.run(
        Request[IO](Method.POST, uri"/payments").withEntity(
          CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
        )
      )
      created <- postResponse.as[PaymentResponse]
      putResponse <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/payments" / created.id)
          .withEntity(UpdatePaymentRequest("pending"))
      )
      replaced <- putResponse.as[PaymentResponse]
    } yield {
      assertEquals(putResponse.status, Status.Ok)
      assertEquals(replaced.id, created.id)
    }
  }

  test(
    "PUT /payments/{id} returns 404 with a JSON error body for an unknown id"
  ) {
    for {
      store <- PaymentStore.inMemory[IO]
      routes = PaymentRoutes.routes[IO](store, NoOpLogger[IO])
      response <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/payments" / "unknown-id")
          .withEntity(UpdatePaymentRequest("pending"))
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
    "PUT /payments/{id} returns 400 with a JSON error body for an invalid status, and persists nothing"
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
      putResponse <- routes.orNotFound.run(
        Request[IO](Method.PUT, uri"/payments" / created.id)
          .withEntity(UpdatePaymentRequest("bogus"))
      )
      body <- putResponse.as[io.circe.Json]
      getResponse <- routes.orNotFound.run(
        Request[IO](Method.GET, uri"/payments" / created.id)
      )
      unchanged <- getResponse.as[PaymentResponse]
    } yield {
      assertEquals(putResponse.status, Status.BadRequest)
      assert(
        body.asObject.exists(_.contains("error")),
        s"expected a JSON error body, got: $body"
      )
      assertEquals(unchanged.status, "pending")
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
          .withEntity(CreatePaymentRequest("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999))
        response <- routes.orNotFound.run(request)
        spans <- testTracer.finishedSpans
      } yield {
        assertEquals(response.status, Status.Created)
        assertEquals(spans.map(_.getName), List("POST /payments"))
      }
    }
  }
}
