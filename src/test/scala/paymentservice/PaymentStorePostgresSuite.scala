package paymentservice

import paymentservice.TestConversions.given
import scala.language.implicitConversions
import cats.effect.IO
import cats.syntax.all._
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import paymentservice.domain.{OrderId, PaymentId, PaymentMethodId}

import java.util.UUID
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter
import purerest.metrics.{LatencyBuckets, Metrics}

import java.sql.DriverManager
import java.time.Instant
import scala.jdk.CollectionConverters._

class PaymentStorePostgresSuite
    extends CatsEffectSuite
    with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(dockerImageName =
      DockerImageName.parse("postgres:16-alpine")
    )

  private def configFor(postgres: PostgreSQLContainer): PostgresConfig =
    PostgresConfig(
      host = postgres.host,
      port = postgres.mappedPort(5432),
      database = postgres.databaseName,
      user = postgres.username,
      password = postgres.password
    )

  test("create persists an entity and returns it with a generated id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store.create("order-create", 4999).map { entity =>
            assert(PaymentId.render(entity.id).nonEmpty)
          }
        }
    }
  }

  test("create produces distinct ids across calls") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations
        .run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            first <- store.create("order-1", 4999)
            second <- store.create("order-2", 4999)
          } yield assertNotEquals(first.id, second.id)
        }
    }
  }

  test("get returns the persisted entity") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create(
              "order-get",
              4999
            )
            found <- store.get(created.id)
          } yield assertEquals(found, Some(created))
        }
    }
  }

  test("get returns None for an unknown id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .get(PaymentId.fromUuid(UUID.randomUUID()))
            .map(assertEquals(_, None))
        }
    }
  }

  /** `TestContainerForAll` shares one container across every test in this
    * suite, and every test here reuses the same hardcoded `orderId` — so
    * filtering by `paymentId` (embedded in each row's own payload, unique per
    * `store.create` call) is what actually isolates one test's rows from
    * another's, not a global "the table is empty" assertion.
    */
  /** Rows in `outbox_events` whose `aggregate_id` equals `orderId` — the
    * outbox's aggregate/correlation key for payment events.
    */
  private def outboxRowsForOrder(
      postgres: PostgreSQLContainer,
      orderId: String
  ): List[String] = {
    val conn = DriverManager.getConnection(
      postgres.jdbcUrl,
      postgres.username,
      postgres.password
    )
    try {
      val rs = conn
        .createStatement()
        .executeQuery("select aggregate_id from outbox_events")
      Iterator
        .unfold(())(_ =>
          if (rs.next()) Some((rs.getString("aggregate_id"), ())) else None
        )
        .toList
        .filter(_ == orderId)
    } finally conn.close()
  }

  test(
    "delete removes the entity and returns true, and get then returns None"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create(
              "order-delete",
              4999
            )
            deleted <- store.delete(created.id)
            found <- store.get(created.id)
          } yield {
            assert(deleted)
            assertEquals(found, None)
          }
        }
    }
  }

  test("delete returns false for an unknown id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .delete(PaymentId.fromUuid(UUID.randomUUID()))
            .map(deleted => assert(!deleted))
        }
    }
  }

  test("ping returns true against a real, reachable database") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store.ping.map(assert(_))
        }
    }
  }

  test("ping returns false when the database is unreachable") {
    withContainers { postgres =>
      val unreachableConfig = configFor(postgres).copy(port = 1)
      PaymentStore
        .postgres[IO](unreachableConfig, Meter.noop[IO])
        .use { store =>
          store.ping.map(ready => assert(!ready))
        }
    }
  }

  test(
    "create and get each record a db.client.operation.duration measurement, tagged by operation"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Metrics.test[IO]("payment-store-postgres-metrics-test").use { testMeter =>
        Migrations.run[IO](config) *> PaymentStore
          .postgres[IO](config, testMeter.meter)
          .use { store =>
            for {
              created <- store.create(
                "order-metrics",
                4999
              )
              _ <- store.get(created.id)
              metrics <- testMeter.collectMetrics
            } yield {
              val data =
                metrics.find(_.getName == "db.client.operation.duration")
              assert(
                data.isDefined,
                s"expected a db.client.operation.duration series, got: $metrics"
              )
              data.get.getHistogramData.getPoints.asScala.foreach { point =>
                assertEquals(
                  point.getBoundaries.asScala.map(_.doubleValue).toVector,
                  LatencyBuckets.seconds.boundaries
                )
              }
              val dbOperationKey =
                io.opentelemetry.api.common.AttributeKey
                  .stringKey("db.operation")
              val operations = data.get.getHistogramData.getPoints.asScala
                .flatMap(point =>
                  Option(point.getAttributes.get(dbOperationKey))
                )
                .toSet
              assert(
                operations
                  .contains("insert") && operations.contains("select"),
                s"expected db.operation attributes for both insert and select, got: $operations"
              )
            }
          }
      }
    }
  }

  test(
    "a failing query records a db.client.operation.duration measurement tagged with error.type"
  ) {
    withContainers { postgres =>
      // Port 1 is a privileged port nothing binds to in these tests; unlike
      // `mappedPort(5432) + 1`, it can't collide with another concurrently-running
      // Testcontainers Postgres instance's dynamically assigned port.
      val unreachableConfig = configFor(postgres).copy(port = 1)
      Metrics.test[IO]("payment-store-postgres-metrics-test").use { testMeter =>
        PaymentStore
          .postgres[IO](unreachableConfig, testMeter.meter)
          .use { store =>
            for {
              result <- store
                .create("order-unreachable", 4999)
                .attempt
              metrics <- testMeter.collectMetrics
            } yield {
              assert(
                result.isLeft,
                s"expected the connection failure to propagate, got: $result"
              )
              val data =
                metrics.find(_.getName == "db.client.operation.duration")
              assert(
                data.isDefined,
                s"expected a db.client.operation.duration series, got: $metrics"
              )
              val errorTypeKey =
                io.opentelemetry.api.common.AttributeKey.stringKey(
                  "error.type"
                )
              val hasErrorAttribute =
                data.get.getHistogramData.getPoints.asScala
                  .exists(point =>
                    Option(point.getAttributes.get(errorTypeKey)).isDefined
                  )
              assert(
                hasErrorAttribute,
                s"expected a point tagged with error.type, got: ${data.get.getHistogramData.getPoints}"
              )
            }
          }
      }
    }
  }

  test(
    "full CRUD lifecycle: create -> read -> delete -> read-404, plus a readiness check"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            ready <- store.ping
            created <- store.create(
              "order-crud",
              4999
            )
            read1 <- store.get(created.id)
            deleted <- store.delete(created.id)
            read3 <- store.get(created.id)
          } yield {
            assert(ready, "expected the database to be ready")
            assertEquals(read1, Some(created))
            assertEquals(created.status, PaymentStatus.Pending)
            assert(deleted, "expected delete to report the entity existed")
            assertEquals(read3, None)
          }
        }
    }
  }

  private val authorization =
    Authorization("auth_pg", Instant.parse("2099-11-03T12:00:00Z"))

  private def withStores[A](postgres: PostgreSQLContainer)(
      f: (PaymentStore[IO], PaymentMethod) => IO[A]
  ): IO[A] = {
    val config = configFor(postgres)
    Migrations.run[IO](config) *> (
      PaymentStore.postgres[IO](config, Meter.noop[IO]),
      PaymentMethodStore.postgres[IO](config, Meter.noop[IO])
    ).tupled.use { case (store, methods) =>
      methods
        .create(
          "customer-1",
          Card("4242", CardBrand.Visa, "12/99"),
          Address("1 High St", None, "London", "SW1A 1AA", "GB")
        )
        .flatMap(method => f(store, method))
    }
  }

  private def query(
      postgres: PostgreSQLContainer,
      sql: String
  ): List[String] = {
    val conn = DriverManager.getConnection(
      postgres.jdbcUrl,
      postgres.username,
      postgres.password
    )
    try {
      val rs = conn.createStatement().executeQuery(sql)
      Iterator
        .unfold(())(_ => if (rs.next()) Some((rs.getString(1), ())) else None)
        .toList
    } finally conn.close()
  }

  test(
    "recordAuthorization persists an Authorized payment and its PaymentAuthorized outbox row"
  ) {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          payment <- store
            .recordAuthorization(
              orderId,
              4999,
              method.id,
              authorization
            )
            .map(_.getOrElse(fail("expected the authorization to be inserted")))
          found <- store.get(payment.id)
        } yield {
          assertEquals(payment.status, PaymentStatus.Authorized)
          assertEquals(payment.paymentMethodId, Some(method.id))
          assertEquals(payment.authId, Some("auth_pg"))
          assertEquals(payment.expiresAt, Some(authorization.expiresAt))
          assertEquals(found, Some(payment))
          assertEquals(
            query(
              postgres,
              s"select aggregate_id || '|' || correlation_id || '|' || (payload->>'eventType') || '|' || (payload->>'source') || '|' || (payload->'payload'->>'authId') || '|' || (payload->'payload'->>'orderId') from outbox_events where aggregate_id = '$orderId'"
            ),
            List(
              s"$orderId|$orderId|PaymentAuthorized|payment-service|auth_pg|$orderId"
            )
          )
        }
      }
    }
  }

  test("recordDecline persists a Failed payment and writes no outbox row") {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          payment <- store
            .recordDecline(
              orderId,
              4999,
              method.id,
              DeclineReason("insufficient_funds")
            )
            .map(_.getOrElse(fail("expected the decline to be inserted")))
          found <- store.get(payment.id)
        } yield {
          assertEquals(payment.status, PaymentStatus.Failed)
          assertEquals(payment.declineReason, Some("insufficient_funds"))
          assertEquals(found, Some(payment))
          assertEquals(
            query(
              postgres,
              s"select id from outbox_events where aggregate_id = '$orderId'"
            ),
            Nil
          )
        }
      }
    }
  }

  test(
    "recordAuthorization rolls back the payment row when the outbox write fails"
  ) {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          // Break only the outbox insert: a CHECK no envelope can satisfy.
          _ <- IO.blocking {
            val conn = DriverManager.getConnection(
              postgres.jdbcUrl,
              postgres.username,
              postgres.password
            )
            try
              conn
                .createStatement()
                .execute(
                  "alter table outbox_events add constraint reject_all check (false) not valid"
                )
            finally conn.close()
          }
          result <- store
            .recordAuthorization(orderId, 4999, method.id, authorization)
            .attempt
          _ <- IO.blocking {
            val conn = DriverManager.getConnection(
              postgres.jdbcUrl,
              postgres.username,
              postgres.password
            )
            try
              conn
                .createStatement()
                .execute("alter table outbox_events drop constraint reject_all")
            finally conn.close()
          }
        } yield {
          assert(result.isLeft)
          assertEquals(
            query(
              postgres,
              s"select id from payment where order_id = '$orderId'"
            ),
            Nil
          )
        }
      }
    }
  }

  test("findByOrderId returns the stored row and None for an unknown order") {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          _ <- store
            .recordAuthorization(orderId, 4999, method.id, authorization)
            .map(_.getOrElse(fail("expected the authorization to be inserted")))
          found <- store.findByOrderId(orderId)
          missing <- store.findByOrderId("order-unknown")
        } yield {
          assertEquals(found.map(_.orderId), Some(OrderId.trusted(orderId)))
          assertEquals(found.map(_.status), Some(PaymentStatus.Authorized))
          assertEquals(missing, None)
        }
      }
    }
  }

  test(
    "a repeated recordAuthorization for the same order returns None, one row and one outbox event"
  ) {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          first <- store
            .recordAuthorization(orderId, 4999, method.id, authorization)
            .map(_.getOrElse(fail("expected the first insert")))
          second <- store.recordAuthorization(
            orderId,
            4999,
            method.id,
            authorization
          )
          found <- store.findByOrderId(orderId)
        } yield {
          assertEquals(second, None)
          assertEquals(found.map(_.id), Some(first.id))
          assertEquals(
            query(
              postgres,
              s"select id from payment where order_id = '$orderId'"
            ).size,
            1
          )
          assertEquals(
            query(
              postgres,
              s"select id from outbox_events where aggregate_id = '$orderId'"
            ).size,
            1
          )
        }
      }
    }
  }

  test(
    "recordCapture transitions Authorized to Captured and writes its PaymentCaptured outbox row"
  ) {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          _ <- store
            .recordAuthorization(orderId, 4999, method.id, authorization)
            .map(_.getOrElse(fail("expected the authorization to be inserted")))
          captured <- store
            .recordCapture(orderId)
            .map(_.getOrElse(fail("expected the capture to be recorded")))
          found <- store.findByOrderId(orderId)
        } yield {
          assertEquals(captured.status, PaymentStatus.Captured)
          assertEquals(found.map(_.status), Some(PaymentStatus.Captured))
          assertEquals(
            query(
              postgres,
              s"select aggregate_id || '|' || correlation_id || '|' || (payload->>'eventType') || '|' || (payload->>'source') || '|' || (payload->'payload'->>'orderId') || '|' || (payload->'payload'->>'amountCents') from outbox_events where aggregate_id = '$orderId' and payload->>'eventType' = 'PaymentCaptured'"
            ),
            List(
              s"$orderId|$orderId|PaymentCaptured|payment-service|$orderId|4999"
            )
          )
        }
      }
    }
  }

  test(
    "a repeated recordCapture returns None and leaves exactly one PaymentCaptured outbox row"
  ) {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          _ <- store
            .recordAuthorization(orderId, 4999, method.id, authorization)
            .map(_.getOrElse(fail("expected the authorization to be inserted")))
          _ <- store
            .recordCapture(orderId)
            .map(_.getOrElse(fail("expected the first capture")))
          second <- store.recordCapture(orderId)
        } yield {
          assertEquals(second, None)
          assertEquals(
            query(
              postgres,
              s"select id from outbox_events where aggregate_id = '$orderId' and payload->>'eventType' = 'PaymentCaptured'"
            ).size,
            1
          )
        }
      }
    }
  }

  test(
    "recordVoid marks an Authorized payment Voided and writes no outbox row"
  ) {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          authorized <- store
            .recordAuthorization(orderId, 4999, method.id, authorization)
            .map(_.getOrElse(fail("expected the authorization")))
          voided <- store.recordVoid(orderId)
          again <- store.recordVoid(orderId)
          captured <- store.recordCapture(orderId)
        } yield {
          assertEquals(voided.map(_.id), Some(authorized.id))
          assertEquals(voided.map(_.status), Some(PaymentStatus.Voided))
          assertEquals(again, None)
          assertEquals(captured, None)
          assertEquals(
            query(
              postgres,
              s"select payload->>'eventType' from outbox_events where aggregate_id = '$orderId'"
            ),
            List("PaymentAuthorized")
          )
        }
      }
    }
  }

  test("recordVoid returns None for a non-authorized payment, unchanged") {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create(orderId, 4999)
            result <- store.recordVoid(orderId)
            after <- store.get(created.id)
          } yield {
            assertEquals(result, None)
            assertEquals(after.map(_.status), Some(PaymentStatus.Pending))
          }
        }
    }
  }

  test("recordCapture returns None for a non-authorized payment") {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            _ <- store.create(orderId, 4999)
            result <- store.recordCapture(orderId)
          } yield {
            assertEquals(result, None)
            assertEquals(
              query(
                postgres,
                s"select id from outbox_events where aggregate_id = '$orderId'"
              ),
              Nil
            )
          }
        }
    }
  }

  test(
    "recordCaptureFailure marks the payment Failed and writes its PaymentFailed outbox row"
  ) {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          authorized <- store
            .recordAuthorization(orderId, 4999, method.id, authorization)
            .map(_.getOrElse(fail("expected the authorization to be inserted")))
          failed <- store
            .recordCaptureFailure(orderId, "capture_failed")
            .map(
              _.getOrElse(fail("expected the capture failure to be recorded"))
            )
        } yield {
          assertEquals(failed.id, authorized.id)
          assertEquals(failed.status, PaymentStatus.Failed)
          assertEquals(failed.declineReason, Some("capture_failed"))
          assertEquals(
            query(
              postgres,
              s"select (payload->'payload'->>'paymentId') || '|' || (payload->'payload'->>'amountCents') || '|' || (payload->'payload'->>'reason') || '|' || (payload->>'eventType') from outbox_events where aggregate_id = '$orderId' and payload->>'eventType' = 'PaymentFailed'"
            ),
            List(s"${failed.id}|4999|capture_failed|PaymentFailed")
          )
        }
      }
    }
  }

  test(
    "recordCapture rolls back the status change when the outbox write fails"
  ) {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          _ <- store
            .recordAuthorization(orderId, 4999, method.id, authorization)
            .map(_.getOrElse(fail("expected the authorization to be inserted")))
          // Break only the outbox insert: a CHECK no envelope can satisfy.
          _ <- IO.blocking {
            val conn = DriverManager.getConnection(
              postgres.jdbcUrl,
              postgres.username,
              postgres.password
            )
            try
              conn
                .createStatement()
                .execute(
                  "alter table outbox_events add constraint reject_all check (false) not valid"
                )
            finally conn.close()
          }
          result <- store.recordCapture(orderId).attempt
          _ <- IO.blocking {
            val conn = DriverManager.getConnection(
              postgres.jdbcUrl,
              postgres.username,
              postgres.password
            )
            try
              conn
                .createStatement()
                .execute("alter table outbox_events drop constraint reject_all")
            finally conn.close()
          }
        } yield {
          assert(result.isLeft)
          assertEquals(
            query(
              postgres,
              s"select status from payment where order_id = '$orderId'"
            ),
            List("authorized")
          )
        }
      }
    }
  }

  test("create rejects a second payment for the same order") {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            _ <- store.create(orderId, 4999)
            result <- store.create(orderId, 4999).attempt
          } yield assertEquals(result, Left(PaymentAlreadyExists(orderId)))
        }
    }
  }

  test("recordCapture leaves an expired authorization untouched") {
    withContainers { postgres =>
      val orderId = java.util.UUID.randomUUID().toString
      withStores(postgres) { (store, method) =>
        for {
          _ <- store.recordAuthorization(
            orderId,
            4999,
            method.id,
            Authorization("auth-x", Instant.parse("2020-01-01T00:00:00Z"))
          )
          result <- store.recordCapture(orderId)
          after <- store.findByOrderId(orderId)
        } yield {
          assertEquals(result, None)
          assertEquals(after.map(_.status), Some(PaymentStatus.Authorized))
        }
      }
    }
  }
}
