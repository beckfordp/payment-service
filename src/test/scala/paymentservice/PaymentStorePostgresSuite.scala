package paymentservice

import cats.effect.IO
import cats.syntax.all._
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter
import purerest.metrics.Metrics

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
          store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999).map {
            entity =>
              assert(entity.id.nonEmpty)
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
            first <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
            second <- store.create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
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
              "3fa85f64-5717-4562-b3fc-2c963f66afa6",
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
            .get(java.util.UUID.randomUUID().toString)
            .map(assertEquals(_, None))
        }
    }
  }

  test("get returns None for a malformed (non-UUID) id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store.get("not-a-uuid").map(assertEquals(_, None))
        }
    }
  }

  test("update returns the updated entity and returns it") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create(
              "3fa85f64-5717-4562-b3fc-2c963f66afa6",
              4999
            )
            updated <- store.update(created.id, PaymentStatus.Pending)
          } yield {
            assertEquals(updated.map(_.id), Some(created.id))
            assert(
              updated.exists(!_.updatedAt.isBefore(created.updatedAt)),
              s"expected updatedAt not to move backwards, got: $updated"
            )
          }
        }
    }
  }

  test("update returns None for an unknown id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .update(java.util.UUID.randomUUID().toString, PaymentStatus.Pending)
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
    "update to Settled writes no outbox row (no event until the authorize/capture producers land)"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create(
              "3fa85f64-5717-4562-b3fc-2c963f66afa6",
              4999
            )
            updated <- store.update(created.id, PaymentStatus.Settled)
          } yield {
            assertEquals(updated.map(_.status), Some(PaymentStatus.Settled))
            assertEquals(
              outboxRowsForOrder(
                postgres,
                "3fa85f64-5717-4562-b3fc-2c963f66afa6"
              ),
              Nil
            )
          }
        }
    }
  }

  test(
    "update to a non-Settled status writes no outbox row"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create(
              "3fa85f64-5717-4562-b3fc-2c963f66afa6",
              4999
            )
            _ <- store.update(created.id, PaymentStatus.Pending)
          } yield assertEquals(
            outboxRowsForOrder(
              postgres,
              "3fa85f64-5717-4562-b3fc-2c963f66afa6"
            ),
            Nil
          )
        }
    }
  }

  test(
    "update on an unknown id writes no outbox row (atomicity: no state change, no event)"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          val unknownId = java.util.UUID.randomUUID().toString
          store
            .update(unknownId, PaymentStatus.Settled)
            .map { result =>
              assertEquals(result, None)
              assertEquals(
                outboxRowsForOrder(postgres, unknownId),
                Nil
              )
            }
        }
    }
  }

  test("update returns None for a malformed (non-UUID) id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .update("not-a-uuid", PaymentStatus.Pending)
            .map(assertEquals(_, None))
        }
    }
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
              "3fa85f64-5717-4562-b3fc-2c963f66afa6",
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
            .delete(java.util.UUID.randomUUID().toString)
            .map(deleted => assert(!deleted))
        }
    }
  }

  test("delete returns false for a malformed (non-UUID) id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store.delete("not-a-uuid").map(deleted => assert(!deleted))
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
                "3fa85f64-5717-4562-b3fc-2c963f66afa6",
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
                .create("3fa85f64-5717-4562-b3fc-2c963f66afa6", 4999)
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
    "full CRUD lifecycle: create -> read -> patch -> delete -> read-404, plus a readiness check"
  ) {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            ready <- store.ping
            created <- store.create(
              "3fa85f64-5717-4562-b3fc-2c963f66afa6",
              4999
            )
            read1 <- store.get(created.id)
            updated <- store.update(created.id, PaymentStatus.Pending)
            read2 <- store.get(created.id)
            deleted <- store.delete(created.id)
            read3 <- store.get(created.id)
          } yield {
            assert(ready, "expected the database to be ready")
            assertEquals(read1, Some(created))
            assertEquals(
              updated.map(_.orderId),
              Some("3fa85f64-5717-4562-b3fc-2c963f66afa6")
            )
            assertEquals(updated.map(_.amountCents), Some(4999))
            assertEquals(updated.map(_.status), Some(PaymentStatus.Pending))
            assertEquals(read2, updated)
            assert(deleted, "expected delete to report the entity existed")
            assertEquals(read3, None)
          }
        }
    }
  }

  private val authorization =
    Authorization("auth_pg", Instant.parse("2026-11-03T12:00:00Z"))

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

  private def query(postgres: PostgreSQLContainer, sql: String): List[String] = {
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
          payment <- store.recordAuthorization(
            orderId,
            4999,
            method.id,
            authorization
          )
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
          payment <- store.recordDecline(
            orderId,
            4999,
            method.id,
            DeclineReason("insufficient_funds")
          )
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
}
