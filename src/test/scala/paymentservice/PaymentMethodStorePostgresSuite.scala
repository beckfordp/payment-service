package paymentservice

import cats.effect.IO
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import paymentservice.domain.PaymentMethodId

import java.util.UUID
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter

import java.sql.DriverManager

class PaymentMethodStorePostgresSuite
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

  private val card = Card("0005", CardBrand.Amex, "01/30")
  private val billing = Address("1 High St", None, "London", "SW1A 1AA", "GB")

  test("create then get round-trips every field, billing address included") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentMethodStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          for {
            created <- store.create("customer-1", card, billing)
            found <- store.get(created.id)
          } yield {
            assertEquals(found, Some(created))
            assertEquals(created.cardToken, "tok_amex_0005")
            assertEquals(created.billingAddress, billing)
          }
        }
    }
  }

  test("get returns None for an unknown id") {
    withContainers { postgres =>
      val config = configFor(postgres)
      Migrations.run[IO](config) *> PaymentMethodStore
        .postgres[IO](config, Meter.noop[IO])
        .use { store =>
          store
            .get(PaymentMethodId.fromUuid(UUID.randomUUID()))
            .map(assertEquals(_, None))
        }
    }
  }

  test("payment_methods has no column that could hold a PAN") {
    withContainers { postgres =>
      Migrations.run[IO](configFor(postgres)).map { _ =>
        val conn = DriverManager.getConnection(
          postgres.jdbcUrl,
          postgres.username,
          postgres.password
        )
        try {
          val rs = conn
            .createStatement()
            .executeQuery(
              "select column_name from information_schema.columns where table_name = 'payment_methods' order by ordinal_position"
            )
          val columns = Iterator
            .unfold(())(_ =>
              if (rs.next()) Some((rs.getString("column_name"), ())) else None
            )
            .toList
          assertEquals(
            columns,
            List(
              "id",
              "customer_id",
              "card_token",
              "last4",
              "brand",
              "expiry",
              "billing_address",
              "created_at"
            )
          )
        } finally conn.close()
      }
    }
  }
}
