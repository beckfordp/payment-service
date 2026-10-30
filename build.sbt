val scala3Version = "3.9.0"

val catsEffectVersion = "3.7.0"
val http4sVersion = "0.23.37"
val circeVersion = "0.14.16"
val munitVersion = "1.3.6"
val munitCatsEffectVersion = "2.2.1"
val munitScalacheckVersion = "1.3.1"
val log4catsVersion = "2.8.0"
val tapirVersion = "1.11.25"
val skunkVersion = "1.0.0"
val flywayVersion = "11.8.2"
val postgresqlJdbcVersion = "42.7.13"
val pureconfigVersion = "0.17.10"
val testcontainersScalaVersion = "0.43.6"
// purekafka: Kafka resilience/observability combinators + AsyncAPI docs
// support (ADR 0010). Pinned to match order-service's/inventory-service's
// own version (0.3.0 adds purekafka.docs.SchemaRegistryWriter, Rework 5
// step 3/ADR 0003).
val purekafkaVersion = "0.3.0"
// Pinned to match the purerestlib version this service is built against — see
// README's "Consuming purerest as a dependency" section.
val purerestlibVersion = "0.4.0"

// Skunk 1.0.0 depends on otel4s-core/-core-common/-core-metrics 0.16.0 (its own
// optional tracing integration), which conflicts with purerestlib's otel4s 1.1.0
// (an early-semver 0.x -> 1.x jump, which sbt's version-scheme conflict detection
// treats as an error by default). "Highest version wins" is the correct
// resolution here — nothing in this project invokes Skunk's otel4s integration —
// so these three coordinates are pinned explicitly rather than left to eviction.
// Mirrors purerest's own build.sbt (see its "Transitive version drift" note).
ThisBuild / dependencyOverrides ++= Seq(
  "org.typelevel" %% "otel4s-core" % "1.1.0",
  "org.typelevel" %% "otel4s-core-common" % "1.1.0",
  "org.typelevel" %% "otel4s-core-metrics" % "1.1.0"
)

lazy val root = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin, PureKafkaDocsPlugin)
  .settings(
    name := "payment-service",
    scalaVersion := scala3Version,
    version := "0.1.0",
    // Drives the sbt-purekafka-docs plugin's asyncApiGenerateSpec/
    // asyncApiGenerate tasks, auto-chained after Compile/compile - see
    // AsyncApiChannels.scala (paymentservice.AsyncApiChannels, an
    // AsyncApiChannelsProvider). Title/version match Main.scala's own
    // AsyncApiDocs.routes[IO]("Payment Service", "1.0", ...) call for the
    // live /docs/asyncapi.json route - otherwise the build-time generated
    // docs default to name.value/version.value
    // ("payment-service"/"0.1.0") and silently disagree with what's
    // actually served live.
    asyncApiChannelsClass := "paymentservice.AsyncApiChannels",
    asyncApiTitle := "Payment Service",
    asyncApiVersion := "1.0",
    // Drives schemaRegistryCheck (Rework 5 step 3, ADR 0003) - reuses the
    // same AsyncApiChannels registrations above. Env-var override (not a
    // hardcoded default) because local dev and CI point at two different,
    // unrelated Apicurio instances: locally, the shared OrbStack cluster
    // (default here); in CI, a throwaway `docker run` container with no
    // OrbStack/cluster-DNS access (see ci.yml).
    schemaRegistryUrl := sys.env.getOrElse(
      "SCHEMA_REGISTRY_URL",
      "http://apicurio-registry.gluon-local.svc.cluster.local:8080"
    ),
    // One shared group across every service (not the plugin's own
    // per-service default, name.value) - confirmed live 2026-10-25 that
    // "one group per service" meant a producer's and a consumer's
    // registration of the *same* real topic/eventType contract (e.g.
    // inventory-service producing inventory-events-StockReserved,
    // order-service consuming it) landed in two totally independent
    // groups/version-lineages, so the BACKWARD compatibility check never
    // actually compared them against each other - exactly the
    // producer/consumer mismatch ADR 0003 exists to catch. A shared
    // group means artifactId (<topic>-<eventType>, already globally
    // unique) is the only key that matters, so whichever side registers
    // second is checked against whichever registered first, regardless
    // of which service that is.
    schemaRegistryGroupId := "gluon",
    // purerestlib resolved as an ordinary published artifact from GitHub
    // Packages, not a source/`.dependsOn` link back to the purerest repo —
    // the same real-external-consumer proof purerest's own smoke-test/
    // module established. Requires GITHUB_ACTOR/GITHUB_TOKEN (a PAT with
    // read:packages scope) in the environment.
    resolvers += "GitHub Packages" at "https://maven.pkg.github.com/beckfordp/purerest",
    credentials += Credentials(
      "GitHub Package Registry",
      "maven.pkg.github.com",
      sys.env.getOrElse("GITHUB_ACTOR", ""),
      sys.env.getOrElse("GITHUB_TOKEN", "")
    ),
    // Docker image for local dev via `docker compose up` — see docker-compose.yml.
    Docker / packageName := "payment-service",
    dockerBaseImage := "eclipse-temurin:21-jre",
    dockerUpdateLatest := true,
    dockerExposedPorts := Seq(8080, 9090),
    Universal / javaOptions += "-Dlogback.configurationFile=logback-docker.xml",
    libraryDependencies ++= Seq(
      "io.github.beckfordp" %% "purerestlib" % purerestlibVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      // purekafka: AsyncAPI docs types (purekafka.docs) for this service's
      // outbox/CDC publish contracts, plus the DLQ/supervision runtime for the
      // fulfilment-events consumer (US-18.2/19.1).
      "io.github.beckfordp" %% "purekafka" % purekafkaVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-core" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server" % tapirVersion,
      // skunk: non-blocking, pure-FP Postgres access — this service's persistence layer.
      "org.tpolecat" %% "skunk-core" % skunkVersion,
      // skunk-circe: Codec[Json] for the outbox_events.payload jsonb column
      // (ADR 0008 / TD-3.4) — avoids a text/jsonb OID mismatch from reusing
      // the plain `text` codec against a jsonb column.
      "org.tpolecat" %% "skunk-circe" % skunkVersion,
      // flyway: JDBC-based schema migration tool, run on startup to create/update the
      // payment table. Independent of Skunk (which handles all runtime queries).
      "org.flywaydb" % "flyway-core" % flywayVersion,
      "org.flywaydb" % "flyway-database-postgresql" % flywayVersion,
      // postgresql (pgjdbc): build-only JDBC driver, used solely by Flyway to run
      // migrations. Runtime-only: never referenced directly in code, loaded by
      // Flyway/JDBC's DriverManager via SPI.
      "org.postgresql" % "postgresql" % postgresqlJdbcVersion % Runtime,
      // pureconfig: loads application.conf into typed config case classes.
      "com.github.pureconfig" %% "pureconfig-core" % pureconfigVersion,
      // munit: test framework used across this project (Scala-native, no JUnit dependency).
      "org.scalameta" %% "munit" % munitVersion % Test,
      // munit-cats-effect: lets test bodies return IO[Unit] and run under munit directly.
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectVersion % Test,
      // munit-scalacheck: property-based tests for pure payment rules
      // (scala-fp.md law 8), e.g. id parse round-trips.
      "org.scalameta" %% "munit-scalacheck" % munitScalacheckVersion % Test,
      // testcontainers-scala: spins up a real, ephemeral Postgres/Kafka container
      // for integration tests (not used by main code).
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-kafka" % testcontainersScalaVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersScalaVersion % Test,
      // log4cats-testing: purerestlib keeps this Test-scoped (doesn't propagate to
      // consumers), so this service declares its own copy to assert on log output
      // (StructuredTestingLogger) in its own tests.
      "org.typelevel" %% "log4cats-testing" % log4catsVersion % Test
    )
  )
