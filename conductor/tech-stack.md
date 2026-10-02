# Tech Stack — payment-service

## Language / runtime
- Scala 3.9.0
- JDK 21

## Effects / HTTP
- Cats Effect 3.7.0
- http4s 0.23.37 (ember-server)
- circe 0.14.16

## Messaging
- fs2-kafka 3.6.0 — `order.reserved` consumer + `payment.settled`/
  `payment.failed` producer, matching inventory-service's (US-5.1) and
  order-service's (US-5.2) choice. Plain `String` key/value, hand-rolled
  circe JSON (no circe-kafka serializer layer) — same convention already
  established cross-repo.

## API layer
- tapir 1.11.25 — route definitions and generated Swagger/OpenAPI docs
  (`purerest.docs.Docs`), served at `/docs`, kept in sync by construction

## Persistence
- skunk 1.0.0 — pure-FP, non-blocking Postgres access (runtime queries)
- Flyway 11.8.2 (+ flyway-database-postgresql) — schema migrations on startup
- postgresql JDBC 42.7.13 — Flyway-only (runtime scope), never used directly
  in application code

## Config
- pureconfig 0.17.10 — typed config from `application.conf`

## Shared platform library
- `purerestlib` 0.1.0 (`io.github.beckfordp`) — tracing, structured logging,
  metrics, resilience (retry + circuit breaker), tapir-based docs. Consumed
  as a published GitHub Packages artifact, never vendored/`.dependsOn`.
  ADR 0005 flags a possible future migration off GitHub Packages (JitPack or
  Maven Central) — not yet adopted.

## Testing
- munit 1.3.6 + munit-cats-effect 2.2.1
- log4cats-testing 2.8.0 — assert on structured log output
- testcontainers-scala 0.43.6 (postgresql + kafka + munit modules) — real,
  ephemeral Postgres/Kafka for integration tests, no manual local setup
- scalafmt (default Scala 3 style) — `sbt scalafmtCheck test` run in CI

## Packaging / local deploy
- sbt-native-packager (`JavaAppPackaging`, `DockerPlugin`)
- Docker image: `eclipse-temurin:21-jre`
- Docker Compose — local Postgres + Kafka (single-node KRaft broker,
  `apache/kafka:3.8.0`, matching inventory-service's docker-compose.yml)

## Not yet in `build.sbt` (needed for upcoming backlog items)
- Redis client — for US-6.2 (idempotency keys to avoid double-charging on
  retry/redelivery)

## Target infrastructure (platform-wide, from `gluon/docs/system-design.md`)
- Local: OrbStack Kubernetes (see ADR 0002)
- Promotion: dev → staging → prod, all on AWS EKS, single AWS account /
  multi-namespace (see ADR 0004)
- CI/CD: GitHub Actions, candidate release = container image tag promoted
  through environments with automated smoke-test gates
