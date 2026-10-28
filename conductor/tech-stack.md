# Tech Stack — payment-service

## Language / runtime
- Scala 3.9.0
- JDK 21

## Effects / HTTP
- Cats Effect 3.7.0
- http4s 0.23.37 (ember-server)
- circe 0.14.16
- CORS — `org.http4s.server.middleware.CORS` (ships in `http4s-server`, no new
  dependency) wraps every response (`Cors.scala`). Allow all origins, methods
  limited to this service's five CRUD routes, `Content-Type` header allowed,
  credentials disallowed. Ported from `pure-service-generator`'s
  `cors_20261010` track (TD-1.3).

## Messaging
- Payment authorization is a synchronous REST call (2026-10-26 decision). The
  four `payment-events` contracts are published through the `outbox_events`
  table + Debezium CDC: `PaymentAuthorized` in the same transaction as its
  `Authorized` row (`PaymentStore.recordAuthorization`, Journey 1),
  `PaymentCaptured`/`PaymentFailed` in the capture transition (`recordCapture`/
  `recordCaptureFailure`, US-19.1), and `PaymentAuthorizationExpired` with
  Journey 5's sweep.
- Kafka consumption is back via fs2-kafka: `FulfilmentEventConsumer` consumes
  `fulfilment-events` — `DispatchFailed` voids a hold (US-18.2),
  `DispatchConfirmed` captures it (US-19.1) — supervised by purekafka's
  `StreamSupervisor` with its own `DeadLetter` DLQ. The old
  `OrderReservedConsumer` was retired by Rework 7; it stays retired.
- purekafka (`io.github.beckfordp`) — supplies the `purekafka.docs` AsyncAPI
  types (`AsyncApiChannel`, `PublisherKind.Cdc`) plus `StreamSupervisor` and the
  `DeadLetter` retry/DLQ machinery.

## Payment gateway
- `PaymentGatewayClient[F]` (`authorize`/`capture`/`void`, ADR 0011) — only a
  `simulated` interpreter, no real provider, no network. Outcomes are keyed by
  simulated card token from PureConfig `payment-gateway.simulated`:
  `default-outcome`/`outcomes` for authorize
  (`"tok_visa_0002" = "decline:insufficient_funds"`),
  `default-capture-outcome`/`capture-outcomes` for capture US-19.1
  (`"tok_mastercard_4444" = "decline:capture_declined"`), plus
  `authorization-ttl = 7d`. Added 2026-10-27 (`checkout-authorize_20261027`);
  capture outcomes added 2026-10-28 (`capture-on-dispatch_20261028`).

## API layer
- tapir 1.11.25 — route definitions and generated Swagger/OpenAPI docs
  (`purerest.docs.Docs`), served at `/docs`, kept in sync by construction
- purekafka.docs (`AsyncApiChannel`/`Docs`) — generates AsyncAPI documentation
  for this service's Kafka event contracts (`AsyncApiChannels`), served at
  `/docs/asyncapi.json` (raw spec) and `/docs/asyncapi` (rendered static HTML)
  — same tapir-OpenAPI pattern, for the Kafka side. Generation handled by the
  `sbt-purekafka-docs` plugin, auto-chained after `Compile / compile` — no
  manual script needed (ADR 0010).
- `schemaRegistryCheck` (sbt-purekafka-docs, Rework 5 step 3/ADR 0003) —
  reuses the same `AsyncApiChannels` registrations to check/register each
  event's JSON Schema against Apicurio Registry, run explicitly (not
  chained after `compile`): locally against the shared OrbStack instance
  (`schemaRegistryUrl`'s default), in CI against a throwaway
  `docker run` Apicurio container (`SCHEMA_REGISTRY_URL` env override —
  see `ci.yml`). Build/CI-time only, zero runtime presence.

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
