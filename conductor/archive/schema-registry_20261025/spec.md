# Spec — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Overview
Wire payment-service's existing `AsyncApiChannels` registrations into
`sbt-purekafka-docs`'s `schemaRegistryCheck` task (purerest `v0.3.0`/
`sbt-purekafka-docs 0.2.0-dev`, already really published to GitHub Packages),
so both of this service's registered Kafka event schemas are checked/
registered against Apicurio Registry — locally against the real OrbStack
instance, and in real CI via an ephemeral container. Reuses the exact
design settled in order-service's/inventory-service's equivalent tracks (no
new design questions): default `schemaRegistryGroupId`, CI via a throwaway
`docker run` Apicurio container, no new purerest release needed.

## Functional Requirements
- Bump `project/plugins.sbt`'s `sbt-purekafka-docs` version `0.1.0-dev` ->
  `0.2.0-dev`.
- Bump `build.sbt`'s `purekafkaVersion` `0.2.1` -> `0.3.0`.
- Add `schemaRegistryUrl` (local-dev default: the OrbStack cluster-DNS
  address, env-var overridable) / `schemaRegistryGroupId` (plugin default,
  `name.value` = `"payment-service"`) settings to `build.sbt`.
- Add the same CI step shape as order-service/inventory-service's
  `ci.yml`: `docker run` a plain `apicurio/apicurio-registry:3.3.2`
  container, wait for `/health/ready` on port 9000, run `sbt
  schemaRegistryCheck` against it, tear the container down.
- New `scripts/verify-schema-registry-check.sh` (mirroring the prior two
  services' scripts), registering both of this service's real channels
  (`OrderReserved`, `PaymentSettled` — `PaymentFailedEvent` is
  deliberately not registered here, matching `AsyncApiChannels.scala`'s
  own documented reasoning that it's never actually published).

## Non-Functional Requirements
- No change to runtime behavior — build/CI-time only.
- `payment-service`'s CI currently lacks the `GH_PACKAGES_TOKEN` repo
  secret (same pre-existing gap found in order-service's track) — the
  user adds this secret themselves (commands given, same as the prior two
  services); not something I can do myself (blocked by the auto-mode
  classifier).

## Acceptance Criteria
- `sbt schemaRegistryCheck` succeeds locally against the real OrbStack
  Apicurio instance, registering both channels.
- The new CI step passes on a real GitHub Actions run, once
  `GH_PACKAGES_TOKEN` is in place.
- Full existing test suite + the new verify script run clean, no
  regressions.

## Out of Scope
- `notification-service` adoption (its own, separate track).
- `pure-service-generator` template retrofit (Rework 5 step 4, separate
  track).
- Any change to the Debezium CDC / outbox publish path, or to
  `PaymentFailedEvent`'s (non-)registration.
