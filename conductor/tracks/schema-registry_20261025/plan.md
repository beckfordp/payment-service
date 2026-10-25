# Plan — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Phase 1: Local sbt wiring + verification against the real OrbStack Apicurio instance [checkpoint: TBD]
- [x] Task: Bump `project/plugins.sbt`'s `sbt-purekafka-docs` `0.1.0-dev`
      -> `0.2.0-dev`; bump `build.sbt`'s `purekafkaVersion` `0.2.1` ->
      `0.3.0` `27b8d07`
- [x] Task: Add `schemaRegistryUrl`/`schemaRegistryGroupId` settings to
      `build.sbt` (same env-var-override shape as the prior two services)
      `27b8d07`
- [x] Task: New `scripts/verify-schema-registry-check.sh` (mirroring the
      prior two services' own scripts), run against the real local
      Apicurio instance: registers both real channels (`OrderReserved`,
      `PaymentSettled`) + sets the `BACKWARD` rule on each on first run;
      a second identical run is a no-op `27b8d07` — live-verified: both
      artifacts confirmed present under the real `payment-service`
      group; second run succeeded as a no-op
- [x] Task: Run full test suite + the verify script end-to-end; confirm
      no regressions `27b8d07` — found and fixed along the way, unrelated
      to this track: `scalafmtCheck`/`Test/scalafmtCheck` failed on 2
      pre-existing unformatted test files
      (`KafkaConsumersSuite.scala`/`OrderReservedConsumerSuite.scala`) —
      ran `Test/scalafmt` (whitespace-only diff, no behavior change),
      then `scalafmtCheck; Test/scalafmtCheck; test`: clean, exit 0;
      verify script: all checks passed
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Local sbt
      wiring + verification' (Protocol in workflow.md)

## Phase 2: Real CI wiring
- [ ] Task: Add the same `docker run` Apicurio CI step shape as the
      prior two services' `ci.yml` to this repo's `ci.yml`
- [ ] Task: Confirm the `GH_PACKAGES_TOKEN` repo secret is in place
      (pre-existing gap, same as the prior two services)
- [ ] Task: Push this branch and confirm the new CI step passes on a
      real GitHub Actions run
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Real CI
      wiring' (Protocol in workflow.md)
