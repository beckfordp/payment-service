# Plan — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Phase 1: Local sbt wiring + verification against the real OrbStack Apicurio instance [checkpoint: ddba59f]
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
- [x] Task: Conductor - User Manual Verification 'Phase 1: Local sbt
      wiring + verification' (Protocol in workflow.md) `ddba59f` —
      reduced-prompting mode: automated-test + verify-script
      verification only, no interactive walkthrough; see git note on
      `ddba59f`

## Phase 2: Real CI wiring [checkpoint: 2ee82a8]
- [x] Task: Add the same `docker run` Apicurio CI step shape as the
      prior two services' `ci.yml` to this repo's `ci.yml` `d1ed26e`
- [x] Task: Confirm the `GH_PACKAGES_TOKEN` repo secret is in place
      (pre-existing gap, same as the prior two services) — was missing
      (confirmed via `gh secret list`); user added it (took two
      attempts — the first one didn't take for an unclear reason)
- [x] Task: Push this branch and confirm the new CI step passes on a
      real GitHub Actions run — CI run `40185935902`: all steps green,
      including `schemaRegistryCheck` against the throwaway Apicurio
      container, first try
- [x] Task: Conductor - User Manual Verification 'Phase 2: Real CI
      wiring' (Protocol in workflow.md) — reduced-prompting mode: the
      real CI run itself is the verification; no interactive walkthrough
