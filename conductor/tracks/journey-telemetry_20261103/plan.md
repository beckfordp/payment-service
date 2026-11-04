# Plan: Journey telemetry (payment-service)

## Phase 1: Adopt 0.8.0 cross-cutting telemetry
- [x] Task: Bump purerestlib + purekafka 0.8.0; `Telemetry.resource` takes
      `ioRuntimeMetrics`; `Main` passes `runtime.metrics` 3350a6c
- [x] Task: Write failing tests, then wrap pool(s) with `PoolMetrics`; `EventAge` in both consumers 3350a6c
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Business metrics and milestones
- [x] Task: Write failing tests for the business metrics below aef6de4
- [x] Task: Implement and wire in `Main` aef6de4
- [x] Task: Full `sbt scalafmtCheck test` + coverage aef6de4
- [x] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Deploy + live verification
- [x] Task: Bump purerest 0.8.2 — JFR runtime telemetry off (gluon TD-28) 6a04838
- [x] Task: Build/deploy to gluon-local; `scripts/verify-journey-telemetry.sh` greps the
      metrics port for the new series after real traffic 4fd0af8
- [x] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)
