# Plan: Journey telemetry (payment-service)

## Phase 1: Adopt 0.8.0 cross-cutting telemetry
- [ ] Task: Bump purerestlib + purekafka 0.8.0; `Telemetry.resource` takes
      `ioRuntimeMetrics`; `Main` passes `runtime.metrics`
- [ ] Task: Write failing tests, then wrap pool(s) with `PoolMetrics`; `EventAge` in both consumers
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Business metrics and milestones
- [ ] Task: Write failing tests for the business metrics below
- [ ] Task: Implement and wire in `Main`
- [ ] Task: Full `sbt scalafmtCheck test` + coverage
- [ ] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Deploy + live verification
- [ ] Task: Build/deploy to gluon-local; `scripts/verify-journey-telemetry.sh` greps the
      metrics port for the new series after real traffic
- [ ] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)
