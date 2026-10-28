# Plan: US-19.1 — capture on DispatchConfirmed

## Phase 1: Store capture transitions + outbox

- [~] Task: Failing tests — `recordCapture`/`recordCaptureFailure` in both interpreters: authorized -> captured/failed, outbox `PaymentCaptured`/`PaymentFailed` payloads, `None` + no event from pending/captured/failed/voided, rollback when the outbox insert fails; update the stale "update to Captured writes no outbox row" test (Red)
- [ ] Task: Implement the two envelope builders and both store methods (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Store capture transitions + outbox' (Protocol in workflow.md)

## Phase 2: Simulated capture outcomes

- [ ] Task: Failing tests — `PaymentGatewayClientSuite`: capture succeeds by default, declines for a token in `captureOutcomes`; config parsing; revise the "capture and void always succeed" test (Red)
- [ ] Task: Implement `capture(authId, cardToken): F[Either[DeclineReason, Unit]]`, the config fields and the `application.conf` demo failure token (Green)
- [ ] Task: Refactor, full suite green
- [ ] Task: Conductor - User Manual Verification 'Simulated capture outcomes' (Protocol in workflow.md)

## Phase 3: Consumer + wiring

- [ ] Task: Failing consumer tests — `DispatchConfirmed` captures once, duplicate no-ops (gateway called once), unknown/non-authorized skip, decline -> `failed` + `PaymentFailed` with no DLQ retry, existing void tests stay green (Red)
- [ ] Task: Implement the `DispatchConfirmed` branch; thread `PaymentMethodStore` through `Main` -> `KafkaConsumers` -> `FulfilmentEventConsumer` (Green)
- [ ] Task: Refactor, full suite + coverage green
- [ ] Task: Conductor - User Manual Verification 'Consumer + wiring' (Protocol in workflow.md)

## Phase 4: AsyncAPI, registry, live proof, docs

- [ ] Task: Subscribe `DispatchConfirmed` in `AsyncApiChannels`; update `AsyncApiChannelsSuite`; run `schemaRegistryCheck`
- [ ] Task: Update README, `conductor/product.md` and `conductor/tech-stack.md` (stale "no Kafka consumers/runtime" claims)
- [ ] Task: Rebuild/redeploy payment-service; `scripts/verify-capture.sh` — success path (captured + one `PaymentCaptured` row), duplicate injection harmless, capture-decline token (`failed` + `PaymentFailed {reason: capture_failed}`); cleanup
- [ ] Task: Conductor - User Manual Verification 'AsyncAPI, registry, live proof, docs' (Protocol in workflow.md)

## Risks / notes

- Crash between the gateway capture and the DB write can double-capture on
  redelivery; the simulated gateway has no side effect, but a real provider
  needs its own idempotency key (out of scope, pending ADR).
- A capture decline is a value; only transport/unknown gateway errors retry
  through the DLQ.
- Widening `capture` and threading `PaymentMethodStore` touches Main,
  KafkaConsumers and the existing test stubs.
