# Plan: US-18.2 — void authorization on DispatchFailed

## Phase 1: Status + store

- [ ] Task: Failing tests — `PaymentStatus.Voided` parses/serializes; migration `V9` allows `voided`; store gains `markVoided(orderId)` updating only from `authorized` (Red)
- [ ] Task: Implement the status, `V9__add_voided_status.sql`, and `markVoided` in both interpreters (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Status + store' (Protocol in workflow.md)

## Phase 2: Consumer + gateway wiring

- [ ] Task: Failing consumer tests (Kafka): synthetic `DispatchFailed` voids an authorized payment once and marks it `voided`; duplicate no-ops; non-authorized/missing skipped; gateway failure leaves it authorized (Red)
- [ ] Task: Implement `FulfilmentEventConsumer` (null-safe, filtered, DLQ-guarded) merged independently supervised in `KafkaConsumers`; wire the gateway in `Main` (Green)
- [ ] Task: Refactor, full suite green
- [ ] Task: Conductor - User Manual Verification 'Consumer + gateway wiring' (Protocol in workflow.md)

## Phase 3: AsyncAPI, registry, live proof, docs

- [ ] Task: Register Subscribe `DispatchFailed` on `fulfilment-events`; run `schemaRegistryCheck`
- [ ] Task: Update `gluon/docs/system-design.md`, README/product/tech-stack
- [ ] Task: Rebuild/redeploy payment-service; `scripts/verify-void.sh` — authorized order forced to fail → payment row `voided`; duplicate harmless; cleanup
- [ ] Task: Conductor - User Manual Verification 'AsyncAPI, registry, live proof, docs' (Protocol in workflow.md)

## Risks / notes

- Void first, then status; gateway failure retries via DLQ with the payment still authorized.
- Duplicate sees `voided` and no-ops; no double void.
- No event published on void.
