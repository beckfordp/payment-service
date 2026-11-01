# Plan — TD-16.1 payment-service review fixes

## Phase 1: Pure core (M1, H2, H5)
- [ ] Task: Move `PaymentStatus`/`transition` into `domain/` (M1)
- [ ] Task: `LiveAuthorization` rule once in `domain/`; consumer, replay, in-memory store, SQL param use it (H2)
- [ ] Task: `PaymentRules.validate` shared by `POST /payments` and `/payments/authorize` (H5)
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Pure core' (Protocol in workflow.md)

## Phase 2: Typed errors and totality (H3, H4, H6)
- [ ] Task: `PaymentStore.create` returns `Either[PaymentAlreadyExists, Payment]` (H3)
- [ ] Task: Exhaustive `Outcome` and replay matches in `AuthorizeRoutes` (H4, H6)
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Typed errors' (Protocol in workflow.md)

## Phase 3: Capability, cancellation, DLQ (H1, M2, L1)
- [ ] Task: Narrow `FulfilmentEventConsumer.handle` to `MonadThrow`+`Clock` (H1)
- [ ] Task: `uncancelable`+`poll` around authorize-then-record (M2)
- [ ] Task: Dead-letter null-value `fulfilment-events` records (L1)
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Capability, cancellation, DLQ' (Protocol in workflow.md)
