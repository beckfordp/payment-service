# Plan: Checkout Journey 1 payment-service — gateway, payment methods, authorize

## Phase 1: Domain foundations
- [x] Task: `PaymentStatus.Authorized` — failing `PaymentStatusSuite`/`MigrationsSuite` cases (Red); add case + `V4__add_authorized_status.sql` widening `payment_status_check`; Settled doc comment → "captured" (Green) `0b0e98f`
- [ ] Task: `Address` + validation (US-12.1) — failing `AddressSuite` (blank line1/city/postalCode, bad country, optional line2) (Red); implement `Address` + circe/tapir codecs + `validate` (Green)
- [ ] Task: Card validation + simulated tokenizer — failing `CardSuite` (Luhn, 13–19 digits, spaces stripped, brand prefixes incl. 2221–2720, MM/YY bad month/past expiry with injected clock, `tok_<brand>_<last4>`) (Red); implement `Card` (Green)
- [ ] Task: `PaymentGatewayClient[F]` + `simulated` — failing `PaymentGatewayClientSuite` (approve, decline:reason, default-outcome, TTL → expiresAt, `auth_` prefix, capture/void succeed) + `PaymentServiceConfigSuite` (parses `payment-gateway.simulated`, rejects bad outcome string) (Red); implement algebra, `SimulatedGatewayConfig` ConfigReader, `application.conf` block (default approve, `tok_visa_0002` → `decline:insufficient_funds`, ttl 7d) (Green)
- [ ] Task: Full `sbt test` + coverage >80% new code
- [ ] Task: Conductor - User Manual Verification 'Domain foundations' (Protocol in workflow.md)

## Phase 2: Payment methods (US-13.1, US-13.2)
- [ ] Task: `PaymentMethodStore[F]` — failing `PaymentMethodStoreSuite` (inMemory create/get) + `PaymentMethodStorePostgresSuite` (round-trip, billing JSONB, schema has no PAN column) (Red); `V5__create_payment_methods.sql`, `inMemory`/`postgres` with `timed` telemetry (Green)
- [ ] Task: `POST /payments/methods` — failing route tests (201 body `{id,last4,brand,expiry}`, no token in body, 400 per invalid card/expiry/address/blank customerId) (Red); tapir endpoint in `PaymentRoutes`, wire store in `Main`/`AppRoutes`, docs suite updated (Green)
- [ ] Task: Full `sbt test` + coverage
- [ ] Task: Conductor - User Manual Verification 'Payment methods' (Protocol in workflow.md)

## Phase 3: Authorize (US-14.1)
- [ ] Task: Outbox writer + authorize persistence — failing `PaymentStoreSuite`/`PaymentStorePostgresSuite` (`authorize` writes Authorized payment w/ method/authId/expiresAt + `PaymentAuthorized` envelope outbox row in one tx; `decline` writes Failed payment w/ reason, no outbox row; rollback leaves neither) (Red); `V6__add_authorization_columns.sql`, `PendingOutboxEvent` mirroring order-service, `inMemoryWithOutbox` (Green)
- [ ] Task: `POST /payments/authorize` — failing route tests (200 `{authId,expiresAt}`, 402 `{error,reason}`, 404 unknown method, 400 non-positive amount/blank orderId) (Red); endpoint orchestrating method lookup → gateway → store, wired in `Main` (Green)
- [ ] Task: CORS allowed methods check/update; `CorsSuite` green
- [ ] Task: Full `sbt test` + coverage
- [ ] Task: `scripts/verify-checkout-authorize.sh` — real Postgres + `sbt run`: create method (4242… approve), authorize → 200 + outbox row; create method ending 0002 visa (decline token) → 402, no new outbox row; unknown method → 404; trap cleanup. Run for real
- [ ] Task: Conductor - User Manual Verification 'Authorize' (Protocol in workflow.md)

## Phase 4: Docs sync
- [ ] Task: Diff `/docs` OpenAPI vs `gluon/docs/system-design.md` REST contracts; fix drift (gluon commit separate)
- [ ] Task: Update `product.md` (domain model, US-12.1/13.1/13.2/14.1 done), `tech-stack.md` (gateway config, outbox writer), README endpoints

Note: decline test card = Luhn-valid visa ending 0002 (e.g. 4000 0000 0000 0002) → `tok_visa_0002`.
