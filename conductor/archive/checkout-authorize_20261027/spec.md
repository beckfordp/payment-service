# Spec — Checkout Journey 1: payment-service (US-12.1, US-13.1, US-13.2, US-14.1, ADR 0011)

## Overview
Replace the retired create-then-settle charge with ADR 0011's authorize/capture split, authorize half only. Adds a `PaymentGatewayClient[F]` algebra + PureConfig-driven `simulated` interpreter, a `PaymentMethod` store with simulated tokenizer, gshop-facing `POST /payments/methods`, and order-service-facing synchronous `POST /payments/authorize` that publishes `PaymentAuthorized` via the outbox on success only. Contracts authoritative in `gluon/docs/system-design.md` (REST contracts, Payload contracts, Local payment simulation).

## Functional Requirements
1. **PaymentStatus** gains `Authorized` (`"authorized"`), between `Pending` and `Settled`. `Settled` redefined (docs/comments) to mean *captured*. Flyway migration widens `payment_status_check`.
2. **PaymentGatewayClient[F]** algebra: `authorize(cardToken, amountCents)` → `F[Either[DeclineReason, Authorization(authId, expiresAt)]]`; `capture(authId)`, `void(authId)` (simulated: always succeed; consumers land in Journeys 3/4).
   - `simulated` interpreter config: `payment-gateway.simulated { default-outcome, outcomes { "<token>" = "approve" | "decline:<reason>" }, authorization-ttl = 7d }`. Unlisted token → default. Invalid outcome string → config load failure.
   - `authId` = generated `auth_<uuid>`; `expiresAt` = now + TTL.
3. **PaymentMethod** domain + `PaymentMethodStore[F]` (`inMemory`/`postgres`): `id`, `customerId`, `cardToken`, `last4`, `brand` (visa|mastercard|amex|other), `expiry` (MM/YY), `billingAddress` (Address), `createdAt`. New `payment_methods` table (billing address as JSONB). PAN never stored or logged.
   - Simulated tokenizer: `tok_<brand>_<last4>` (simulator-only, collision-prone — documented).
4. **Card validation**: 13–19 digits (spaces stripped), Luhn checksum, brand by prefix (4→visa, 51–55/2221–2720→mastercard, 34/37→amex, else other), expiry `MM/YY` valid month and not in past.
5. **Address validation** (US-12.1): `line1`, `city`, `postalCode` non-blank; `line2` optional; `country` ISO 3166-1 alpha-2 shape (2 uppercase letters).
6. **`POST /payments/methods`**: request `{customerId, cardNumber, expiry, billingAddress}` → `201 {id, last4, brand, expiry}`; `400 {error}` on invalid card/expiry/address/customerId. Token never returned.
7. **`POST /payments/authorize`**: request `{orderId, amountCents, paymentMethodId}`.
   - `404 {error: "Payment method not found"}` unknown method; `400` non-positive amount / blank orderId.
   - Approve → `200 {authId, expiresAt}`; persists `Payment{status: Authorized, paymentMethodId, authId, expiresAt}` **and** outbox `PaymentAuthorized` envelope in one transaction.
   - Decline → `402 {error, reason}`; persists `Payment{status: Failed, paymentMethodId, declineReason}`; nothing written to outbox.
   - Payment table migration adds nullable `payment_method_id`, `auth_id`, `expires_at`, `decline_reason`.
8. **Outbox writer** restored in `PaymentStore` (mirrors order-service's `PendingOutboxEvent` pattern, ADR 0008/0009 envelope, trace/correlation ids).
9. Both new endpoints in tapir → `/docs` OpenAPI; CORS allowed methods updated if needed. `AsyncApiChannels` unchanged (contract already registered by Rework 7).

## Non-Functional Requirements
- Tagless-final, dual interpreters, existing telemetry (`timed` spans/metrics) on new store ops.
- Coverage per workflow.md; Testcontainers-Postgres for store + transactional outbox.
- No real provider, no network in gateway.

## Acceptance Criteria
- Unit tests: gateway outcome map (approve, decline:reason, default, bad config), Luhn/brand/expiry/address validation, tokenizer.
- Route tests: methods 201/400; authorize 200/402/404/400.
- Postgres tests: method round-trip (no PAN column); authorize success writes payment + outbox row atomically; decline writes payment, no outbox row.
- `scripts/verify-*.sh` runs real service: create method (approve token + configured decline token) → authorize → assert 200/402 and outbox row.
- `system-design.md` contracts match `/docs` output; product.md / tech-stack.md synced.

## Out of Scope
- Capture/void triggers (Journeys 3/4), expiry sweep (Journey 5), US-6.2 idempotency (duplicate authorize per orderId not guarded), real provider, removing generated CRUD routes, order-service `PaymentClient` (separate track).
