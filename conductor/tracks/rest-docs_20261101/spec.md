# Spec: REST docs convention (gluon TD-20.4)

## Overview
payment-service `/docs` (Swagger, via `purerest.docs.Docs`) lists bare paths and
unexplained request/response schemas. Apply the TD-20 convention (purerest README
"Documenting endpoints") to every served endpoint, assert it in a test, and pin each
endpoint's error set. Mirrors order-service's `rest-docs_20261101` (TD-20.3).

## Functional Requirements
- FR1: purerestlib 0.4.0 → 0.7.3 (+ aligned pins); fix any API drift; suite green;
  `conductor/tech-stack.md` synced.
- FR2: Per-endpoint error outputs — TD-16.1 already split these into one `oneOf` per
  endpoint (no all-errors-everywhere block). Verify and pin with a test, changing
  nothing if already narrow: POST /payments `400, 409`; GET /payments/{id} `404`;
  DELETE /payments/{id} `404`; POST /payments/methods `400`; POST
  /payments/authorize `400, 402, 404, 409, 500`; GET /health none; GET
  /health/ready `503`.
- FR3: Every endpoint `.summary`/`.description`/`.tag` (`payments`, `payment-methods`,
  `health`) — see Appendix.
- FR4: `id` path parameter described + example (UUID). There are no query or header
  inputs.
- FR5: Every request/response DTO and field `@description`; an example on every field
  (`@encodedExample`) covering `CreatePaymentRequest`, `PaymentResponse`,
  `CreatePaymentMethodRequest`, `PaymentMethodResponse`, `AuthorizePaymentRequest`,
  `AuthorizePaymentResponse`, `Address`, `ErrorResponse`, `DeclinedResponse`.
- FR6: Every error variant described + example body; `GET /health/ready`'s bodyless
  `503` carries a description.
- FR7: `PaymentDocsSuite` asserts `Docs.undocumented(endpoints.map(_.endpoint)) == Nil`
  over exactly the endpoints passed to `Docs.routes` in `AppRoutes` (7 endpoints).
- FR8: `scripts/verify-rest-docs.sh` (adapted from order-service) checks the served
  `/docs/docs.yaml`.

## Non-Functional
- Wire format unchanged (encode/decode identical); scalafmtCheck clean.
- No real ids/credentials in examples; simulated card data only.

## Acceptance Criteria
- Full suite green, zero `DocGap`s, verify script PASS, CI green.
- `/docs` describes every endpoint, parameter, DTO field and error variant, and each
  endpoint lists only the errors it can return.

## Out of Scope
- Client mirror DTOs (order-service's `PaymentClient` etc.).
- Behaviour/wire changes; AsyncAPI side (TD-19).
- Enforcing `customerId` as a parsed UUID (TD-16.1 L3) — documented as a UUID here only.

## Appendix: proposed descriptions (2026-11-01, awaiting approval)

Convention: `@description` on the class and every field, `@encodedExample` on every
field (Swagger builds the payload from them). Nothing below is applied to code until
approved.

### Endpoint summary / description / tag
| Endpoint | summary | description | tag |
|---|---|---|---|
| POST /payments | Create a payment | Records a new payment in `pending` status for an order. `orderId` is unique: a second payment for the same order is `409`. Does not charge, authorize or publish an event — that is `POST /payments/authorize`. | payments |
| GET /payments/{id} | Get a payment | One payment by its UUID. Read-only. | payments |
| DELETE /payments/{id} | Delete a payment | Hard-deletes the payment record. No event published. `404` when the id is unknown. | payments |
| POST /payments/methods | Create a payment method | Validates the card (length, digits, Luhn, expiry) and billing address, then stores only a simulated token, last four digits, brand and expiry. The PAN is never stored, logged or echoed back. Not idempotent — each call creates a new method. | payment-methods |
| POST /payments/authorize | Authorize a payment | Places a hold for an order and records it as `authorized`, publishing `PaymentAuthorized` via the outbox. A repeated identical request replays the stored authorization (same `authId`/`expiresAt`, no second hold); a different amount or payment method for the same order is `409`; a replayed decline is `402` with the original reason. Declines are synchronous and publish nothing. | payments |
| GET /health | Liveness probe | `200` while the process is up. Polled by Kubernetes. | health |
| GET /health/ready | Readiness probe | `200` when Postgres is reachable, `503` otherwise. Polled by Kubernetes. | health |

### Parameters
| Parameter | In | description | example |
|---|---|---|---|
| id | path (GET/DELETE /payments/{id}) | Payment id (UUID). | `9b2f1c3d-4e5a-4b6c-8d7e-9f0a1b2c3d4e` |

No query/header inputs exist on any endpoint.

### DTO fields
Applied with `@description` on the class and every field; example = `@encodedExample`.

**CreatePaymentRequest** — "A new payment to record."
| Field | description | example |
|---|---|---|
| orderId | Order id this payment is for; non-blank, unique across payments. | `3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f` |
| amountCents | Amount to charge, in cents; must be positive. | `4999` |

**PaymentResponse** — "A payment as stored."
| Field | description | example |
|---|---|---|
| id | Payment id (UUID). | `9b2f1c3d-4e5a-4b6c-8d7e-9f0a1b2c3d4e` |
| orderId | Order id the payment is for. | `3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f` |
| amountCents | Amount to charge, in cents. | `4999` |
| status | Lifecycle status: `pending`, `authorized`, `captured`, `failed` or `voided`. | `pending` |
| createdAt | Creation time (UTC). | `2026-11-01T12:00:00Z` |
| updatedAt | Last status change (UTC). | `2026-11-01T12:00:00Z` |

**CreatePaymentMethodRequest** — "A card and billing address to tokenize."
| Field | description | example |
|---|---|---|
| customerId | Customer id (UUID). | `c1f2a3b4-5d6e-4f70-8a9b-0c1d2e3f4a5b` |
| cardNumber | Simulated PAN. Validated (length, digits, Luhn) and immediately tokenized; never stored, logged or returned. | `4242 4242 4242 4242` |
| expiry | Card expiry, `MM/YY`; must not be in the past. | `12/99` |
| billingAddress | Billing address stored with the method. | Address example below |

**PaymentMethodResponse** — "A tokenized payment method; never the PAN."
| Field | description | example |
|---|---|---|
| id | Payment method id (UUID). | `5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a8b` |
| last4 | Last four PAN digits. | `4242` |
| brand | Card brand: `visa`, `mastercard`, `amex` or `other`. | `visa` |
| expiry | Card expiry, `MM/YY`. | `12/99` |

**AuthorizePaymentRequest** — "An order to authorize, and the method to hold against."
| Field | description | example |
|---|---|---|
| orderId | Order id to authorize; non-blank. | `3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f` |
| amountCents | Amount to hold, in cents; must be positive. | `4999` |
| paymentMethodId | Saved payment method (UUID) to authorize against. | `5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a8b` |

**AuthorizePaymentResponse** — "The placed hold."
| Field | description | example |
|---|---|---|
| authId | Provider authorization id (the hold reference). | `auth_2f1c3b4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d` |
| expiresAt | Hold expiry (UTC); after this a capture is refused. | `2026-11-08T12:00:00Z` |

**Address** — "Billing address."
| Field | description | example |
|---|---|---|
| line1 | Street address. | `1 High Street` |
| line2 | Optional second address line. | `Flat 2` |
| city | City or town. | `London` |
| postalCode | Postal code. | `SW1A 1AA` |
| country | ISO 3166-1 alpha-2 country code. | `GB` |

**ErrorResponse** — "A failed request."
| Field | description | example |
|---|---|---|
| error | Human-readable reason. | `Payment not found` |

**DeclinedResponse** — "The provider declined the authorization."
| Field | description | example |
|---|---|---|
| error | Human-readable outcome. | `Payment declined` |
| reason | Provider decline reason. | `insufficient_funds` |

### Error variants
| Endpoint | Status | When | Example body |
|---|---|---|---|
| POST /payments | 400 | `orderId` blank | `{"error":"orderId is required"}` |
| POST /payments | 400 | `amountCents` ≤ 0 | `{"error":"amountCents must be positive"}` |
| POST /payments | 409 | The order already has a payment | `{"error":"Order 3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f already has a payment"}` |
| GET /payments/{id} | 404 | No payment with that UUID | `{"error":"Payment not found"}` |
| DELETE /payments/{id} | 404 | No payment with that UUID | `{"error":"Payment not found"}` |
| POST /payments/methods | 400 | `customerId` blank | `{"error":"customerId is required"}` |
| POST /payments/methods | 400 | PAN fails length/digits/Luhn | `{"error":"Invalid card number"}` |
| POST /payments/methods | 400 | Expiry not `MM/YY` | `{"error":"Invalid expiry, expected MM/YY"}` |
| POST /payments/methods | 400 | Card already expired | `{"error":"Card has expired"}` |
| POST /payments/methods | 400 | Billing address missing `line1`/`city`/`postalCode` | `{"error":"line1 is required"}` |
| POST /payments/methods | 400 | `country` not ISO alpha-2 | `{"error":"country must be an ISO 3166-1 alpha-2 code"}` |
| POST /payments/authorize | 400 | `orderId` blank / `amountCents` ≤ 0 | `{"error":"amountCents must be positive"}` |
| POST /payments/authorize | 404 | `paymentMethodId` unknown | `{"error":"Payment method not found"}` |
| POST /payments/authorize | 402 | Provider declined the hold | `{"error":"Payment declined","reason":"insufficient_funds"}` |
| POST /payments/authorize | 409 | Same order, different amount or method | `{"error":"Order already has a payment with a different amount or payment method"}` |
| POST /payments/authorize | 409 | Stored authorization has expired | `{"error":"Order's stored authorization has expired"}` |
| POST /payments/authorize | 409 | Stored payment is `pending`, `captured` or `voided` | `{"error":"Order already has a payment in status captured"}` |
| POST /payments/authorize | 500 | Stored `authorized` row missing `authId`/`expiresAt` | `{"error":"Stored authorization is incomplete"}` |
| POST /payments/authorize | 500 | Stored `failed` row missing its reason | `{"error":"Stored decline has no reason"}` |
| POST /payments/authorize | 500 | Insert lost a race and the row can't be read back | `{"error":"Payment could not be recorded or read back"}` |
| GET /health/ready | 503 | Postgres unreachable | no body |

Notes:
- Tapir generates its own `400` for an undecodable body or a non-UUID `id` path value
  before the handler runs (plain-text `Invalid value for: body` / `path parameter id`);
  it stays alongside the endpoint's declared `400`.
- `POST /payments/methods`'s `customerId` is accepted as any non-blank string today
  (TD-16.1 L3); the docs describe it as the UUID the platform sends.
