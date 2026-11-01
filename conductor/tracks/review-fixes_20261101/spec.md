# Spec: TD-16.1 payment-service review fixes

## Overview
Apply the findings `/conductor:review` raised on `payment-review-fixes_20261029`
(listed in gluon `TECHNICAL_DEBT.md` TD-16.1), plus what a fresh pass over main +
test code against `scala-fp.md`'s Review checklist and `general.md` found.
Baseline: 195 tests green at `a3a40db`. No wire-contract change (REST or Kafka).

## Findings
| # | Sev | Finding | Action |
|---|-----|---------|--------|
| H1 | High | `FulfilmentEventConsumer.handle` asks for `Async`; it only sequences, raises and reads the clock | Narrow to `MonadThrow`+`Clock` |
| H2 | High | "capture needs a live authorization" defined 4x: consumer pre-check, authorize replay, in-memory store, SQL `$bool` param computed inline | Define once in `domain/` (`LiveAuthorization`); callers + SQL param use it |
| H3 | High | `PaymentStore.create` raises `PaymentAlreadyExists` (expected failure as exception) | Return `Either[PaymentAlreadyExists, Payment]`; plain value, not `RuntimeException` |
| H4 | High | `case _` over `Outcome` in `AuthorizeRoutes`' `guaranteeCase` | Exhaustive `Errored`/`Canceled` |
| H5 | High | (new) `orderId` non-blank + `amountCents > 0` rule duplicated in `PaymentRoutes.validate` and `AuthorizeRoutes.validate` | One pure `domain.PaymentRules.validate` |
| H6 | High | (new) `case _` over `(Option, Option)` in `AuthorizeRoutes.replay` | Explicit `(None, _) \| (_, None)` |
| M1 | Medium | `PaymentStatus` + `transition` live outside `domain/` | Move to `paymentservice.domain` |
| M2 | Medium | Cancellation window between `gateway.authorize` returning and `guaranteeCase` installing leaks the hold | `uncancelable` + `poll` around authorize-then-record |
| L1 | Low | Null-value `fulfilment-events` records are logged and committed | Dead-letter them (DLQ value type `Option[String]`, null preserved) |
| L2 | Low | (new) `customerId` on `POST /payments/methods` is an unparsed `String` (platform-wide a UUID) | Deferred: enforcing UUID rejects inputs accepted today (verify scripts send free-form ids); documented as UUID in TD-20.4 |

## Acceptance Criteria
- Each fixed finding has a failing-first test (compile-level for H1) now green.
- Full suite green, coverage > 80%, scalafmtCheck clean, AsyncAPI generation unchanged.

## Out of Scope
- Real payment provider; Journey 5 expiry sweep; REST docs (TD-20.4).
