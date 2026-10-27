#!/usr/bin/env bash
set -euo pipefail

# Verifies US-14.1 (track checkout-authorize_20261027, ADR 0011) against a real
# sbt run instance + Postgres, using the shipped application.conf simulator
# map: an approving card authorizes (200 {authId, expiresAt}, Authorized
# payment row, one PaymentAuthorized outbox row); 4000 0000 0000 0002
# (tok_visa_0002) declines (402 insufficient_funds, Failed row, no new outbox
# row); unknown method -> 404; bad amount -> 400.
#
# Usage: ./scripts/verify-checkout-authorize.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0
SBT_PID=""

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill -9 "$SBT_PID" >/dev/null 2>&1 || true
  lsof -ti tcp:8080 | xargs kill -9 >/dev/null 2>&1 || true
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 120); do
    status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health/ready || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: payment-service did not become ready in time." >&2
  return 1
}

check() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    echo "   OK: ${label} (got ${actual})"
  else
    echo "   FAIL: ${label} - expected ${expected}, got ${actual}" >&2
    FAILED=1
  fi
}

if lsof -ti tcp:8080 >/dev/null 2>&1; then
  echo "FAIL: something is already listening on :8080." >&2
  exit 1
fi

echo "1. docker compose up -d postgres (fresh volume)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --wait postgres >/dev/null
echo "   OK: postgres healthy"

echo
echo "2. Starting payment-service (sbt run) in the background..."
sbt run </dev/null >/tmp/payment-service-verify-checkout-authorize.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: payment-service ready on :8080 (config incl. payment-gateway loaded)"

echo
psql() {
  docker compose exec -T postgres psql -U payment -d payment -tAc "$1"
}

create_method() {
  curl -s -X POST -H 'Content-Type: application/json' \
    -d "{\"customerId\":\"verify-authorize\",\"cardNumber\":\"$1\",\"expiry\":\"12/99\",\"billingAddress\":{\"line1\":\"1 High St\",\"line2\":null,\"city\":\"London\",\"postalCode\":\"SW1A 1AA\",\"country\":\"GB\"}}" \
    http://localhost:8080/payments/methods | sed -E 's/.*"id":"([^"]+)".*/\1/'
}

authorize() {
  curl -s -w '\n%{http_code}' -X POST -H 'Content-Type: application/json' \
    -d "{\"orderId\":\"$1\",\"amountCents\":$2,\"paymentMethodId\":\"$3\"}" \
    http://localhost:8080/payments/authorize
}

echo "3. Create an approving and a declining payment method..."
approve_id="$(create_method "4242 4242 4242 4242")"
decline_id="$(create_method "4000 0000 0000 0002")"
check "decline card tokenized as tok_visa_0002" "tok_visa_0002" \
  "$(psql "select card_token from payment_methods where id = '${decline_id}'")"

echo
echo "4. Authorize with the approving card..."
ORDER_OK="$(uuidgen | tr 'A-Z' 'a-z')"
resp="$(authorize "$ORDER_OK" 4999 "$approve_id")"
check "returns 200" "200" "$(echo "$resp" | tail -1)"
auth_id="$(echo "$resp" | head -1 | sed -E 's/.*"authId":"([^"]+)".*/\1/')"
check "authId has auth_ prefix" "auth_" "${auth_id:0:5}"
check "payment row Authorized with authId/expiresAt" "authorized|${auth_id}|true" \
  "$(psql "select status || '|' || auth_id || '|' || (expires_at > now() + interval '6 days') from payment where order_id = '${ORDER_OK}'")"
check "one PaymentAuthorized outbox row for the order" "1|PaymentAuthorized|${auth_id}" \
  "$(psql "select count(*) || '|' || max(payload->>'eventType') || '|' || max(payload->'payload'->>'authId') from outbox_events where aggregate_id = '${ORDER_OK}'")"

echo
echo "5. Authorize with the declining card..."
ORDER_DECLINE="$(uuidgen | tr 'A-Z' 'a-z')"
outbox_before="$(psql "select count(*) from outbox_events")"
resp="$(authorize "$ORDER_DECLINE" 4999 "$decline_id")"
check "returns 402" "402" "$(echo "$resp" | tail -1)"
check "decline reason" "insufficient_funds" \
  "$(echo "$resp" | head -1 | sed -E 's/.*"reason":"([^"]+)".*/\1/')"
check "payment row Failed with reason" "failed|insufficient_funds" \
  "$(psql "select status || '|' || decline_reason from payment where order_id = '${ORDER_DECLINE}'")"
check "no new outbox row" "$outbox_before" "$(psql "select count(*) from outbox_events")"

echo
echo "6. Unknown method -> 404, non-positive amount -> 400..."
check "unknown method" "404" \
  "$(authorize "$ORDER_OK" 4999 "3fa85f64-5717-4562-b3fc-2c963f66afa6" | tail -1)"
check "zero amount" "400" "$(authorize "$ORDER_OK" 0 "$approve_id" | tail -1)"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
