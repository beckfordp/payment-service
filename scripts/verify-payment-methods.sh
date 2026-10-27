#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 2 of track checkout-authorize_20261027 (US-13.1/13.2/12.1)
# against a real sbt run instance + Postgres: POST /payments/methods tokenizes
# a card into payment_methods (token + display fields + billing address), the
# PAN appears nowhere in the database or the response, invalid input gets a
# 400, and the browser-facing CORS preflight is allowed for gshop.
#
# Usage: ./scripts/verify-payment-methods.sh

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
sbt run </dev/null >/tmp/payment-service-verify-payment-methods.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: payment-service ready on :8080 (config incl. payment-gateway loaded)"

echo
echo "3. POST /payments/methods with a valid card..."
PAN="4242424242424242"
resp="$(curl -s -w '\n%{http_code}' -X POST -H 'Content-Type: application/json' \
  -d "{\"customerId\":\"verify-methods\",\"cardNumber\":\"4242 4242 4242 4242\",\"expiry\":\"12/99\",\"billingAddress\":{\"line1\":\"1 High St\",\"line2\":null,\"city\":\"London\",\"postalCode\":\"SW1A 1AA\",\"country\":\"GB\"}}" \
  http://localhost:8080/payments/methods)"
code="$(echo "$resp" | tail -1)"
body="$(echo "$resp" | head -1)"
check "returns 201" "201" "$code"
id="$(echo "$body" | sed -E 's/.*"id":"([^"]+)".*/\1/')"
check "response brand" "visa" "$(echo "$body" | sed -E 's/.*"brand":"([^"]+)".*/\1/')"
check "response last4" "4242" "$(echo "$body" | sed -E 's/.*"last4":"([^"]+)".*/\1/')"
check "response has no token" "0" "$(echo "$body" | grep -c 'tok_' || true)"

echo
echo "4. Row stored with token + billing address, PAN nowhere in the DB..."
row="$(docker compose exec -T postgres psql -U payment -d payment -tAc \
  "select card_token || '|' || (billing_address->>'city') from payment_methods where id = '${id}'")"
check "stored token|city" "tok_visa_4242|London" "$row"
pan_hits="$(docker compose exec -T postgres pg_dump -U payment -d payment --data-only | grep -c "$PAN" || true)"
check "PAN occurrences in pg_dump" "0" "$pan_hits"
log_hits="$(grep -c -e "$PAN" -e '4242 4242 4242 4242' /tmp/payment-service-verify-payment-methods.log || true)"
check "PAN occurrences in service log" "0" "$log_hits"

echo
echo "5. Invalid card / expired card / bad billing country -> 400..."
for payload in \
  '"cardNumber":"4242424242424241","expiry":"12/99","billingAddress":{"line1":"1","line2":null,"city":"L","postalCode":"P","country":"GB"}' \
  '"cardNumber":"4242424242424242","expiry":"01/20","billingAddress":{"line1":"1","line2":null,"city":"L","postalCode":"P","country":"GB"}' \
  '"cardNumber":"4242424242424242","expiry":"12/99","billingAddress":{"line1":"1","line2":null,"city":"L","postalCode":"P","country":"gb"}'; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    -d "{\"customerId\":\"verify-methods\",${payload}}" http://localhost:8080/payments/methods)"
  check "invalid payload -> 400" "400" "$code"
done

echo
echo "6. CORS preflight for POST /payments/methods..."
preflight="$(curl -s -D - -o /dev/null -X OPTIONS \
  -H 'Origin: http://localhost:5173' \
  -H 'Access-Control-Request-Method: POST' \
  -H 'Access-Control-Request-Headers: Content-Type' \
  http://localhost:8080/payments/methods)"
check "preflight allows origin" "1" "$(echo "$preflight" | grep -ci 'access-control-allow-origin' || true)"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
