#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 1 of track checkout-authorize_20261027 (ADR 0011) against a
# real sbt run instance + Postgres: the service boots with the new
# payment-gateway.simulated config block, Flyway applies V4, and the new
# `authorized` status is accepted by the widened payment_status_check and read
# back through GET /payments/{id} (PaymentStatus decodes it). Status is
# server-owned since the 2026-10-29 review fixes - no PATCH - so the row is
# moved directly in Postgres.
#
# Usage: ./scripts/verify-domain-foundations.sh

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
sbt run </dev/null >/tmp/payment-service-verify-domain-foundations.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: payment-service ready on :8080 (config incl. payment-gateway loaded)"

echo
echo "3. Flyway applied V4__add_authorized_status..."
v4="$(docker compose exec -T postgres psql -U payment -d payment -tAc \
  "select count(*) from flyway_schema_history where version = '4' and success")"
check "V4 recorded in flyway_schema_history" "1" "$v4"

echo
echo "4. 'authorized' is a valid stored status..."
id="$(curl -s -X POST -H 'Content-Type: application/json' \
  -d "{\"orderId\":\"verify-foundations-$(date +%s)\",\"amountCents\":100}" \
  http://localhost:8080/payments | sed -E 's/.*"id":"([^"]+)".*/\1/')"
docker compose exec -T postgres psql -U payment -d payment -tAc \
  "update payment set status = 'authorized' where id = '${id}'" >/dev/null
read_back="$(curl -s "http://localhost:8080/payments/${id}")"
status="$(echo "$read_back" | sed -E 's/.*"status":"([^"]+)".*/\1/')"
check "GET reads back status=authorized" "authorized" "$status"
patch_code="$(curl -s -o /dev/null -w '%{http_code}' -X PATCH \
  -H 'Content-Type: application/json' -d '{"status":"captured"}' \
  "http://localhost:8080/payments/${id}")"
check "PATCH is gone (404/405)" "yes" \
  "$([ "$patch_code" = 404 ] || [ "$patch_code" = 405 ] && echo yes || echo "no: $patch_code")"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
