#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 1 of track checkout-authorize_20261027 (ADR 0011) against a
# real sbt run instance + Postgres: the service boots with the new
# payment-gateway.simulated config block, Flyway applies V4, and the new
# `authorized` status round-trips through PATCH /payments/{id} (accepted by
# both PaymentStatus and the widened payment_status_check). Address/Card/
# gateway logic has no HTTP surface yet - covered by their unit suites.
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
echo "4. A payment can be moved to 'authorized'..."
id="$(curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"orderId":"verify-foundations","amountCents":100}' \
  http://localhost:8080/payments | sed -E 's/.*"id":"([^"]+)".*/\1/')"
patched="$(curl -s -X PATCH -H 'Content-Type: application/json' \
  -d '{"status":"authorized"}' "http://localhost:8080/payments/${id}")"
status="$(echo "$patched" | sed -E 's/.*"status":"([^"]+)".*/\1/')"
check "PATCH status=authorized echoes authorized" "authorized" "$status"
stored="$(docker compose exec -T postgres psql -U payment -d payment -tAc \
  "select status from payment where id = '${id}'")"
check "row stored as authorized" "authorized" "$stored"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
