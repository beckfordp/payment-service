#!/usr/bin/env bash
set -euo pipefail

# Verifies CORS support end-to-end (track cors_20261010, TD-1.3): brings up
# Postgres + Kafka and a real sbt run instance, drives real HTTP traffic
# through it to confirm Access-Control-Allow-Origin on a plain POST and the
# expected Allow-Methods/Allow-Headers on an OPTIONS preflight - not just
# that the test suite passes. Uses POST /payments rather than GET, since no
# bare GET /payments route exists (GET/PATCH/PUT/DELETE all require a path
# id). Also serves as TD-1.3's "confirm affected" step.
#
# Usage: ./scripts/verify-cors.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0
SBT_PID=""

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill "$SBT_PID" >/dev/null 2>&1 || true
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 90); do
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

check_contains() {
  local label="$1" needle="$2" haystack="$3"
  if echo "$haystack" | grep -qi "$needle"; then
    echo "   OK: ${label} (contains '${needle}')"
  else
    echo "   FAIL: ${label} - expected to contain '${needle}', got '${haystack}'" >&2
    FAILED=1
  fi
}

echo "1. docker compose up -d (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres/kafka starting"

echo
echo "2. Starting payment-service (sbt run) in the background..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >/tmp/payment-service-verify-cors.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: payment-service ready on :8080"

echo
echo "3. POST /payments with an Origin header receives Access-Control-Allow-Origin..."
post_headers="$(curl -s -D - -o /dev/null -X POST \
  -H 'Origin: http://example.com' \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"verify-cors","amountCents":100}' \
  http://localhost:8080/payments)"
post_status="$(echo "$post_headers" | head -1 | tr -d '\r' | awk '{print $2}')"
check "POST /payments returns 201" "201" "$post_status"
check_contains "POST /payments sends Access-Control-Allow-Origin" "access-control-allow-origin" "$post_headers"
credentials_count="$(echo "$post_headers" | grep -ci 'access-control-allow-credentials' || true)"
check "POST /payments omits Access-Control-Allow-Credentials" "0" "$credentials_count"

echo
echo "4. OPTIONS preflight for POST /payments receives the expected allow-methods/headers..."
preflight_headers="$(curl -s -D - -o /dev/null -X OPTIONS \
  -H 'Origin: http://example.com' \
  -H 'Access-Control-Request-Method: POST' \
  -H 'Access-Control-Request-Headers: Content-Type' \
  http://localhost:8080/payments)"
preflight_status="$(echo "$preflight_headers" | head -1 | tr -d '\r' | awk '{print $2}')"
check "OPTIONS /payments returns a success status" "200" "$preflight_status"
check_contains "OPTIONS /payments allows POST" "post" "$preflight_headers"
check_contains "OPTIONS /payments allows Content-Type" "content-type" "$preflight_headers"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
