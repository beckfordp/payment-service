#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 2 of US-6.1 (Event payloads + publisher): PaymentEventPublisher
# itself is already covered end-to-end against a real Testcontainers Kafka by
# PaymentEventPublisherSuite (publishSettled/publishFailed each produce exactly
# one correctly-shaped message, and the bounded-retry-then-drop path is
# exercised against an unreachable broker) - it isn't wired into any live HTTP
# or Kafka-consumer code path yet (that's Phase 3's OrderReservedConsumer), so
# there's nothing additional to drive end-to-end here. The one thing worth
# confirming live is that payment-service still boots cleanly with the new
# PaymentEventPublisher/PaymentEvents code present alongside real Postgres +
# Kafka.
#
# Usage: ./scripts/verify-kafka-publisher.sh

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

echo "1. docker compose up -d (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres + kafka starting"

echo
echo "2. Starting payment-service (sbt run) - confirms it still boots cleanly with PaymentEventPublisher/PaymentEvents present..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >/tmp/payment-service-verify.log 2>&1 &
SBT_PID=$!
if wait_ready; then
  check "payment-service booted and ready" "200" "200"
else
  echo "   FAIL: payment-service failed to start - check /tmp/payment-service-verify.log" >&2
  tail -40 /tmp/payment-service-verify.log >&2 || true
  FAILED=1
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
