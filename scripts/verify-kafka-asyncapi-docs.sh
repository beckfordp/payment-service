#!/usr/bin/env bash
set -euo pipefail

# Verifies payment-service's AsyncAPI documentation end-to-end, generated via
# the sbt-purekafka-docs plugin (asyncApiGenerateSpec/asyncApiGenerate,
# auto-chained after Compile/compile) — generation needs no running server at
# all, since AsyncApiSpecWriter reads AsyncApiChannels directly via
# reflection rather than curling a live HTTP route. Confirms both registered
# event types show up in the generated spec/HTML *and* the live-served raw
# spec + static HTML routes, so both halves (build-time generation and
# runtime serving) are checked, not just that a process started or a task
# exited 0. Adapted from order-service's/inventory-service's own
# verify-kafka-asyncapi-docs.sh.
#
# No order-service produces a real OrderReserved event here - payment-service
# starts with no inbound traffic, same as inventory-service's own verify
# script; this only checks the AsyncAPI docs surface, not checkout flow.
#
# Usage: ./scripts/verify-kafka-asyncapi-docs.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PAYMENT_SERVICE_URL="http://localhost:8080"
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
    status="$(curl -s -o /dev/null -w '%{http_code}' "${PAYMENT_SERVICE_URL}/health" || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: payment-service did not become ready in time." >&2
  return 1
}

echo "1. sbt asyncApiGenerate (no server needed - AsyncApiSpecWriter reads AsyncApiChannels directly)..."
sbt asyncApiGenerate

echo
echo "2. Checking the generated raw spec for both registered event types..."
SPEC_FILE="target/asyncapi-docs-spec/asyncapi.json"
for needle in "order-events" "payment-events" "OrderReserved" "PaymentSettled"; do
  if ! grep -q "$needle" "$SPEC_FILE"; then
    echo "FAIL: generated spec missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: generated spec contains \"${needle}\""
  fi
done

echo
echo "3. Checking the generated static HTML for the same content..."
HTML_FILE="target/asyncapi-docs/index.html"
for needle in "OrderReserved" "PaymentSettled"; do
  if ! grep -q "$needle" "$HTML_FILE"; then
    echo "FAIL: generated HTML missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: generated HTML contains \"${needle}\""
  fi
done

echo
echo "4. docker compose up -d (fresh volumes, postgres + kafka)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   waiting for postgres to be healthy..."
for _ in $(seq 1 60); do
  health="$(docker compose ps --format json postgres 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('Health',''))" 2>/dev/null || true)"
  [ "$health" = "healthy" ] && break
  sleep 1
done

echo
echo "5. Starting payment-service (sbt run)..."
sbt run >/tmp/payment-service-asyncapi-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: payment-service ready on :8080"

echo
echo "6. Checking the live raw-spec route (/docs/asyncapi.json) for the same content..."
for needle in "OrderReserved" "PaymentSettled"; do
  if ! curl -s "${PAYMENT_SERVICE_URL}/docs/asyncapi.json" | grep -q "$needle"; then
    echo "FAIL: live raw-spec route missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: live raw-spec route contains \"${needle}\""
  fi
done

echo
echo "7. Checking the live static HTML route (/docs/asyncapi/) for the same content..."
if ! curl -s "${PAYMENT_SERVICE_URL}/docs/asyncapi/index.html" | grep -q "OrderReserved"; then
  echo "FAIL: live static HTML route missing expected content: OrderReserved" >&2
  FAILED=1
else
  echo "   OK: live static HTML route contains \"OrderReserved\""
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
  exit 0
else
  echo "One or more checks failed — see FAIL lines above." >&2
  exit 1
fi
