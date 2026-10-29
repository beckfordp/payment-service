#!/usr/bin/env bash
set -euo pipefail

# Verifies payment-service's AsyncAPI documentation end-to-end, generated via
# the sbt-purekafka-docs plugin (asyncApiGenerateSpec/asyncApiGenerate,
# auto-chained after Compile/compile) — generation needs no running server at
# all, since AsyncApiSpecWriter reads AsyncApiChannels directly via
# reflection rather than curling a live HTTP route. Confirms all four
# registered publish event types show up in the generated spec/HTML *and* the
# live-served raw spec + static HTML routes, so both halves (build-time
# generation and runtime serving) are checked, not just that a process started
# or a task exited 0. Adapted from order-service's/inventory-service's own
# verify-kafka-asyncapi-docs.sh.
#
# payment-service has no Kafka consumers and starts with no inbound traffic;
# this only checks the AsyncAPI docs surface, not checkout flow.
#
# Usage: ./scripts/verify-kafka-asyncapi-docs.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PAYMENT_SERVICE_URL="http://localhost:8080"
EVENT_TYPES=(
  "PaymentAuthorized"
  "PaymentCaptured"
  "PaymentAuthorizationExpired"
  "PaymentFailed"
)
FAILED=0
SBT_PID=""
SBT_LOG="$(mktemp -t payment-asyncapi-sbt-run.XXXXXX)"

cleanup() {
  echo
  echo "Cleaning up..."
  if [ -n "$SBT_PID" ]; then
    pkill -9 -P "$SBT_PID" >/dev/null 2>&1 || true
    kill -9 "$SBT_PID" >/dev/null 2>&1 || true
    wait "$SBT_PID" 2>/dev/null || true
  fi
  docker compose down -v >/dev/null 2>&1 || true
  rm -f "$SBT_LOG"
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
echo "2. Checking the generated raw spec for all four registered event types..."
SPEC_FILE="target/asyncapi-docs-spec/asyncapi.json"
for needle in "payment-events" "${EVENT_TYPES[@]}"; do
  if ! grep -q "$needle" "$SPEC_FILE"; then
    echo "FAIL: generated spec missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: generated spec contains \"${needle}\""
  fi
done
for retired in "OrderReserved" "PaymentSettled"; do
  if grep -q "$retired" "$SPEC_FILE"; then
    echo "FAIL: generated spec still contains retired event type: ${retired}" >&2
    FAILED=1
  fi
done

echo
echo "3. Checking the generated static HTML for the same content..."
HTML_FILE="target/asyncapi-docs/index.html"
for needle in "${EVENT_TYPES[@]}"; do
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
(cd "$ROOT_DIR" && exec sbt -batch run >"$SBT_LOG" 2>&1 </dev/null) &
SBT_PID=$!
wait_ready
echo "   OK: payment-service ready on :8080"

echo
echo "6. Checking the live raw-spec route (/docs/asyncapi.json) for the same content..."
for needle in "${EVENT_TYPES[@]}"; do
  if ! curl -s "${PAYMENT_SERVICE_URL}/docs/asyncapi.json" | grep -q "$needle"; then
    echo "FAIL: live raw-spec route missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: live raw-spec route contains \"${needle}\""
  fi
done

echo
echo "7. Checking the live static HTML route (/docs/asyncapi/) for the same content..."
ok=0
for _ in $(seq 1 30); do
  if curl -s "${PAYMENT_SERVICE_URL}/docs/asyncapi/index.html" | grep -q "PaymentAuthorized"; then
    ok=1
    break
  fi
  sleep 1
done
if [ "$ok" -ne 1 ]; then
  echo "FAIL: live static HTML route missing expected content: PaymentAuthorized" >&2
  FAILED=1
else
  echo "   OK: live static HTML route contains \"PaymentAuthorized\""
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
  exit 0
else
  echo "One or more checks failed — see FAIL lines above." >&2
  exit 1
fi
