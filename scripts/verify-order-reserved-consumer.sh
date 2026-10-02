#!/usr/bin/env bash
set -euo pipefail

# Verifies US-6.1 (track order-reserved-consumer_20261002) end-to-end: brings
# up Postgres + Kafka + the real service, publishes a synthetic order.reserved
# event directly onto the topic (no live order-service needed, per the
# stub/fan-out pattern), and confirms the real consumer creates+settles a
# Payment and produces the right payment.settled message - not just that the
# unit/integration test suite passes.
#
# Usage: ./scripts/verify-order-reserved-consumer.sh

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

produce_one() {
  local topic="$1" json="$2"
  echo "$json" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$topic" >/dev/null 2>&1
}

# Consumes up to one message from a topic, printing it (empty if none within
# the timeout). Runs the broker's own console consumer inside the container.
consume_one() {
  local topic="$1"
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$topic" \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 15000 2>/dev/null || true
}

echo "1. docker compose up -d (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres + kafka starting"

echo
echo "2. Pre-create order.reserved (avoids a cold-subscription race: the"
echo "   consumer subscribes at startup, and auto-created-by-producer topics"
echo "   aren't always picked up by an already-subscribed consumer in time)..."
for _ in $(seq 1 30); do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic order.reserved --partitions 1 --replication-factor 1 \
    >/dev/null 2>&1 && break
  sleep 1
done
echo "   OK: order.reserved topic created"

echo
echo "3. Starting payment-service (sbt run) in the background..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >/tmp/payment-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: payment-service ready on :8080"

echo
echo "4. Publish a synthetic order.reserved event and confirm payment.settled..."
order_id="verify-order-$(date +%s)"
produce_one order.reserved "{\"orderId\":\"${order_id}\",\"customerId\":\"cust-verify\",\"totalCents\":4999,\"timestamp\":\"2026-01-01T00:00:00Z\"}"
settled_message="$(consume_one payment.settled)"
echo "   message: ${settled_message:-<none>}"
settled_order_id="$(echo "$settled_message" | python3 -c "import json,sys; print(json.load(sys.stdin).get('orderId',''))" 2>/dev/null || echo "")"
settled_amount="$(echo "$settled_message" | python3 -c "import json,sys; print(json.load(sys.stdin).get('amountCents',''))" 2>/dev/null || echo "")"
payment_id="$(echo "$settled_message" | python3 -c "import json,sys; print(json.load(sys.stdin).get('paymentId',''))" 2>/dev/null || echo "")"
check "payment.settled message has correct orderId" "$order_id" "$settled_order_id"
check "payment.settled message has correct amountCents" "4999" "$settled_amount"

echo
echo "5. Confirm the Payment record itself via GET /payments/{id}..."
if [ -n "$payment_id" ]; then
  get_response="$(curl -s http://localhost:8080/payments/"$payment_id")"
  echo "   response: $get_response"
  payment_status="$(echo "$get_response" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")"
  check "Payment status is settled" "settled" "$payment_status"
else
  echo "   FAIL: no paymentId extracted from payment.settled message, skipping GET check" >&2
  FAILED=1
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
