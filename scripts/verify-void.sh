#!/usr/bin/env bash
# Live verification for track void-authorization_20261028 (US-18.2).
# Authorized order -> fulfilment forced failure -> payment row `voided`;
# a duplicate DispatchFailed leaves it voided (no double void). Cleanup.
set -euo pipefail

NAMESPACE="${NAMESPACE:-gluon-local}"
ORDER_URL="${ORDER_URL:-http://localhost:18080}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:18081}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:18082}"
FULFILMENT_URL="${FULFILMENT_URL:-http://localhost:18085}"
PIDS=()
TMP_DIR="$(mktemp -d)"
cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

start_pf() { kubectl -n "$NAMESPACE" port-forward "svc/$1" "$2:8080" >/dev/null 2>&1 & PIDS+=("$!"); }
wait_ready() {
  for _ in $(seq 1 30); do curl -sf "$1/health/ready" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "FAIL: $2 never became ready" >&2; exit 1
}
post_json() { curl -sS -o "$3" -w '%{http_code}' -X POST "$1" -H 'Content-Type: application/json' -d "$2"; }
json_field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)" < "$2"; }
payment_status() {
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- \
    psql -U payment -d payment -tAc \
    "select status from payment where order_id = '$1'" | tr -d '[:space:]'
}

echo "== US-18.2 live verification =="
start_pf order-service 18080
start_pf payment-service 18081
start_pf inventory-service 18082
start_pf fulfilment-service 18085
wait_ready "$ORDER_URL" order-service
wait_ready "$PAYMENT_URL" payment-service
wait_ready "$INVENTORY_URL" inventory-service
wait_ready "$FULFILMENT_URL" fulfilment-service

ADDRESS='{"line1":"1 Verification Lane","city":"London","postalCode":"EC1A 1BB","country":"GB"}'
CUSTOMER="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SKU="$(curl -sf "$INVENTORY_URL/inventorys" | python3 -c '
import json,sys
rows=json.load(sys.stdin); stocked=[r for r in rows if r["quantityAvailable"]>0]
if not stocked: sys.exit("no stocked sku")
print(stocked[0]["sku"])')"

echo "-- authorized order --"
STATUS="$(post_json "$PAYMENT_URL/payments/methods" \
  "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"4242424242424242\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" "$TMP_DIR/pm.json")"
[ "$STATUS" = "201" ] || { echo "FAIL: payment method -> $STATUS" >&2; exit 1; }
PM="$(json_field '["id"]' "$TMP_DIR/pm.json")"
STATUS="$(post_json "$ORDER_URL/orders" \
  "{\"customerId\":\"$CUSTOMER\",\"items\":[{\"sku\":\"$SKU\",\"productName\":\"Verify Watch\",\"unitPriceCents\":100,\"quantity\":1}],\"shippingAddress\":$ADDRESS,\"paymentMethodId\":\"$PM\"}" "$TMP_DIR/order.json")"
[ "$STATUS" = "201" ] || { echo "FAIL: order -> $STATUS" >&2; cat "$TMP_DIR/order.json" >&2; exit 1; }
ORDER_ID="$(json_field '["id"]' "$TMP_DIR/order.json")"
[ "$(payment_status "$ORDER_ID")" = "authorized" ] || { echo "FAIL: payment not authorized" >&2; exit 1; }
echo "   payment authorized for $ORDER_ID"

echo "-- forced fulfilment failure voids the hold --"
for _ in $(seq 1 30); do
  curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=PENDING" | grep -q "$ORDER_ID" && break
  sleep 1
done
STATUS="$(post_json "$FULFILMENT_URL/fulfilment-tasks/$ORDER_ID/simulate-next" \
  '{"forceFailure":true,"reason":"Simulated failure"}' "$TMP_DIR/fail.json")"
[ "$STATUS" = "200" ] || { echo "FAIL: forced failure -> $STATUS" >&2; exit 1; }
for _ in $(seq 1 20); do
  [ "$(payment_status "$ORDER_ID")" = "voided" ] && break
  sleep 1
done
[ "$(payment_status "$ORDER_ID")" = "voided" ] || { echo "FAIL: payment not voided" >&2; exit 1; }
echo "   payment voided"

echo "-- duplicate DispatchFailed is harmless --"
EVENT="$(python3 -c "
import json,uuid
print(json.dumps({'eventId':str(uuid.uuid4()),'eventType':'DispatchFailed','timestamp':'2026-10-28T12:00:00Z','source':'verify-script','correlationId':'$ORDER_ID','payload':{'orderId':'$ORDER_ID','reason':'duplicate'}}))")"
printf '%s\n' "$EVENT" | kubectl exec -i -n "$NAMESPACE" deploy/kafka -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic fulfilment-events >/dev/null
sleep 3
[ "$(payment_status "$ORDER_ID")" = "voided" ] || { echo "FAIL: duplicate changed the status" >&2; exit 1; }
ROWS="$(kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -tAc \
  "select count(*) from payment where order_id = '$ORDER_ID'" | tr -d '[:space:]')"
[ "$ROWS" = "1" ] || { echo "FAIL: expected 1 payment row, got $ROWS" >&2; exit 1; }
echo "   still voided, one payment row"

echo "-- cleaning up --"
curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$ORDER_ID/release" >/dev/null || true
curl -sf -X DELETE "$ORDER_URL/orders/$ORDER_ID" >/dev/null || true
kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
  "delete from outbox_events where aggregate_id = '$ORDER_ID'; delete from fulfilment_task where order_id = '$ORDER_ID'" >/dev/null
kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
  "delete from outbox_events where aggregate_id = '$ORDER_ID'; delete from payment where order_id = '$ORDER_ID'" >/dev/null

echo "All US-18.2 live checks passed."
