#!/usr/bin/env bash
# Live verification for track capture-on-dispatch_20261028 (US-19.1).
# Dispatched order -> payment `captured` + one PaymentCaptured outbox row;
# a duplicate DispatchConfirmed is harmless (no double capture); a
# capture-decline card authorizes but fails at capture -> `failed` +
# PaymentFailed {reason: capture_failed}. Cleanup.
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
payment_reason() {
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- \
    psql -U payment -d payment -tAc \
    "select coalesce(decline_reason, '') from payment where order_id = '$1'" | tr -d '[:space:]'
}
outbox_count() {
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- \
    psql -U payment -d payment -tAc \
    "select count(*) from outbox_events where aggregate_id = '$1' and payload->>'eventType' = '$2'" | tr -d '[:space:]'
}
stocked_sku() {
  curl -sf "$INVENTORY_URL/inventorys" | python3 -c '
import json,sys
rows=json.load(sys.stdin); stocked=[r for r in rows if r["quantityAvailable"]>0]
if not stocked: sys.exit("no stocked sku")
print(stocked[0]["sku"])'
}
wait_payment_status() {
  for _ in $(seq 1 30); do
    [ "$(payment_status "$1")" = "$2" ] && return 0
    sleep 1
  done
  echo "FAIL: payment for $1 never became $2 (is $(payment_status "$1"))" >&2
  exit 1
}
drive_to_dispatch() {
  local order_id="$1"
  for _ in $(seq 1 30); do
    curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=PENDING" | grep -q "$order_id" && break
    sleep 1
  done
  for step in 1 2 3; do
    STATUS="$(post_json "$FULFILMENT_URL/fulfilment-tasks/$order_id/simulate-next" '{}' "$TMP_DIR/step.json")"
    [ "$STATUS" = "200" ] || { echo "FAIL: simulate-next step $step -> $STATUS" >&2; cat "$TMP_DIR/step.json" >&2; exit 1; }
  done
}
create_order() {
  local card="$1"
  STATUS="$(post_json "$PAYMENT_URL/payments/methods" \
    "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"$card\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" "$TMP_DIR/pm.json")"
  [ "$STATUS" = "201" ] || { echo "FAIL: payment method -> $STATUS" >&2; cat "$TMP_DIR/pm.json" >&2; exit 1; }
  local pm sku
  pm="$(json_field '["id"]' "$TMP_DIR/pm.json")"
  sku="$(stocked_sku)"
  STATUS="$(post_json "$ORDER_URL/orders" \
    "{\"customerId\":\"$CUSTOMER\",\"items\":[{\"sku\":\"$sku\",\"productName\":\"Verify Watch\",\"unitPriceCents\":100,\"quantity\":1}],\"shippingAddress\":$ADDRESS,\"paymentMethodId\":\"$pm\"}" "$TMP_DIR/order.json")"
  [ "$STATUS" = "201" ] || { echo "FAIL: order -> $STATUS" >&2; cat "$TMP_DIR/order.json" >&2; exit 1; }
  json_field '["id"]' "$TMP_DIR/order.json"
}
inject_dispatch_confirmed() {
  local order_id="$1" tracking="$2"
  python3 -c "
import json,uuid
print(json.dumps({'eventId':str(uuid.uuid4()),'eventType':'DispatchConfirmed','timestamp':'2026-10-28T12:00:00Z','source':'verify-script','correlationId':'$order_id','payload':{'orderId':'$order_id','trackingNo':'$tracking','items':[]}}))" |
    kubectl exec -i -n "$NAMESPACE" deploy/kafka -- \
      /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic fulfilment-events >/dev/null
}
cleanup_order() {
  local order_id="$1"
  curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$order_id/release" >/dev/null || true
  curl -sf -X DELETE "$ORDER_URL/orders/$order_id" >/dev/null || true
  kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
    "delete from outbox_events where aggregate_id = '$order_id'; delete from fulfilment_task where order_id = '$order_id'" >/dev/null
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
    "delete from outbox_events where aggregate_id = '$order_id'; delete from payment where order_id = '$order_id'" >/dev/null
}

echo "== US-19.1 live verification =="
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

echo "-- dispatch captures the hold --"
ORDER_OK="$(create_order 4242424242424242)"
[ "$(payment_status "$ORDER_OK")" = "authorized" ] || { echo "FAIL: payment not authorized" >&2; exit 1; }
drive_to_dispatch "$ORDER_OK"
wait_payment_status "$ORDER_OK" captured
[ "$(outbox_count "$ORDER_OK" PaymentCaptured)" = "1" ] || { echo "FAIL: expected one PaymentCaptured row" >&2; exit 1; }
echo "   payment captured for $ORDER_OK"

echo "-- duplicate DispatchConfirmed is harmless --"
inject_dispatch_confirmed "$ORDER_OK" "DUP-TRACK-1"
sleep 3
[ "$(payment_status "$ORDER_OK")" = "captured" ] || { echo "FAIL: duplicate changed the status" >&2; exit 1; }
[ "$(outbox_count "$ORDER_OK" PaymentCaptured)" = "1" ] || { echo "FAIL: duplicate published another PaymentCaptured" >&2; exit 1; }
echo "   still captured, one PaymentCaptured row"

echo "-- capture decline records PaymentFailed --"
ORDER_DECLINED="$(create_order 5555555555554444)"
[ "$(payment_status "$ORDER_DECLINED")" = "authorized" ] || { echo "FAIL: decline-card payment not authorized" >&2; exit 1; }
drive_to_dispatch "$ORDER_DECLINED"
wait_payment_status "$ORDER_DECLINED" failed
[ "$(payment_reason "$ORDER_DECLINED")" = "capture_failed" ] || { echo "FAIL: decline reason not capture_failed" >&2; exit 1; }
[ "$(outbox_count "$ORDER_DECLINED" PaymentFailed)" = "1" ] || { echo "FAIL: expected one PaymentFailed row" >&2; exit 1; }
echo "   capture declined, PaymentFailed published"

echo "-- cleaning up --"
cleanup_order "$ORDER_OK"
cleanup_order "$ORDER_DECLINED"
# Methods last: each order's payment row references its customer's method.
kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
  "delete from payment_methods where customer_id = '$CUSTOMER'" >/dev/null

echo "All US-19.1 live checks passed."
