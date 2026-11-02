#!/usr/bin/env bash
# Live verification for Checkout Journey 5 (US-20.1/20.3/20.4/20.5, US-9.2-9.5/9.9).
# Places a real authorized order, waits for its fulfilment task, forces the
# authorization past its expiry in Postgres, then proves the sweep and the
# shared OrderCancelled fan-out:
#   payment voided (reason authorization_expired) + one
#   PaymentAuthorizationExpired outbox row; order cancelled with reason
#   auth_expired; inventory released; fulfilment CANCELLED; notification
#   expiry copy logged. A second sweep tick publishes nothing. Cleans up.
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
payment_row() {
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -tAc \
    "select status || '|' || coalesce(decline_reason, '') from payment where order_id = '$1'" | tr -d '[:space:]'
}
payment_expired_events() {
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -tAc \
    "select count(*) from outbox_events where aggregate_id = '$1' and payload->>'eventType' = 'PaymentAuthorizationExpired'" | tr -d '[:space:]'
}
order_status() { curl -sf "$ORDER_URL/orders/$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])'; }
order_cancelled_reason() {
  kubectl exec -n "$NAMESPACE" deploy/order-postgres -- psql -U order -d order -tAc \
    "select coalesce((select payload->'payload'->>'reason' from outbox_events where aggregate_id = '$1' and payload->>'eventType' = 'OrderCancelled' limit 1), '')" | tr -d '[:space:]'
}
stocked_sku() {
  curl -sf "$INVENTORY_URL/inventorys" | python3 -c '
import json,sys
rows=json.load(sys.stdin); stocked=[r for r in rows if r["quantityAvailable"]>0]
if not stocked: sys.exit("no stocked sku")
print(stocked[0]["sku"])'
}
reserved_for_sku() {
  curl -sf "$INVENTORY_URL/inventorys?sku=$1" | python3 -c '
import json,sys
rows=json.load(sys.stdin)
print(rows[0]["quantityReserved"] if rows else -1)'
}
fulfilment_state() {
  curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=CANCELLED" | grep -q "$1" && echo CANCELLED || echo not-cancelled
}
wait_for() {
  local label="$1" expected="$2" actual_fn="$3"
  for _ in $(seq 1 60); do
    [ "$("$actual_fn" "$4")" = "$expected" ] && return 0
    sleep 1
  done
  echo "FAIL: $label never became '$expected' (is '$("$actual_fn" "$4")')" >&2
  exit 1
}
cleanup_order() {
  local order_id="$1"
  curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$order_id/release" >/dev/null || true
  curl -sf -X DELETE "$ORDER_URL/orders/$order_id" >/dev/null || true
  kubectl exec -n "$NAMESPACE" deploy/order-postgres -- psql -U order -d order -q -c \
    "delete from outbox_events where aggregate_id = '$order_id'" >/dev/null
  kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
    "delete from outbox_events where aggregate_id = '$order_id'; delete from fulfilment_task where order_id = '$order_id'" >/dev/null
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
    "delete from outbox_events where aggregate_id = '$order_id'; delete from payment where order_id = '$order_id'" >/dev/null
}

echo "== Journey 5 live verification: authorization expiry =="
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
SKU="$(stocked_sku)"
BEFORE_RESERVED="$(reserved_for_sku "$SKU")"

echo "-- authorized order --"
STATUS="$(post_json "$PAYMENT_URL/payments/methods" \
  "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"4242424242424242\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" "$TMP_DIR/pm.json")"
[ "$STATUS" = "201" ] || { echo "FAIL: payment method -> $STATUS" >&2; cat "$TMP_DIR/pm.json" >&2; exit 1; }
PM="$(json_field '["id"]' "$TMP_DIR/pm.json")"
STATUS="$(post_json "$ORDER_URL/orders" \
  "{\"customerId\":\"$CUSTOMER\",\"items\":[{\"sku\":\"$SKU\",\"productName\":\"Verify Watch\",\"unitPriceCents\":100,\"quantity\":1}],\"shippingAddress\":$ADDRESS,\"paymentMethodId\":\"$PM\"}" "$TMP_DIR/order.json")"
[ "$STATUS" = "201" ] || { echo "FAIL: order -> $STATUS" >&2; cat "$TMP_DIR/order.json" >&2; exit 1; }
ORDER_ID="$(json_field '["id"]' "$TMP_DIR/order.json")"
[ "$(payment_row "$ORDER_ID")" = "authorized|" ] || { echo "FAIL: payment not authorized" >&2; exit 1; }
echo "   order $ORDER_ID authorized"

echo "-- wait for the fulfilment task before expiring --"
for _ in $(seq 1 30); do
  curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=PENDING" | grep -q "$ORDER_ID" && break
  sleep 1
done
curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=PENDING" | grep -q "$ORDER_ID" \
  || { echo "FAIL: no PENDING fulfilment task for $ORDER_ID" >&2; exit 1; }

echo "-- force the authorization past expiresAt --"
kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
  "update payment set expires_at = now() - interval '1 minute' where order_id = '$ORDER_ID'" >/dev/null

wait_for "payment status|reason" "voided|authorization_expired" payment_row "$ORDER_ID"
echo "   payment voided (authorization_expired)"
wait_for "order status" cancelled order_status "$ORDER_ID"
echo "   order cancelled"
[ "$(order_cancelled_reason "$ORDER_ID")" = "auth_expired" ] \
  || { echo "FAIL: OrderCancelled reason is '$(order_cancelled_reason "$ORDER_ID")', expected auth_expired" >&2; exit 1; }
echo "   OrderCancelled reason=auth_expired"
[ "$(payment_expired_events "$ORDER_ID")" = "1" ] \
  || { echo "FAIL: expected one PaymentAuthorizationExpired outbox row" >&2; exit 1; }
echo "   one PaymentAuthorizationExpired outbox row"

wait_for "inventory reserved" "$BEFORE_RESERVED" reserved_for_sku "$SKU"
echo "   inventory released back to $BEFORE_RESERVED reserved"
wait_for "fulfilment state" CANCELLED fulfilment_state "$ORDER_ID"
echo "   fulfilment task CANCELLED"

echo "-- notification expiry copy --"
for _ in $(seq 1 30); do
  kubectl logs -n "$NAMESPACE" deploy/notification-service --since=10m 2>/dev/null \
    | grep -q "$ORDER_ID" && break
  sleep 1
done
kubectl logs -n "$NAMESPACE" deploy/notification-service --since=10m 2>/dev/null \
  | grep "$ORDER_ID" | grep -qi "payment authorization expired" \
  || { echo "FAIL: no expiry copy logged for $ORDER_ID" >&2; exit 1; }
echo "   email body says the authorization expired"

echo "-- a second sweep tick publishes nothing --"
sleep 12
[ "$(payment_expired_events "$ORDER_ID")" = "1" ] \
  || { echo "FAIL: duplicate PaymentAuthorizationExpired row after a second tick" >&2; exit 1; }
echo "   still one PaymentAuthorizationExpired outbox row"

cleanup_order "$ORDER_ID"
echo "== Journey 5 live verification passed =="
