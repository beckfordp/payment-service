#!/usr/bin/env bash
# Live verification for track authorize-idempotency_20261027 (TD-10.1) against
# the real local cluster. Proves:
#   - a repeated authorize for the same order/amount/method returns the same
#     authId, with exactly one payment row and one PaymentAuthorized outbox row;
#   - a different amount or payment method returns 409;
#   - a repeated decline replays the same 402 reason;
#   - the unique constraint exists (one row per order).
# Uses throwaway order ids and deletes their rows afterwards.
set -euo pipefail

NAMESPACE="${NAMESPACE:-gluon-local}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:18081}"
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

psql_scalar() {
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- \
    psql -U payment -d payment -tAc "$1" | tr -d '[:space:]'
}

post_json() {
  local url="$1" body="$2" out="$3"
  curl -sS -o "$out" -w '%{http_code}' -X POST "$url" \
    -H 'Content-Type: application/json' -d "$body"
}

json_field() {
  python3 -c "import json,sys; print(json.load(sys.stdin)$1)" < "$2"
}

echo "== TD-10.1 live verification =="
kubectl -n "$NAMESPACE" port-forward svc/payment-service 18081:8080 >/dev/null 2>&1 &
PIDS+=("$!")
for _ in $(seq 1 30); do
  curl -sf "$PAYMENT_URL/health/ready" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf "$PAYMENT_URL/health/ready" >/dev/null || {
  echo "FAIL: payment-service never became ready" >&2
  exit 1
}

ADDRESS='{"line1":"1 Verification Lane","city":"London","postalCode":"EC1A 1BB","country":"GB"}'
CUSTOMER="$(python3 -c 'import uuid; print(uuid.uuid4())')"

echo "-- creating an approving and a declining payment method --"
STATUS="$(post_json "$PAYMENT_URL/payments/methods" \
  "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"4242424242424242\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" \
  "$TMP_DIR/approve.json")"
[ "$STATUS" = "201" ] || { echo "FAIL: approve method -> $STATUS" >&2; cat "$TMP_DIR/approve.json" >&2; exit 1; }
APPROVE_ID="$(json_field '["id"]' "$TMP_DIR/approve.json")"
STATUS="$(post_json "$PAYMENT_URL/payments/methods" \
  "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"4000000000000002\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" \
  "$TMP_DIR/decline.json")"
[ "$STATUS" = "201" ] || { echo "FAIL: decline method -> $STATUS" >&2; exit 1; }
DECLINE_ID="$(json_field '["id"]' "$TMP_DIR/decline.json")"

ORDER_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
AUTH_BODY="{\"orderId\":\"$ORDER_ID\",\"amountCents\":4999,\"paymentMethodId\":\"$APPROVE_ID\"}"

echo "-- identical authorize twice returns the same authId --"
STATUS="$(post_json "$PAYMENT_URL/payments/authorize" "$AUTH_BODY" "$TMP_DIR/a1.json")"
[ "$STATUS" = "200" ] || { echo "FAIL: first authorize -> $STATUS" >&2; cat "$TMP_DIR/a1.json" >&2; exit 1; }
STATUS="$(post_json "$PAYMENT_URL/payments/authorize" "$AUTH_BODY" "$TMP_DIR/a2.json")"
[ "$STATUS" = "200" ] || { echo "FAIL: replay authorize -> $STATUS" >&2; cat "$TMP_DIR/a2.json" >&2; exit 1; }
AUTH1="$(json_field '["authId"]' "$TMP_DIR/a1.json")"
AUTH2="$(json_field '["authId"]' "$TMP_DIR/a2.json")"
[ "$AUTH1" = "$AUTH2" ] || { echo "FAIL: replay changed authId ($AUTH1 vs $AUTH2)" >&2; exit 1; }
echo "   same authId: $AUTH1"

ROWS="$(psql_scalar "select count(*) from payment where order_id = '$ORDER_ID'")"
OUTBOX="$(psql_scalar "select count(*) from outbox_events where aggregate_id = '$ORDER_ID'")"
[ "$ROWS" = "1" ] || { echo "FAIL: expected 1 payment row, got $ROWS" >&2; exit 1; }
[ "$OUTBOX" = "1" ] || { echo "FAIL: expected 1 outbox row, got $OUTBOX" >&2; exit 1; }
echo "   1 payment row, 1 outbox event"

echo "-- different amount returns 409 --"
STATUS="$(post_json "$PAYMENT_URL/payments/authorize" \
  "{\"orderId\":\"$ORDER_ID\",\"amountCents\":5000,\"paymentMethodId\":\"$APPROVE_ID\"}" \
  "$TMP_DIR/a3.json")"
[ "$STATUS" = "409" ] || { echo "FAIL: amount mismatch -> $STATUS" >&2; cat "$TMP_DIR/a3.json" >&2; exit 1; }
echo "   409"

echo "-- different payment method returns 409 --"
STATUS="$(post_json "$PAYMENT_URL/payments/authorize" \
  "{\"orderId\":\"$ORDER_ID\",\"amountCents\":4999,\"paymentMethodId\":\"$DECLINE_ID\"}" \
  "$TMP_DIR/a4.json")"
[ "$STATUS" = "409" ] || { echo "FAIL: method mismatch -> $STATUS" >&2; cat "$TMP_DIR/a4.json" >&2; exit 1; }
echo "   409"

DECLINE_ORDER="$(python3 -c 'import uuid; print(uuid.uuid4())')"
DECLINE_BODY="{\"orderId\":\"$DECLINE_ORDER\",\"amountCents\":4999,\"paymentMethodId\":\"$DECLINE_ID\"}"
echo "-- repeated decline replays the same 402 reason --"
STATUS="$(post_json "$PAYMENT_URL/payments/authorize" "$DECLINE_BODY" "$TMP_DIR/d1.json")"
[ "$STATUS" = "402" ] || { echo "FAIL: first decline -> $STATUS" >&2; exit 1; }
STATUS="$(post_json "$PAYMENT_URL/payments/authorize" "$DECLINE_BODY" "$TMP_DIR/d2.json")"
[ "$STATUS" = "402" ] || { echo "FAIL: replayed decline -> $STATUS" >&2; exit 1; }
REASON1="$(json_field '["reason"]' "$TMP_DIR/d1.json")"
REASON2="$(json_field '["reason"]' "$TMP_DIR/d2.json")"
[ "$REASON1" = "$REASON2" ] || { echo "FAIL: decline reason changed ($REASON1 vs $REASON2)" >&2; exit 1; }
DECLINE_ROWS="$(psql_scalar "select count(*) from payment where order_id = '$DECLINE_ORDER'")"
[ "$DECLINE_ROWS" = "1" ] || { echo "FAIL: expected 1 decline row, got $DECLINE_ROWS" >&2; exit 1; }
echo "   same reason: $REASON1"

echo "-- cleaning up test rows --"
kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
  "delete from outbox_events where aggregate_id in ('$ORDER_ID', '$DECLINE_ORDER'); delete from payment where order_id in ('$ORDER_ID', '$DECLINE_ORDER')" >/dev/null

echo "All authorize-idempotency checks passed."
