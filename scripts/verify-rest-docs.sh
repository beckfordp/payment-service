#!/usr/bin/env bash
set -euo pipefail

# Verifies the REST docs convention end-to-end (track rest-docs_20261101,
# gluon TD-20.4): brings up Postgres + Kafka and a real sbt run instance,
# fetches the served OpenAPI spec at /docs/docs.yaml, and checks it carries
# every endpoint's summary/description/tag, the path parameter's description
# and example, the error descriptions and example bodies, DTO field
# descriptions and body examples, and that each endpoint lists only the error
# statuses it can return - not just that PaymentDocsSuite's in-process
# Docs.undocumented check passes.
#
# Requires: sbt, Docker, python3 with PyYAML, GITHUB_TOKEN/GITHUB_ACTOR set
# (purerestlib resolution).
#
# Usage: ./scripts/verify-rest-docs.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SBT_PID=""
SPEC="$(mktemp)"

cleanup() {
  echo
  echo "Cleaning up..."
  if [ -n "$SBT_PID" ]; then
    pkill -9 -P "$SBT_PID" >/dev/null 2>&1 || true
    kill -9 "$SBT_PID" >/dev/null 2>&1 || true
  fi
  docker compose down -v >/dev/null 2>&1 || true
  rm -f "$SPEC"
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 90); do
    status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: payment-service did not become ready in time." >&2
  return 1
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
sbt run </dev/null >/tmp/payment-service-verify-rest-docs.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: payment-service ready on :8080"

echo
echo "3. GET /docs/docs.yaml..."
status="$(curl -s -o "$SPEC" -w '%{http_code}' http://localhost:8080/docs/docs.yaml)"
[ "$status" = "200" ] || { echo "FAIL: /docs/docs.yaml returned $status" >&2; exit 1; }
echo "   OK: served (200)"

echo
echo "4. Checking the spec..."
python3 -I - "$SPEC" <<'PY'
import sys, yaml

spec = yaml.safe_load(open(sys.argv[1]))
paths, schemas = spec["paths"], spec["components"]["schemas"]
failed = False

def check(label, ok):
    global failed
    print(("   OK: " if ok else "   FAIL: ") + label, file=sys.stdout if ok else sys.stderr)
    failed |= not ok

# Statuses each endpoint may list (success + its narrowed errors; tapir adds
# 400 for undecodable inputs on its own).
allowed = {
    ("/payments", "post"): {"201", "400", "409"},
    ("/payments/{id}", "get"): {"200", "400", "404"},
    ("/payments/{id}", "delete"): {"204", "400", "404"},
    ("/payments/methods", "post"): {"201", "400"},
    ("/payments/authorize", "post"): {"200", "400", "402", "404", "409", "500"},
    ("/health", "get"): {"200"},
    ("/health/ready", "get"): {"200", "503"},
    ("/reconciliations", "get"): {"200", "400"},
    ("/reconciliations/{orderId}", "patch"): {"200", "400", "401", "404"},
}
for (path, method), statuses in allowed.items():
    op = paths.get(path, {}).get(method)
    name = f"{method.upper()} {path}"
    if op is None:
        check(f"{name} documented", False)
        continue
    check(f"{name} has summary/description/tag",
          bool(op.get("summary")) and bool(op.get("description")) and bool(op.get("tags")))
    for p in op.get("parameters", []):
        check(f"{name} parameter '{p['name']}' described with example",
              bool(p.get("description")) and ("example" in p or "example" in p.get("schema", {})))
    responses = op.get("responses", {})
    check(f"{name} lists only {sorted(statuses)} (got {sorted(responses)})",
          set(responses) <= statuses)
    for code, r in responses.items():
        if code.startswith("2"):
            continue
        check(f"{name} {code} described", bool(r.get("description")))

authorize = paths["/payments/authorize"]["post"]
check("POST /payments/authorize lists 402/404/409/500",
      {"402", "404", "409", "500"} <= set(authorize["responses"]))
check("POST /payments/authorize request body has an example",
      any("example" in c for c in authorize["requestBody"]["content"].values()))
check("POST /payments/authorize 402 has an example body",
      any("example" in c for c in authorize["responses"]["402"]["content"].values()))
create = paths["/payments"]["post"]
check("POST /payments request body has an example",
      any("example" in c for c in create["requestBody"]["content"].values()))
check("POST /payments 409 has an example body",
      any("example" in c for c in create["responses"]["409"]["content"].values()))
check("POST /payments/methods request body has an example",
      any("example" in c for c in paths["/payments/methods"]["post"]["requestBody"]["content"].values()))

for name in ["CreatePaymentRequest", "PaymentResponse", "CreatePaymentMethodRequest",
             "PaymentMethodResponse", "AuthorizePaymentRequest",
             "AuthorizePaymentResponse", "Address", "ErrorResponse",
             "DeclinedResponse", "ReconciliationResponse",
             "ResolveReconciliationRequest"]:
    props = schemas.get(name, {}).get("properties", {})
    # A field that only references another component is described by it
    # (tapir folds a single-use field's description into the component).
    def described(s):
        ref = s.get("$ref", "").rsplit("/", 1)[-1]
        return bool(s.get("description")) or bool(ref and schemas.get(ref, {}).get("description"))
    missing = [f for f, s in props.items() if not described(s)]
    check(f"schema {name} fields all described"
          + (f" (missing: {missing}: {[props[f] for f in missing]})" if missing else ""),
          bool(props) and not missing)

sys.exit(1 if failed else 0)
PY

echo
echo "All checks passed."
