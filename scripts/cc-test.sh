#!/usr/bin/env bash
# Runs the Confluent Cloud tests C1-C5 and C7 (CloudIT) against the connector deployed by scripts/cc-deploy.sh.
# C6 (50 MB/s) is intentionally not part of this run.
# Usage: scripts/cc-test.sh                      all tests, in order
#        TESTS='CloudIT#c1*' scripts/cc-test.sh  a subset (tests build on each other, so C4/C5/C7 need C1-C3 first)
# The tests take a long time: new topics are only discovered after about 5 minutes, and every connector update or
# redeploy takes several minutes. The API secret stays in this process's environment; it is never written to disk.
set -euo pipefail
cd "$(dirname "$0")/.."

STATE=cc-state
j() { python3 -c "import json,sys; print(json.load(open('$1'))['$2'])"; }
ENV_ID=$(j $STATE/identity.json environment)
CLUSTER=$(j $STATE/identity.json cluster)
SA=$(j $STATE/identity.json service_account)
CONNECTOR_ID=$(j $STATE/connector-fanin-test-connector.json id)

# C1 needs topics that do NOT match the connector's regex, so the service account must be allowed to write to them.
confluent kafka acl create --allow --service-account "$SA" --operations write,describe \
  --topic fanin-test-nomatch. --prefix --cluster "$CLUSTER" --environment "$ENV_ID" >/dev/null 2>&1 || true

KEY_JSON=$(confluent api-key create --resource "$CLUSTER" --service-account "$SA" --environment "$ENV_ID" \
  --description "fanin-test key for cc-test (temporary)" -o json)
FANIN_API_KEY=$(echo "$KEY_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_key'])")
FANIN_API_SECRET=$(echo "$KEY_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_secret'])")
unset KEY_JSON
python3 - "$STATE/identity.json" "$FANIN_API_KEY" <<'EOF'
import json, sys
p, key = sys.argv[1], sys.argv[2]
d = json.load(open(p))
keys = d.setdefault("api_keys", [])
if key not in keys:
    keys.append(key)
json.dump(d, open(p, "w"))
EOF

FANIN_BOOTSTRAP=$(confluent kafka cluster describe "$CLUSTER" --environment "$ENV_ID" -o json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['endpoint'].split('://')[1])")
export FANIN_API_KEY FANIN_API_SECRET FANIN_BOOTSTRAP FANIN_ENV="$ENV_ID" FANIN_CLUSTER="$CLUSTER" \
  FANIN_CONNECTOR_ID="$CONNECTOR_ID"

set +e
mvn -B test -Dtest="${TESTS:-CloudIT}" -Dit.excludedGroups=none -Dgroups=cloud \
  -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | tee target/cc-test.log
STATUS=${PIPESTATUS[0]}
set -e
grep -E "^RESULT |Tests run:|BUILD|AssertionError|timeout waiting" target/cc-test.log | sed 's/^\[[A-Z]*\] //' \
  > docs/cc-test-results.txt || true
exit "$STATUS"
