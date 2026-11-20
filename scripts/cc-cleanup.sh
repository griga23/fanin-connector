#!/usr/bin/env bash
# Deletes everything created for the fan-in test on Confluent Cloud and prints what was deleted.
# Only touches resources whose names start with fanin-test- (and their API keys / ACLs / app log topics).
# Run only after the user has agreed.  Usage: scripts/cc-cleanup.sh [--yes]
set -uo pipefail
cd "$(dirname "$0")/.."

STATE=cc-state
ENV_ID=$(python3 -c "import json; print(json.load(open('$STATE/identity.json'))['environment'])")
CLUSTER=$(python3 -c "import json; print(json.load(open('$STATE/identity.json'))['cluster'])")
SA=$(python3 -c "import json; print(json.load(open('$STATE/identity.json'))['service_account'])")
KEY=$(python3 -c "import json; print(json.load(open('$STATE/identity.json'))['api_key'])")
PLUGIN=$(python3 -c "import json; print(json.load(open('$STATE/plugin.json'))['id'])" 2>/dev/null || true)

if [ "${1:-}" != "--yes" ]; then
  echo "This deletes, in $ENV_ID / $CLUSTER: all connectors named fanin-test-*, plugin $PLUGIN, topics fanin-test-*"
  echo "(and the connectors' app log topics), ACLs and API key $KEY of service account $SA, and the service account."
  read -r -p "Type yes to continue: " ans
  [ "$ans" = "yes" ] || exit 1
fi

DELETED=()
FAILED=0
del() { # description, command...
  local what=$1; shift
  if "$@" >/dev/null 2>&1; then DELETED+=("$what"); else echo "not deleted (missing or failed): $what"; FAILED=$((FAILED + 1)); fi
}

# 1. connectors (clcc-... ids whose name starts with fanin-test-)
confluent connect cluster list --cluster "$CLUSTER" --environment "$ENV_ID" -o json 2>/dev/null \
  | python3 -c "
import json, sys
for c in json.load(sys.stdin):
    if c['name'].startswith('fanin-test-'):
        print(c['id'], c['name'])" \
  | while read -r id name; do
      del "connector $name ($id)" confluent connect cluster delete "$id" --cluster "$CLUSTER" --environment "$ENV_ID" --force
      echo "deleted connector $name ($id)"
    done

# 2. plugin (only possible once no connector uses it)
sleep 10
[ -n "$PLUGIN" ] && del "plugin $PLUGIN" confluent connect custom-plugin delete "$PLUGIN" --force

# 3. topics: fanin-test-* and the app log topics of connectors created by this work. APPLOG_IDS is a space separated
#    list of clcc-... ids (every connector this work created, also the already deleted ones, whose app log topic stays).
#    Other clcc-* app log topics in the cluster belong to somebody else and are never touched.
APPLOG_IDS=${APPLOG_IDS:-}
for f in "$STATE"/connector-*.json; do
  [ -f "$f" ] && APPLOG_IDS="$APPLOG_IDS $(python3 -c "import json; print(json.load(open('$f'))['id'])")"
done
export APPLOG_IDS
for t in $(confluent kafka topic list --cluster "$CLUSTER" --environment "$ENV_ID" -o json 2>/dev/null \
    | python3 -c "
import json, os, sys
ids = set(os.environ['APPLOG_IDS'].split())
for t in json.load(sys.stdin):
    n = t['name']
    if n.startswith('fanin-test-') or (n.startswith('clcc-') and n.endswith('-app-logs')
                                       and n[len('clcc-'):-len('-app-logs')] in {i[len('clcc-'):] for i in ids}):
        print(n)"); do
  del "topic $t" confluent kafka topic delete "$t" --cluster "$CLUSTER" --environment "$ENV_ID" --force
done

# 4. ACLs, API keys, service account
for spec in \
  "--operations read,describe,write --topic fanin-test-src. --prefix" \
  "--operations read,describe,write --topic fanin-test-out" \
  "--operations write,describe --topic fanin-test-nomatch. --prefix" \
  "--operations read --consumer-group connect- --prefix" \
  "--operations read --consumer-group fanin-test- --prefix"; do
  # shellcheck disable=SC2086
  confluent kafka acl delete --allow --service-account "$SA" --cluster "$CLUSTER" --environment "$ENV_ID" $spec --force >/dev/null 2>&1
done
DELETED+=("ACLs of $SA")
# every API key owned by the service account (found by listing: the state file may miss some)
for k in $(confluent api-key list --service-account "$SA" --environment "$ENV_ID" -o json 2>/dev/null \
    | python3 -c "import json,sys; print(' '.join(x['key'] for x in json.load(sys.stdin)))"); do
  del "API key $k" confluent api-key delete "$k" --force
done
del "service account $SA" confluent iam service-account delete "$SA" --force

echo; echo "Deleted:"; printf '  - %s\n' "${DELETED[@]}"
if [ "$FAILED" -eq 0 ]; then rm -rf "$STATE"; else echo "$FAILED deletions failed: kept $STATE so the script can be run again"; fi
