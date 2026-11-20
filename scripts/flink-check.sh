#!/usr/bin/env bash
# Phase 5: checks, with Confluent Cloud Flink, that the fan-in topic is readable and how the source topic can be read.
# Creates a Flink API key, keeps its secret in this process's environment only, runs scripts/flink_check.py and
# deletes the key again when it ends (also on failure). Output: docs/flink-results.txt
# Usage: scripts/flink-check.sh [compute-pool-id]      default pool <pool-id> (<existing compute pool>)
set -euo pipefail
cd "$(dirname "$0")/.."

STATE=cc-state
j() { python3 -c "import json,sys; print(json.load(open('$1'))['$2'])"; }
ENV_ID=$(j $STATE/identity.json environment)
CLUSTER=$(j $STATE/identity.json cluster)

ENV_NAME=$(confluent environment describe "$ENV_ID" -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['name'])")
CLUSTER_JSON=$(confluent kafka cluster describe "$CLUSTER" --environment "$ENV_ID" -o json)
CLUSTER_NAME=$(echo "$CLUSTER_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['name'])")
REGION=$(echo "$CLUSTER_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['region'])")
CLOUD=$(echo "$CLUSTER_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['cloud'])")
ORG=$(confluent organization describe -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")

KEY_JSON=$(confluent api-key create --resource flink --cloud "$CLOUD" --region "$REGION" --environment "$ENV_ID" \
  --description "fanin-test flink key (temporary, deleted by flink-check.sh)" -o json)
FLINK_API_KEY=$(echo "$KEY_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_key'])")
FLINK_API_SECRET=$(echo "$KEY_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_secret'])")
unset KEY_JSON
trap 'confluent api-key delete "$FLINK_API_KEY" --force >/dev/null 2>&1 && echo "Flink API key $FLINK_API_KEY deleted"' EXIT
echo "Flink API key $FLINK_API_KEY created (secret kept in memory only)"

export FLINK_API_KEY FLINK_API_SECRET FLINK_ORG="$ORG" FLINK_ENV_ID="$ENV_ID" FLINK_ENV_NAME="$ENV_NAME" \
  FLINK_CLUSTER_NAME="$CLUSTER_NAME" FLINK_POOL="${1:-<pool-id>}" FLINK_REGION="$REGION" \
  FLINK_CLOUD="$(echo "$CLOUD" | tr '[:upper:]' '[:lower:]')"

python3 -I scripts/flink_check.py 2>&1 | tee docs/flink-results.txt
