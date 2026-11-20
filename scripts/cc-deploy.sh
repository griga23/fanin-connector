#!/usr/bin/env bash
# Deploys the fan-in connector to Confluent Cloud (test cluster only, everything named fanin-test-*).
#
# Prerequisites (done once, state in cc-state/, which is git-ignored and holds no secrets):
#   cc-state/identity.json   service account id, API key id, environment, cluster
#   cc-state/plugin.json     output of `confluent connect custom-plugin create ... -o json`
#
# The API secret never touches disk: this script creates a fresh API key for the service account, keeps the
# secret in this process's environment and passes the connector config through process substitution.
#
# Knobs (environment variables):
#   CONNECTOR_NAME   default fanin-test-connector
#   TOPICS_MODE      regex (default) | list      regex => topics.regex, list => topics=fanin-test-src.a,fanin-test-src.b
#   TOPICS_REGEX     default ^fanin-test-src\..+
#   ENDPOINTS        yes (default) | no          whether confluent.custom.connection.endpoints is set
#   TASKS            default 1
#   OUTPUT_MODE      HEADERS (default) | JSON_ENVELOPE
#   UPDATE_ID        clcc-... id: update that connector in place instead of creating a new one
#   EXTRA_JSON       extra connector properties as a JSON object, e.g. '{"consumer.override.auto.offset.reset":"earliest"}'
set -euo pipefail
cd "$(dirname "$0")/.."

STATE=cc-state
[ -f $STATE/identity.json ] && [ -f $STATE/plugin.json ] || { echo "missing $STATE/identity.json or plugin.json" >&2; exit 1; }
j() { python3 -c "import json,sys; print(json.load(open('$1'))['$2'])"; }

ENV_ID=$(j $STATE/identity.json environment)
CLUSTER=$(j $STATE/identity.json cluster)
SA=$(j $STATE/identity.json service_account)
PLUGIN=$(j $STATE/plugin.json id)

export CONNECTOR_NAME=${CONNECTOR_NAME:-fanin-test-connector}
export TOPICS_MODE=${TOPICS_MODE:-regex}
export TOPICS_REGEX=${TOPICS_REGEX:-'^fanin-test-src\..+'}
export ENDPOINTS=${ENDPOINTS:-yes}
export TASKS=${TASKS:-1}
export OUTPUT_MODE=${OUTPUT_MODE:-HEADERS}
export EXTRA_JSON=${EXTRA_JSON:-'{}'}
export PLUGIN

BOOTSTRAP_HOST=$(confluent kafka cluster describe "$CLUSTER" --environment "$ENV_ID" -o json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['endpoint'].split('://')[1])")
export BOOTSTRAP_HOST

# Fresh API key for the service account; secret stays in the environment of this process only.
KEY_JSON=$(confluent api-key create --resource "$CLUSTER" --service-account "$SA" --environment "$ENV_ID" \
  --description "fanin-test key (temporary)" -o json)
FANIN_API_KEY=$(echo "$KEY_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_key'])")
FANIN_API_SECRET=$(echo "$KEY_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['api_secret'])")
export FANIN_API_KEY FANIN_API_SECRET
unset KEY_JSON
confluent api-key store "$FANIN_API_KEY" "$FANIN_API_SECRET" --resource "$CLUSTER" --environment "$ENV_ID" --force >/dev/null
# Keys are never deleted here, because older connectors still use them. Every key is listed in identity.json
# ("api_keys") and cc-cleanup.sh deletes them all.
python3 - "$STATE/identity.json" "$FANIN_API_KEY" <<'EOF'
import json, sys
p, key = sys.argv[1], sys.argv[2]
d = json.load(open(p))
d["api_key"] = key
keys = d.setdefault("api_keys", [])
if key not in keys:
    keys.append(key)
json.dump(d, open(p, "w"))
EOF
echo "using API key $FANIN_API_KEY"

gen_config() {
python3 <<'EOF'
import json, os
e = os.environ
cfg = {
    "name": e["CONNECTOR_NAME"],
    "connector.class": "com.example.fanin.FanInSinkConnector",
    "confluent.connector.type": "CUSTOM",
    "confluent.custom.plugin.id": e["PLUGIN"],
    "confluent.custom.connect.plugin.runtime": "4.2",
    "confluent.custom.connect.java.version": "21",
    "confluent.custom.schema.registry.auto": "false",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": e["FANIN_API_KEY"],
    "kafka.api.secret": e["FANIN_API_SECRET"],
    "tasks.max": e["TASKS"],
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "fanin.target.topic": "fanin-test-out",
    "fanin.target.bootstrap.servers": e["BOOTSTRAP_HOST"],
    "fanin.target.api.key": e["FANIN_API_KEY"],
    "fanin.target.api.secret": e["FANIN_API_SECRET"],
    "fanin.output.mode": e["OUTPUT_MODE"],
}
if e["TOPICS_MODE"] == "regex":
    cfg["topics.regex"] = e["TOPICS_REGEX"]
else:
    cfg["topics"] = "fanin-test-src.a,fanin-test-src.b"
if e["ENDPOINTS"] == "yes":
    cfg["confluent.custom.connection.endpoints"] = e["BOOTSTRAP_HOST"] + ":TCP"
cfg.update(json.loads(e["EXTRA_JSON"]))
print(json.dumps({"name": e["CONNECTOR_NAME"], "config": cfg}))
EOF
}

if [ -n "${UPDATE_ID:-}" ]; then
  # Update an existing connector in place (for example TASKS=4 or EXTRA_JSON=...). The connector restarts.
  confluent connect cluster update "$UPDATE_ID" --config-file <(gen_config) --cluster "$CLUSTER" --environment "$ENV_ID"
else
  confluent connect cluster create --config-file <(gen_config) --cluster "$CLUSTER" --environment "$ENV_ID" -o json \
    | tee "$STATE/connector-$CONNECTOR_NAME.json"
fi
