#!/usr/bin/env bash
# Local integration tests L1-L20: builds the plugin, starts Kafka (KRaft) + a Connect worker in Docker,
# runs the LocalIT test class, and keeps the output in docs/local-it-results.txt.
# Usage: scripts/local-it.sh                       all of L1-L20
#        TESTS='LocalIT#l1*+l7*' scripts/local-it.sh   a subset
#        KEEP_STACK=1 scripts/local-it.sh   leaves the containers running afterwards.
#        IT_PROJECT=fanin-it2 IT_KAFKA_PORT=39092 IT_CONNECT_PORT=18093 scripts/local-it.sh
#                                            runs a second stack with other names and ports.
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE=(docker compose)
docker compose version >/dev/null 2>&1 || COMPOSE=(docker-compose)
export IT_PROJECT="${IT_PROJECT:-fanin-it}" IT_KAFKA_PORT="${IT_KAFKA_PORT:-29092}" IT_CONNECT_PORT="${IT_CONNECT_PORT:-8083}"

mvn -B -q clean verify -DskipTests
"${COMPOSE[@]}" -f docker/docker-compose.yml down -v >/dev/null 2>&1 || true
"${COMPOSE[@]}" -f docker/docker-compose.yml up -d

for _ in $(seq 1 60); do
  curl -sf "localhost:$IT_CONNECT_PORT/connector-plugins" >/dev/null 2>&1 && break
  sleep 3
done

set +e
mvn -B test -Dtest="${TESTS:-LocalIT}" -Dit.bootstrap="localhost:$IT_KAFKA_PORT" \
  -Dit.connect="http://localhost:$IT_CONNECT_PORT" -Dit.excludedGroups=none -Dgroups=integration -Dsurefire.failIfNoSpecifiedTests=false \
  2>&1 | tee target/local-it.log
STATUS=${PIPESTATUS[0]}
set -e

mkdir -p docs
grep -E "^RESULT |Tests run:|BUILD" target/local-it.log | sed 's/^\[[A-Z]*\] //' > docs/local-it-results.txt || true
[ "${KEEP_STACK:-0}" = "1" ] || "${COMPOSE[@]}" -f docker/docker-compose.yml down -v
exit "$STATUS"
