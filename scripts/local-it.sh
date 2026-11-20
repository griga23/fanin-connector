#!/usr/bin/env bash
# Local integration tests L1-L9: builds the plugin, starts Kafka (KRaft) + a Connect worker in Docker,
# runs the LocalIT test class, and keeps the output in docs/local-it-results.txt.
# Usage: scripts/local-it.sh                       all of L1-L9
#        TESTS='LocalIT#l1*+l7*' scripts/local-it.sh   a subset
#        KEEP_STACK=1 scripts/local-it.sh   leaves the containers running afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."

mvn -B -q clean verify -DskipTests
docker compose -f docker/docker-compose.yml down -v >/dev/null 2>&1 || true
docker compose -f docker/docker-compose.yml up -d

for _ in $(seq 1 60); do
  curl -sf localhost:8083/connector-plugins >/dev/null 2>&1 && break
  sleep 3
done

set +e
mvn -B test -Dtest="${TESTS:-LocalIT}" -Dit.excludedGroups=none -Dgroups=integration -Dsurefire.failIfNoSpecifiedTests=false \
  2>&1 | tee target/local-it.log
STATUS=${PIPESTATUS[0]}
set -e

grep -E "^RESULT |Tests run:|BUILD" target/local-it.log | sed 's/^\[[A-Z]*\] //' > docs/local-it-results.txt || true
[ "${KEEP_STACK:-0}" = "1" ] || docker compose -f docker/docker-compose.yml down -v
exit "$STATUS"
