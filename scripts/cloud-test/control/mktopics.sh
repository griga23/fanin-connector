#!/usr/bin/env bash
# Creates the topics of the control-topic scenario: PREFIX-control (1 partition, compacted), PREFIX-fanin-out
# (6 partitions) and the source topics a, b, c (3 partitions each). Retries while a deleted topic of the same name
# is still being removed.
# Usage: mktopics.sh [-h]
set -uo pipefail
case "${1:-}" in -h|--help) sed -n '2,5p' "$0"; exit 0;; esac
source "$(dirname "$0")/env.sh"; need_env
mk() {
  local out
  for _ in $(seq 20); do
    if out=$(cc kafka topic create "$@" "${CC[@]}" 2>&1); then echo "$out"; return 0; fi
    echo "$out" | head -1; sleep_s 10
  done
  return 1
}
mk "$(CTL_TOPIC)" --partitions 1 --config cleanup.policy=compact,segment.ms=600000,min.compaction.lag.ms=0,min.cleanable.dirty.ratio=0.01 \
  || mk "$(CTL_TOPIC)" --partitions 1 --config cleanup.policy=compact,segment.ms=600000
mk "$(TARGET)" --partitions 6 --config max.message.bytes=6291456
for x in a b c; do mk "$(src $x)" --partitions 3 --config max.message.bytes=6291456; done
