#!/usr/bin/env bash
# Topics for one variant of the topic-delete gap test: target PREFIX-fanin-out (1 partition,
# message.timestamp.type=LogAppendTime) and the source topic "allowed" (1 partition). With --control also the
# control topic PREFIX-control with "allowed" on the list.
# Usage: gap-setup.sh [--control]     (set PREFIX per variant, for example PREFIX=demo-r2-a)
# Then: mkconn.py NAME --prefix $PREFIX [--control -] [key=value ...], prod.py and mon.py, gap-run.sh.
set -uo pipefail
case "${1:-}" in -h|--help) sed -n '2,6p' "$0"; exit 0;; esac
source "$(dirname "$0")/env.sh"; need_env
cc kafka topic create "$(TARGET)" --partitions 1 --config message.timestamp.type=LogAppendTime "${CC[@]}"
cc kafka topic create "$(src allowed)" --partitions 1 "${CC[@]}"
if [ "${1:-}" = --control ]; then
  cc kafka topic create "$(CTL_TOPIC)" --partitions 1 --config cleanup.policy=compact "${CC[@]}"
  echo "$(ts) $(K ctl "$(CTL_TOPIC)" "$(src allowed)" add)"
fi
