#!/usr/bin/env bash
# One step of the control-topic scenario (add, remove, re-add, missing topic) against a running connector.
# Usage: scenario.sh STEP     (STEP 1 to 5)
#   1  allow a only (writes the add of a first), then RUN_RECORDS records on each of a, b, c
#   2  add b, then RUN_RECORDS records on b
#   3  remove a, then RUN_RECORDS records on a
#   4  add a again, then RUN_RECORDS records on a
#   5  add a name without a topic (typo); one WARN after fanin.control.missing.warn.ms
# Each step prints the source end offsets and the target counts per source topic. Records go to partition 1,
# timestamp = now. RUN_RECORDS defaults to 60. WAIT_S (default 60) is the wait before the counts.
# Expected with fanin.control.add.start=LATEST (target a / b / c): 60/0/0, 60/60/0, 60/60/0, 120/60/0.
# With the default EARLIEST, step 2 forwards the 60 older records of b too: 60/120/0 after step 2.
set -uo pipefail
case "${1:-}" in -h|--help|"") sed -n '2,13p' "$0"; exit 0;; esac
source "$(dirname "$0")/env.sh"; need_env
N=${RUN_RECORDS:-60}; W=${WAIT_S:-60}
put() { echo "$(ts) $(K produce "$(src "$1")" "$N" --partition 1)"; }
count() {
  echo "$(ts) --- $1"
  echo "source end offsets:"; K ends "$(src a)" "$(src b)" "$(src c)"
  echo "target per fanin.source.topic:"; K count "$(TARGET)"
}
ctl() { echo "$(ts) $(K ctl "$(CTL_TOPIC)" "$(src "$1")" "$2")"; }
case $1 in
  1) ctl a add; sleep_s 30; put a; put b; put c; sleep_s "$W"; count "step 1: allow a only, records on a b c" ;;
  2) ctl b add; sleep_s 30; put b; sleep_s "$W"; count "step 2: add b, records on b" ;;
  3) ctl a remove; sleep_s 30; put a; sleep_s "$W"; count "step 3: remove a, records on a" ;;
  4) ctl a add; sleep_s 30; put a; sleep_s "$W"; count "step 4: add a again, records on a" ;;
  5) ctl typo add; echo "typo added" ;;
  *) echo "unknown step $1" >&2; exit 2 ;;
esac
