#!/usr/bin/env bash
# Topic-delete gap test for one variant (prefix demo-r2-*): creates topic y1 that matches the regex, writes 5 records,
# waits until the task is assigned y1, waits 30 s, deletes y1, waits until the allowed partition is assigned again,
# waits 75 s. Run prod.py into the allowed topic and mon.py on the target at the same time, and measure the gap
# afterwards with ana.py. Saves the connector log of the run to OUT_DIR/logs-<connector id>.json.
# Usage: gap-run.sh CONNECTOR_ID [--allow-y1] [--empty-topic]
#   --allow-y1     put y1 on the control topic before the create (variant "deleted topic allowed")
#   --empty-topic  a second cycle with topic y2 that gets no records
# OUT_DIR defaults to the current directory. EVENTS (default OUT_DIR/events.log) gets a timeline line per step.
set -uo pipefail
case "${1:-}" in -h|--help|"") sed -n '2,10p' "$0"; exit 0;; esac
source "$(dirname "$0")/env.sh"; need_env
ID=$1; shift; ALLOW=0; EMPTY=0
for a in "$@"; do case $a in --allow-y1) ALLOW=1;; --empty-topic) EMPTY=1;; *) echo "unknown $a" >&2; exit 2;; esac; done
OUT=${OUT_DIR:-.}; EV=${EVENTS:-$OUT/events.log}; W="$OUT/w-$ID.json"; START=$(iso)
ev() { echo "$(ts) $PREFIX $*" | tee -a "$EV"; }
waitlog() { # SINCE SEARCH NAME LABEL
  for _ in $(seq 30); do
    sleep_s 15
    py "$HERE/logs.py" "$ID" "$1" "$(iso)" INFO "$W" "$2" >/dev/null 2>&1
    if [ -s "$W" ] && python3 -c "import sys; sys.exit(0 if sys.argv[1] in open(sys.argv[2]).read() else 1)" "$3" "$W"; then
      ev "$4 seen in log"; return 0
    fi
    [ "${DRY_RUN:-0}" = 1 ] && { ev "$4 (dry run)"; return 0; }
  done
  ev "$4 NOT seen in log (timeout)"; return 1
}
cycle() { # NAME NRECORDS LABEL
  local t c0 d0; t=$(src "$1")
  [ $ALLOW = 1 ] && ev "$3 $(K ctl "$(CTL_TOPIC)" "$t" add)"
  c0=$(iso); cc kafka topic create "$t" --partitions 1 "${CC[@]}" >/dev/null; ev "$3 created $t"
  if [ "$2" -gt 0 ]; then K produce "$t" "$2" >/dev/null; ev "$3 wrote $2 records"; fi
  waitlog "$c0" "Adding newly assigned" "$PREFIX-$1" "$3 assignment of $1"
  sleep_s 30
  d0=$(iso); cc kafka topic delete "$t" --force "${CC[@]}" >/dev/null; ev "$3 deleted $t"
  waitlog "$d0" "Adding newly assigned" "$PREFIX-allowed" "$3 re-assignment after the delete"
  sleep_s 75; ev "$3 done"
}
cycle y1 5 step1
[ $EMPTY = 1 ] && cycle y2 0 step2
py "$HERE/logs.py" "$ID" "$START" "$(iso)" "INFO|WARN|ERROR" "$OUT/logs-$ID.json"
ev "logs saved to $OUT/logs-$ID.json"
