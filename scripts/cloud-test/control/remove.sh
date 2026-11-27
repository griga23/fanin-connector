#!/usr/bin/env bash
# Time-based remove with records waiting in the lag (as LocalIT L19), then a re-add.
# Pauses the connector, writes 10 records to a partition 1, removes a, writes 10 more, resumes. Expected: the 10
# written before the remove arrive, the 10 after do not, and the ledger of a-1 is fanin.ctl:v2:removed:<time>:<cut>.
# Then adds a again and writes 10 records: they arrive; the 10 written while a was removed stay out.
# Usage: remove.sh CONNECTOR_ID     (run after drive.sh; a must be allowed)
set -uo pipefail
case "${1:-}" in -h|--help|"") sed -n '2,6p' "$0"; exit 0;; esac
source "$(dirname "$0")/env.sh"; need_env
ID=$1; A=$(src a)
L() { LG "$ID" | grep -- '-1 '; }
W() { echo "$(ts) $(K produce "$A" 10 --partition 1)"; }
R() {
  echo "$(ts) counts"; K ends "$A" "$(src b)" "$(src c)"; K count "$(TARGET)"; K offsets "$(TARGET)" "$A" | grep '^p1:'
  echo "$(ts) ledger"; L
}
echo "== remove with records waiting"
L
echo "$(ts) pause"; cc connect cluster pause "$ID" "${CC[@]}"
wait_status "$ID" 'PAUSED*' 30 5
END0=$(K ends "$A" | awk '{print $2}')
W; sleep_s 2
echo "$(ts) $(K ctl "$(CTL_TOPIC)" "$A" remove)"; sleep_s 2
W
K ends "$A"
echo "$(ts) resume"; cc connect cluster resume "$ID" "${CC[@]}"
wait_status "$ID" "RUNNING \['RUNNING'\]" 60 10
sleep_s 90; R
echo "(source end of a before the test: ${END0:-?}; timestamps of the two batches: kc.py timestamps $A 1 LO HI)"
echo "== re-add a, write 10"
echo "$(ts) $(K ctl "$(CTL_TOPIC)" "$A" add)"; sleep_s 45
W; sleep_s 75; R
echo DONE
