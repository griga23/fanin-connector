#!/usr/bin/env bash
# Task restart with old-timestamp records waiting in the lag (as LocalIT L18). Pauses the connector, writes 10
# records to b partition 1 with a timestamp one hour back, restarts the task by a config update, resumes, and prints
# the counts and the ledger. Expected: all 10 records of b arrive (none skipped by the restart).
# Usage: restart.sh CONNECTOR_ID     (run after drive.sh; b must be allowed)
set -uo pipefail
case "${1:-}" in -h|--help|"") sed -n '2,5p' "$0"; exit 0;; esac
source "$(dirname "$0")/env.sh"; need_env
ID=$1; B=$(src b)
L() { LG "$ID" | grep -- '-1 '; }
OLD=$(( $(date +%s) * 1000 - 3600000 ))
echo "$(ts) pause"; cc connect cluster pause "$ID" "${CC[@]}"
wait_status "$ID" 'PAUSED*' 30 5
echo "$(ts) $(K produce "$B" 10 --partition 1 --timestamp "$OLD")"
echo "$(ts) ledger while paused"; L; K ends "$B"
echo "$(ts) config update (restarts the task)"
cc connect cluster update "$ID" --config fanin.control.missing.warn.ms=120001 "${CC[@]}"
sleep_s 20
echo "$(ts) resume"; cc connect cluster resume "$ID" "${CC[@]}"
wait_status "$ID" "RUNNING \['RUNNING'\]" 60 10
sleep_s 90
echo "$(ts) counts"; K ends "$(src a)" "$B" "$(src c)"; K count "$(TARGET)"
echo "$(ts) ledger"; L
echo DONE
