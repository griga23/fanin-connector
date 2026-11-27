#!/usr/bin/env bash
# Runs scenario steps 1 to 4 and prints the committed ledger of partition 1 after each step, then the source
# offsets of a found in the target and the content of the control topic.
# Usage: drive.sh CONNECTOR_ID [--repeated-add]
#   --repeated-add  writes the add of b a second time after step 2 (as a script that rewrites the whole list would)
# Start with empty topics (mktopics.sh) and a connector created by mkconn.py, without a record on the control topic.
set -uo pipefail
case "${1:-}" in -h|--help|"") sed -n '2,6p' "$0"; exit 0;; esac
source "$(dirname "$0")/env.sh"; need_env
ID=$1; REPEAT=${2:-}; S="$HERE/scenario.sh"
L() { echo "$(ts) ledger"; LG "$ID" | grep -- '-1 '; }
bash "$S" 1; L
bash "$S" 2; L
if [ "$REPEAT" = --repeated-add ]; then echo "$(ts) repeated $(K ctl "$(CTL_TOPIC)" "$(src b)" add)"; fi
bash "$S" 3; L
bash "$S" 4; L
K offsets "$(TARGET)" "$(src a)"
K dump "$(CTL_TOPIC)"
echo DONE
