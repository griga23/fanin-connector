# Sourced by the shell scripts in this folder. Checks the settings and defines small helpers.
# Settings (environment variables):
#   FANIN_ENV_FILE  credentials file (KEY=VALUE): BOOTSTRAP_SERVERS, KAFKA_CLUSTER_API_KEY, KAFKA_CLUSTER_API_SECRET
#   CC_ENV          Confluent Cloud environment ID
#   CC_CLUSTER      Kafka cluster ID
#   PREFIX          test prefix for topic names (default demo-c)
#   SRC_PREFIX, SRC_SUFFIX  source topic name = SRC_PREFIX + PREFIX-<x> + SRC_SUFFIX (defaults "src." and "")
#   PY              Python with confluent-kafka (default python3); run with -I
# DRY_RUN=1 prints the confluent CLI and Kafka calls instead of running them.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PREFIX=${PREFIX:-demo-c}
SRC_PREFIX=${SRC_PREFIX-src.}
SRC_SUFFIX=${SRC_SUFFIX-}
export PREFIX SRC_PREFIX SRC_SUFFIX
PYBIN=${PY:-python3}

need_env() {
  local missing=()
  for v in FANIN_ENV_FILE CC_ENV CC_CLUSTER; do [ -n "${!v:-}" ] || missing+=("$v"); done
  if [ ${#missing[@]} -gt 0 ] && [ "${DRY_RUN:-0}" != 1 ]; then
    echo "missing settings: ${missing[*]} (see README.md)" >&2; exit 2
  fi
  if [ -n "${FANIN_ENV_FILE:-}" ] && [ ! -r "$FANIN_ENV_FILE" ] && [ "${DRY_RUN:-0}" != 1 ]; then
    echo "FANIN_ENV_FILE is not readable: $FANIN_ENV_FILE" >&2; exit 2
  fi
  export FANIN_ENV_FILE CC_ENV CC_CLUSTER
  CC=(--environment "${CC_ENV:-ENV}" --cluster "${CC_CLUSTER:-CLUSTER}")
}

ts() { date -u +%H:%M:%SZ; }
iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }
src() { echo "${SRC_PREFIX}${PREFIX}-$1${SRC_SUFFIX}"; }
CTL_TOPIC() { echo "${PREFIX}-control"; }
TARGET() { echo "${PREFIX}-fanin-out"; }

# run CMD...: runs a command, or prints it with DRY_RUN=1
run() { if [ "${DRY_RUN:-0}" = 1 ]; then echo "DRY_RUN: $*"; else "$@"; fi; }
cc() { run confluent "$@"; }
py() { run "$PYBIN" -I "$@"; }
K() { py "$HERE/kc.py" "$@" 2>/dev/null; }
LG() { py "$HERE/ledger.py" "connect-$1" 2>/dev/null; }
sleep_s() { [ "${DRY_RUN:-0}" = 1 ] || sleep "$1"; }

# status ID: "<connector status> [<task states>]"
status() {
  confluent connect cluster describe "$1" "${CC[@]}" -o json | python3 -c \
    "import json,sys; d=json.load(sys.stdin); print(d['connector']['status'], [t['state'] for t in d.get('tasks',[])])"
}
# wait_status ID PATTERN TRIES SLEEP
wait_status() {
  local s
  if [ "${DRY_RUN:-0}" = 1 ]; then echo "DRY_RUN: wait for $1 status $2"; return 0; fi
  for _ in $(seq "$3"); do
    s=$(status "$1")
    # shellcheck disable=SC2254
    case "$s" in $2) echo "$(ts) $s"; return 0;; esac
    sleep_s "$4"
  done
  echo "$(ts) timeout waiting for $2 (last: $s)"; return 1
}
