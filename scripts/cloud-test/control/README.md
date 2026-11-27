# Control topic tests on Confluent Cloud

Scripts for the manual Confluent Cloud tests of `fanin.control.topic`. The results are in `docs/CONTROL-TOPIC.md`,
"Test evidence". The scripts create topics and connectors in the cluster you name, and nothing else.

## Requirements

- `confluent` CLI, logged in, with access to the environment and the cluster.
- Python 3 with `confluent-kafka` (`pip install confluent-kafka`).
- A custom connector plugin built from this repository (`scripts/build.sh`) and uploaded with
  `confluent connect custom-plugin create`. Its ID (`ccp-...`) is `PLUGIN_ID`.

## Settings

| Variable | Required | Meaning |
|---|---|---|
| `FANIN_ENV_FILE` | yes | Path to a credentials file with `KEY=VALUE` lines: `BOOTSTRAP_SERVERS`, `KAFKA_CLUSTER_API_KEY`, `KAFKA_CLUSTER_API_SECRET`. `sr.py` also needs `SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_API_KEY`, `SCHEMA_REGISTRY_API_SECRET`. Keep it outside the repository, mode 600. |
| `CC_ENV` | yes | Environment ID (`env-...`). |
| `CC_CLUSTER` | yes | Kafka cluster ID (`lkc-...`). |
| `PLUGIN_ID` | for `mkconn.py` | Custom plugin ID, or pass `--plugin`. |
| `PREFIX` | no | Test prefix, default `demo-c`. Topics: `PREFIX-control`, `PREFIX-fanin-out`, and the sources `SRC_PREFIX` + `PREFIX-<x>` + `SRC_SUFFIX`. |
| `SRC_PREFIX`, `SRC_SUFFIX` | no | Default `src.` and empty. The runs in the docs used a longer prefix and a suffix; the docs show the default names. |
| `PY` | no | Python with `confluent-kafka`, default `python3`. The scripts run it with `-I`. |
| `DRY_RUN=1` | no | The shell scripts print the CLI and Kafka calls instead of running them. |

The API key needs WRITE on the source and control topics (the test writes records), READ on the target and on the
control topic, and DESCRIBE on the consumer group `connect-<connector id>` (for `ledger.py`). The scripts never print
a secret. `mkconn.py` writes the connector config only to a mode-600 temporary file and removes it after the call.

Every script prints its usage with `-h` or `--help`.

## Scenario: add, remove, re-add

```
export FANIN_ENV_FILE=... CC_ENV=env-... CC_CLUSTER=lkc-... PLUGIN_ID=ccp-... PREFIX=demo-c
scripts/cloud-test/control/mktopics.sh
scripts/cloud-test/control/mkconn.py "$PREFIX-fanin" fanin.control.add.start=LATEST
# wait until the connector is RUNNING, note its ID (clcc-...)
scripts/cloud-test/control/drive.sh clcc-...            # steps 1 to 4, ledger after each
scripts/cloud-test/control/remove.sh clcc-...           # time-based remove with records in the lag, then re-add
scripts/cloud-test/control/restart.sh clcc-...          # restart with old-timestamp records waiting
scripts/cloud-test/control/scenario.sh 5                # name without a topic: one WARN after 2 minutes
```

| Script | What it checks | Expected |
|---|---|---|
| `mktopics.sh` | Creates `PREFIX-control` (1 partition, compacted), `PREFIX-fanin-out` (6 partitions), sources a, b, c (3 partitions). | |
| `mkconn.py NAME [key=value ...]` | Creates the connector (`tasks.max=1`, `fanin.timestamp.mode=COPY`, `fanin.control.missing.warn.ms=120000`). `--control -` creates one without the control topic, `--dry-run` prints the config with the secrets masked. | |
| `scenario.sh STEP` | One step: 1 allow a only, records on a, b, c; 2 add b, records on b; 3 remove a, records on a; 4 add a again, records on a; 5 add a name without a topic. 60 records per run on partition 1 (`RUN_RECORDS`). | Target a / b / c with `LATEST`: 60/0/0, 60/60/0, 60/60/0, 120/60/0. With the default `EARLIEST`, step 2 also forwards the 60 older records of b (60/120/0). |
| `drive.sh ID [--repeated-add]` | Steps 1 to 4 with the committed ledger of partition 1 after each step, the source offsets of a found in the target, and the control topic. `--repeated-add` writes the add of b again after step 2. | Ledger `fanin.ctl:v1:allowed:<t>` while allowed, `fanin.ctl:v2:removed:<t>:<cut>` after a remove; offsets of a written while it was removed are not in the target. |
| `remove.sh ID` | Pause, 10 records on a-1, remove a, 10 records, resume; then re-add a and 10 records. | The first 10 arrive, the second 10 do not; ledger `removed:<t>:<cut>`; after the re-add only the new 10 arrive. |
| `restart.sh ID` | Pause, 10 records on b-1 with a timestamp one hour back, restart the task by a config update, resume. | All 10 records arrive; the ledger keeps the position. |

Helpers: `kc.py` (counts per source topic, end offsets, control records, dump, source offsets in the target,
produce, timestamps of an offset range), `ledger.py GROUP...` (committed offsets with metadata), `sr.py`
(list and delete Schema Registry subjects of test topics).

## Topic delete: forwarding gap (prefix demo-r2)

Measures how long the task stops forwarding when a topic that matches the regex is deleted. One connector per
variant, each with its own `PREFIX`, run at the same time:

```
export PREFIX=demo-r2-a                                   # one prefix per variant
scripts/cloud-test/control/gap-setup.sh [--control]       # target (LogAppendTime) + "allowed" source topic
scripts/cloud-test/control/mkconn.py "$PREFIX" --control -        # a: feature off
#   b, c: --control "$PREFIX-control" ; e: also consumer.override.default.api.timeout.ms=10000
scripts/cloud-test/control/prod.py "${SRC_PREFIX-src.}$PREFIX-allowed$SRC_SUFFIX" 200 &   # 200 records/s
scripts/cloud-test/control/mon.py "$PREFIX-fanin-out" mon-$PREFIX.csv &
scripts/cloud-test/control/gap-run.sh clcc-... [--allow-y1] [--empty-topic]
scripts/cloud-test/control/ana.py mon-$PREFIX.csv HH:MM:SS HH:MM:SS <allowed source topic>
```

| Script | What it does |
|---|---|
| `gap-setup.sh [--control]` | Target `PREFIX-fanin-out` (1 partition, `message.timestamp.type=LogAppendTime`), source `allowed` (1 partition); with `--control` a control topic that lists `allowed`. |
| `gap-run.sh ID [--allow-y1] [--empty-topic]` | Creates `y1`, writes 5 records, waits until the task is assigned `y1` (connector log), waits 30 s, deletes `y1`, waits for the next assignment, waits 75 s. `--allow-y1` lists `y1` on the control topic first; `--empty-topic` repeats with `y2` without records. Saves the connector log to `OUT_DIR/logs-<id>.json` and a timeline to `events.log`. |
| `prod.py TOPIC RATE` | Steady producer, value `{seq, ts}`. |
| `mon.py TARGET OUT_CSV` | Reads the target from its end, one CSV line per record (sequence, produce time, append time, source topic). |
| `ana.py CSV START END TOPIC` | Records, missing and duplicate sequence numbers, latency, and the longest gap between two forwarded records. |
| `logs.py ID START END LEVELS OUT [SEARCH]` | Saves all pages of the connector log of a time window. |

Variants of the run on 2026-11-27: a feature off; b control topic, `y1` allowed (`--allow-y1`); c control topic,
`y1` not listed (paused); d as a with `--empty-topic`; e as c with
`consumer.override.default.api.timeout.ms=10000`. Expected: a gap of about 60 s (`default.api.timeout.ms`) in a to d
and about 10 s in e; no record missing or duplicated.

## Cleanup

Delete the connectors (`confluent connect cluster delete`) and the topics with the prefix
(`confluent kafka topic delete`). The consumer groups `connect-<connector id>` stay until offset retention removes
them.
