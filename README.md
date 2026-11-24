# Fan-in sink connector

A Kafka Connect **sink** connector that subscribes to many topics (selected by a regular expression) and writes every
record to **one** target topic on the same cluster. A reader that needs a single static topic, such as Confluent Cloud
for Apache Flink, can then read the fan-in topic instead of a changing list of topics.

Measured results are in `docs/FINDINGS.md` (kept local, not published). Read its "Open questions" table first.

## Architecture

```
 topics matching topics.regex ──► Connect sink tasks (consumer group, rebalancing, offset commits by the framework)
                                        │  KafkaProducer (acks=all, idempotent), per task
                                        ▼
                                  one target topic  ──►  Flink / any consumer
```

- The subscription is **not** part of the connector. The Connect framework subscribes with `topics.regex`
  (or `topics`) and gives the connector discovery of new matching topics, consumer group management, rebalancing and
  offset commits. A connector cannot change its own subscription, so the "prefix" is expressed as a regex.
- Each task re-produces every record to the target topic with its own producer.
- Records pass through unchanged as bytes (use the `ByteArrayConverter`, see below), so any schema works and Schema
  Registry is not involved.

## Prefix and regex guidance

- Set exactly one of `topics` or `topics.regex`. The connector rejects both or neither.
- A prefix is a regex such as `^acme\.app\..+`. Escape the dots. Kafka uses a **full match** of the whole topic name,
  so `^acme\.app\..+` matches `acme.app.engine` but not `x.acme.app.engine`.
- Keep patterns simple and RE2J compatible: no lookaheads, no backreferences. Managed connectors use RE2J, and a custom
  connector may be moved to it later.
- **The target topic must never match the regex** (or be in `topics`), otherwise the connector consumes its own output
  and loops. The connector refuses such a configuration when it is created (REST validation) and again when a task
  starts. Use a target name outside the prefix, for example `acme-fanin-out` for the prefix above.
- Topics that match are picked up when the consumer refreshes its metadata. That is `metadata.max.age.ms`, 5 minutes by
  default. On Confluent Cloud the setting cannot be changed (see "Limitations"): expect about 4 minutes.

## Configuration reference

Connector properties (all are in the connector's ConfigDef, so Confluent Cloud shows them in its UI):

| Property | Type | Default | Description |
|---|---|---|---|
| `fanin.target.topic` | string | required | The single output topic. Must not match the subscription. |
| `fanin.target.bootstrap.servers` | string | required | Bootstrap endpoint of the cluster with the target topic, `host:port`. |
| `fanin.target.api.key` | string | required | Kafka API key of the producer (SASL_SSL / PLAIN). Use a service account that can only write to the target topic. |
| `fanin.target.api.secret` | password | required | The API secret. Marked sensitive. |
| `fanin.output.mode` | enum | `HEADERS` | `HEADERS` or `JSON_ENVELOPE`, see below. |
| `fanin.partitioning.mode` | enum | `SOURCE_PARTITION_HASH` | `SOURCE_PARTITION_HASH` or `KEY_HASH`, see below. |
| `fanin.start.timestamp` | long | `-1` (off) | Epoch milliseconds. Partitions without a committed offset start at the first record at or after this time instead of at the beginning, see "Where reading starts". |
| `fanin.start.consumer.group` | string | `connect-<connector name>` | Consumer group used to find the partitions without a committed offset. Set it if the group has another name. |
| `fanin.producer.*` | passthrough | none | Extra producer settings with the prefix removed, for example `fanin.producer.compression.type=lz4` or `fanin.producer.max.request.size=4194304`. They override the defaults below. |

Producer defaults: `acks=all`, `enable.idempotence=true`, `security.protocol=SASL_SSL`, `sasl.mechanism=PLAIN`
(JAAS with the API key and secret), byte array serializers. For a local PLAINTEXT cluster set
`fanin.producer.security.protocol=PLAINTEXT`.

Framework properties you must set:

| Property | Value |
|---|---|
| `topics.regex` (or `topics`) | the subscription |
| `key.converter`, `value.converter` | `org.apache.kafka.connect.converters.ByteArrayConverter` |
| `header.converter` | `org.apache.kafka.connect.converters.ByteArrayConverter` (otherwise header values are turned into strings) |

### Output modes

- `HEADERS` (default): key and value are passed through unchanged. The headers `fanin.source.topic`,
  `fanin.source.partition`, `fanin.source.offset` and `fanin.source.timestamp` (text, UTF-8) are added. Original
  headers are kept.
- `JSON_ENVELOPE`: the value becomes
  `{"source_topic":..., "source_partition":..., "source_offset":..., "source_timestamp":..., "key":<base64|null>, "value":<base64|null>}`.
  The key is kept as the record key and the headers are added as above. Use it if a reader cannot see Kafka headers.
  Base64 makes values a third bigger: raise `fanin.producer.max.request.size` and the target topic's
  `max.message.bytes` for large records.

In both modes the output record keeps the Kafka timestamp of the source record, so a reader that uses the record
timestamp as event time (Flink's `$rowtime`) sees the original time, not the time of the copy. A source record without
a timestamp gets the time of the copy. If the target topic uses `message.timestamp.type=LogAppendTime`, the broker
replaces the timestamp; the header `fanin.source.timestamp` still has the original value.

### Partitioning

- `SOURCE_PARTITION_HASH` (default): target partition = `murmur2(sourceTopic + "-" + sourcePartition)` modulo the number
  of partitions of the target topic. Records of one source partition always land on one target partition, in order,
  even with null keys. The partition count is cached and refreshed every 5 minutes. Adding partitions to the target
  topic changes the mapping from then on.
- `KEY_HASH`: the default producer partitioner decides from the record key. Order is per key, and null keys are spread.

## Delivery semantics

**At-least-once.** A task commits source offsets only after its producer has acknowledged every record sent before
(`flush()` in `preCommit`). A failed send makes the task fail, so offsets never move past an undelivered record.
After a crash, a restart, a rebalance or a redeploy, records since the last commit are delivered again, so **duplicates
are possible**. There is no exactly-once mode; consumers must tolerate duplicates (the headers
`fanin.source.topic/partition/offset` identify a record uniquely).

Measured: after killing the worker mid-stream there was no loss and about 3,000 to 4,000 duplicates per 20,000 records
(local test L5). Pause and resume gave none (C4).

Ordering: preserved within one source partition. Not preserved across source partitions.

## Where reading starts

By default the framework starts a partition at the beginning of the topic when its consumer group has no committed
offset for it. That happens on the first deployment, for every newly discovered topic, and after the connector was
deleted and created again. To avoid reading old backlog, set `fanin.start.timestamp` to an epoch time in milliseconds.

- A partition **with** a committed offset always resumes from it. A restart, a pause and resume or an update in place
  never rewind. This is covered by test L11 (80 records, 0 duplicates, with the timestamp far before every record).
- A partition **without** a committed offset starts at the first record whose timestamp is at or after the setting.
  A topic that is created after that time is therefore read from its first record, even though it is only found a few
  minutes later and even with `consumer.override.auto.offset.reset=latest` (test L12).
- A partition that has no record at or after the setting starts at its end.
- **Recreating the connector** starts a new group, so every partition is positioned by the timestamp again. Everything
  since the timestamp is delivered a second time (test L13: 20 records twice, none of the 30 older ones). Keep the
  old timestamp to lose nothing and accept the duplicates, or set a later one to cut the duplicates and accept a gap.
- The offsets are looked up with an Admin client built from the `fanin.target.*` settings. The source topics must
  therefore be on the same cluster as the target topic. The lookup needs read access to the consumer group.
- If the group cannot be found or the lookup fails, the setting is ignored, a warning is logged and the default start
  applies. The task does not fail (test L14, and a test for a client library that lacks a method). On Confluent Cloud
  the group is `connect-<connector id>` and the task's `name` is that same ID, so the default group name is right
  there without setting `fanin.start.consumer.group` (checked on Cloud, see below).
- **Checked on Confluent Cloud** (Azure germanywestcentral, BASIC cluster, runtime 4.2, 2026-11-24): with the setting
  the task moved 3 of 3 partitions without a committed offset (to offsets 30, 31 and 39, the 100 older records
  skipped), the target received exactly the 50 newer records, an in-place update then resumed without replay (20
  more records, no duplicates), and 20 records of a topic created after the start arrived with
  `consumer.override.auto.offset.reset=latest`. The Admin client works with the connector's API key.
- The Cloud runtime's Kafka client has no `ConsumerGroupDescription.groupState()`. The lookup uses only methods that
  older clients have, and the task catches `LinkageError`, so a missing method cannot kill it.
- Alternative without connector code: Kafka 4.x consumers accept `auto.offset.reset=by_duration:<ISO 8601 duration>`,
  for example `by_duration:PT1H`, which starts uncommitted partitions that long before the moment of the reset. It is a
  moving window and not a fixed time. Whether Confluent Cloud accepts that value for
  `consumer.override.auto.offset.reset` has not been tested.

## Limitations

Measured on Confluent Cloud (Azure <azure-region>, STANDARD cluster, runtime 4.2); details and numbers in
`docs/FINDINGS.md` (kept local, not published).

- **Discovery delay.** New matching topics are picked up after about 4 minutes (225 to 246 s measured). Cloud rejects
  `consumer.override.metadata.max.age.ms`, so it cannot be shortened with a client override.
- **Task balance.** With the default assignor, topics with 3 partitions are spread over at most 3 consumers: with
  `tasks.max=4` one task stayed idle in the local test. Cloud rejects `consumer.override.partition.assignment.strategy`,
  so the round-robin assignor cannot be used there. Choose `tasks.max` with this in mind.
- **Redeploying duplicates everything.** The consumer group is named after the connector ID (`connect-<clcc-id>`).
  Deleting and recreating the connector starts a new group from the earliest offset (or from `fanin.start.timestamp`
  if set) and delivers those records again. Update the connector in place instead. Connectors cannot be restarted from the Console or the Restart API.
- **Record size.** Default producer limit is 1 MiB (`fanin.producer.max.request.size`). A larger record fails the task
  with `RecordTooLargeException` until the limit is raised. The target topic's `max.message.bytes` (about 2 MiB by
  default on Cloud) limits too. A 2,000,000-byte record worked after raising the producer limit to 4 MiB.
- **A poison record fails the tasks.** A record that cannot be produced kills the task, the partition moves to the next
  task, which fails too. The connector then needs a configuration update (or a fix of the cause) to recover.
- **Cloud allowed overrides** (tested): `consumer.override.auto.offset.reset`, `consumer.override.max.poll.records`,
  `producer.override.linger.ms`. Not allowed: `consumer.override.metadata.max.age.ms`,
  `consumer.override.partition.assignment.strategy`.
- Java connectors only, archive up to 250 MB, 2 GB memory per connector shared by all tasks, up to 250 tasks, 30 custom
  connectors per organization, custom connectors only in selected regions.
- A runtime log shows `NoSuchMethodError: Connector.defaultValidate(Map)` in the worker's config-validate call when the
  connector is updated in place. The update is still applied. Cause not found (see FINDINGS).
- Schema-aware handling is not implemented: records are opaque bytes.

## Build

Requires Java 17 or newer and Maven. The connector is built against Kafka 4.2.0 (Confluent Cloud custom connector
runtime 4.2, Java 17 or 21).

```
scripts/build.sh          # unit tests, coverage report (target/site/jacoco), plugin archive
```

Result: `target/components/packages/example-fanin-connector-0.1.0.zip` (about 21 KB) with `manifest.json`, `lib/`,
`etc/`, `doc/`, `assets/` under one top-level folder. Only the connector jar is in `lib/`; `kafka-clients` and
`connect-api` come from the Connect runtime. The package name is `com.example.fanin`; rename it before real use.

Local integration tests (Docker, Kafka 4.2.0 KRaft plus a Connect worker):

```
scripts/local-it.sh                       # all of L1 to L9, results in docs/local-it-results.txt
TESTS='LocalIT#l1*+l7*' scripts/local-it.sh
```

## Deploy on Confluent Cloud

1. Create a service account and a Kafka API key for the cluster. Needed ACLs: read and describe on the source topic
   prefix, read on the consumer group prefix `connect-`, write and describe on the target topic.
2. Upload the plugin (choose the cloud of your cluster; the default is aws):
   ```
   confluent connect custom-plugin create <name> --plugin-file target/components/packages/example-fanin-connector-0.1.0.zip \
     --connector-class com.example.fanin.FanInSinkConnector --connector-type sink --cloud azure \
     --sensitive-properties fanin.target.api.secret
   ```
3. Create the connector: `confluent connect cluster create --config-file connector.json --cluster <lkc> --environment <env>`:
   ```json
   {
     "name": "fanin-connector",
     "config": {
       "name": "fanin-connector",
       "connector.class": "com.example.fanin.FanInSinkConnector",
       "confluent.connector.type": "CUSTOM",
       "confluent.custom.plugin.id": "<ccp-id>",
       "confluent.custom.connect.plugin.runtime": "4.2",
       "confluent.custom.connect.java.version": "21",
       "confluent.custom.schema.registry.auto": "false",
       "kafka.auth.mode": "KAFKA_API_KEY",
       "kafka.api.key": "<api key>",
       "kafka.api.secret": "<api secret>",
       "tasks.max": "1",
       "topics.regex": "^acme\\.app\\..+",
       "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
       "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
       "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
       "fanin.target.topic": "acme-fanin-out",
       "fanin.target.bootstrap.servers": "<bootstrap host>:9092",
       "fanin.target.api.key": "<api key>",
       "fanin.target.api.secret": "<api secret>"
     }
   }
   ```
   `confluent.custom.connection.endpoints` was not needed for a cluster with a public endpoint. For private networking
   set it to `<bootstrap host>:9092:TCP` (untested).
   Provisioning takes 3 to 5 minutes.
4. Logs: `confluent connect logs <clcc-id> --level "ERROR|WARN" --start-time <YYYY-MM-DDTHH:MM:SSZ> --end-time <...>`,
   or the Console, or the topic `clcc-<id>-app-logs` (billed).
5. Update in place with `confluent connect cluster update <clcc-id> --config-file ...` (full configuration).

`scripts/cc-deploy.sh`, `cc-test.sh` and `cc-cleanup.sh` automate this for the test setup; they use environment `<environment name>`
and resources named `fanin-test-*`, and keep API secrets in process memory only.

## Reading the fan-in topic from Flink

See "Flink verification" in `docs/FINDINGS.md` (kept local, not published): the table `fanin-test-out` appears in Flink without any
setup, key and value are `BYTES`, and Kafka headers are available once the metadata column `headers` is added.

### Event time and late records

`$rowtime` of the fan-in table is the source record's timestamp (see "Output modes"). One target partition gets
records from many source partitions, so the fan-in topic is **not in event-time order**: a source topic that is behind,
a new topic found about 4 minutes after it was created, or a task restart all put older records behind newer ones.

The default Confluent Cloud watermark is the largest event time seen in a Kafka partition minus 180 ms. With that
default, window aggregations can drop records from a source that is behind. Set a watermark delay that covers the
expected lag between source topics, for example:

```sql
ALTER TABLE `acme-fanin-out` MODIFY WATERMARK FOR `$rowtime` AS `$rowtime` - INTERVAL '5' MINUTE;
```

A larger delay keeps more records but makes window results arrive later. After a redeploy, which re-reads all
topics, the old records are behind the watermark, so closed windows are not counted twice. Not tested on Confluent
Cloud yet.

## Production readiness: what is not done

- **Monitoring and alerting**: none. Watch the connector status, consumer lag of the group `connect-<id>`, and the app
  log topic. A failed task needs an operator.
- **Multi-region / disaster recovery**: none. The connector reads and writes one cluster.
- **Schema-aware handling**: none. Records are opaque bytes; the target topic has no schema.
- **Exactly-once**: not available; consumers must handle duplicates.
- **Discovery latency** of about 4 to 5 minutes, and **no automatic rebalancing of work** beyond what Connect gives.
- **Security**: a single API key is used both for the connector's consumer and its producer. Rotate it and scope it
  with ACLs; there is no OAuth support.
- **Load and capacity**: the maximum throughput was not measured on Confluent Cloud (the 50 MB/s test was dropped).
  Locally about 9,000 records/s (9 MB/s) on a laptop with a single broker, which says nothing about Cloud.
- **Tests**: unit tests (38), local integration tests (14 runs), Cloud tests for the points in FINDINGS. No long-running
  soak test, no test with the real 1,000 to 2,000 topics on Cloud (500 topics were tested locally).
- **Support model**: Confluent supports the Connect platform for custom connectors. The connector code, its bugs and its
  upgrades are owned by the customer. Test against each new runtime (the custom connector runtimes have end-of-life
  dates).
