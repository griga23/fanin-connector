# Control topic

The control topic is a list of the source topics that the connector forwards. The connector still subscribes wide
with `topics.regex`. A record of a topic that is not on the list is not forwarded. You add and remove topics by
writing records to the control topic. The connector does not restart.

## Configuration

| Property | Type | Default | Description |
|---|---|---|---|
| `fanin.control.topic` | string | empty (off) | Compacted topic with the list of allowed topics. Empty turns the feature off. |
| `fanin.control.add.start` | `EARLIEST` or `LATEST` | `EARLIEST` | Where an added topic starts. `EARLIEST`: at its committed offset, or at the beginning if it has none, so the first add of an existing topic forwards its full history. `LATEST`: at the time of the add record. A re-add after a remove always starts at the add time. See "Applying changes". |
| `fanin.control.missing.warn.ms` | long | 3600000 | Time after an add before the connector logs a WARN for a name without a topic. |
| `fanin.control.load.timeout.ms` | long | 60000 | Maximum time for a task to read the control topic when it starts. The task fails after this time. |
| `fanin.control.max.lag.ms` | long | 60000 | Maximum time that the control reader can be behind. After this time the task forwards nothing until the reader has caught up. Minimum 17000. See "Reader health". |

The connector rejects these configurations:

- The control topic matches `topics.regex` (or is in `topics`).
- The control topic is the target topic. Every forwarded record would then be read as a control record.

The control topic must be on the cluster of the target topic. The task reads it with the `fanin.target.*`
connection settings, so with the same API key. That key needs READ and DESCRIBE on the control topic. The reader
uses no consumer group. The full ACL list is in `README.md`, "Deploy on Confluent Cloud".

## Record format

- Key: the full name of the source topic, UTF-8 text. The key is used as it is. A key with white space at the start
  or end gets a WARN line, because it does not match a topic name without that white space. The key is not trimmed:
  compaction treats `a ` and `a` as two keys, so a trimmed key could never be removed by a tombstone.
- Value not null (any content, for example `1`): add the topic.
- Value null (tombstone): remove the topic.
- The time of the change is the Kafka timestamp of the record. A record without a timestamp is skipped with a WARN
  line.
- A record without a key is skipped with a WARN line.

Create the topic with `cleanup.policy=compact` and exactly one partition. The task fails at start if the topic has
another number of partitions, and the reader checks the number again every minute. One partition gives all changes
one order. Compaction keeps the last record of each name, so the topic always holds the current list.

Example with the Kafka console producer:

```
kafka-console-producer --bootstrap-server ... --topic demo-c-control \
  --property parse.key=true --property key.separator=' ' --property null.marker=NULL
src.demo-c-a 1
src.demo-c-a NULL
```

## Design

### Reading the control topic

- When a task starts, it reads the control topic from the beginning to the end offset of that moment
  (`ControlReader.loadInitial`). The task fails if this takes longer than `fanin.control.load.timeout.ms`, if the
  control topic does not exist, or if it does not have exactly one partition.
- After that, a daemon thread of the task reads new control records (`ControlReader.startBackground`). Only this
  thread uses the control consumer.
- The control consumer uses `assign()` and no consumer group. It commits no offsets. It reads with
  `isolation.level=read_committed`, so records of an aborted transaction are not applied.
- The time of a change is the Kafka timestamp of the control record. All tasks see the same time.

### Reader health

- The reader thread retries an error with a backoff (0.5 s, doubled up to 30 s). Only an authorization error or a
  wrong partition count stops it. Then the next `put()` fails the task. A failed task stays failed until somebody
  restarts it, and on Confluent Cloud there is no restart API for a custom connector, so the reader does not stop
  for errors that can go away (for example a broker that cannot be reached, or a control topic that was deleted).
- Every 5 seconds the reader fetches the end offset of the control partition at time X. When its position reaches
  that end offset, the reader is "caught up as of X": all control records written before X are in the state.
- At the start of each `put()` the task compares the time now with X. If the difference is more than
  `fanin.control.max.lag.ms`, the task logs a WARN line, pauses all its partitions and drops every record. It keeps
  the commit of each partition at the first record that it dropped for this reason. When the reader has caught up
  again, the task logs an INFO line, moves each such partition back to that record and resumes. The records are
  then filtered with the current list. No record of an allowed topic is lost.
- Result: a remove takes effect within `fanin.control.max.lag.ms` plus one `put()` interval, also when the control
  topic cannot be read. In normal operation it takes effect at the next `put()` after the reader has read it. "Takes
  effect" means: from then on the task drops the records at or after the remove cut (see "Applying changes"). Which
  records are forwarded does not depend on this delay, except for the records at or after the cut that the task
  forwarded before it applied the remove (see "Limits", "Remove delay").
- The connector rejects `fanin.control.max.lag.ms` below 17000 ms. A healthy reader can look that far behind: it
  measures every 5 s (`ControlReader.CHECK_MS`), the measurement starts after a poll of up to 1 s, the end offset
  request can take up to 10 s, and the reader reaches that end offset after one more poll. A smaller value would
  stop the task every few seconds although the reader is fine. The minimum is computed from these constants
  (`FanInSinkConfig.MIN_CONTROL_MAX_LAG_MS`).

### Applying changes

The framework allows calls to `SinkTaskContext.pause`, `resume` and `offset` only on the task thread. The background
thread therefore only updates `ControlState`. The task thread takes the net changes with `drainChanges()` at the
start of each `put()` call. The add and remove times come in the same `Changes` object, taken under one lock. The
framework calls `put()` on every poll, also with an empty batch, so a change takes effect within about one poll
interval even when all partitions are paused.

- **Remove (time-based, like the add):** for each assigned partition of the topic the task looks up the **remove
  cut**: the first offset whose record timestamp is at or after the remove time (`ListOffsets` by time, as for the
  add), or the end offset of the partition if there is no such record yet. Then:
  - Records below the remove cut were written before the remove time. They are forwarded, also when the task reads
    them long after the remove (consumer lag).
  - Records at or after the remove cut are dropped.
  - The partition keeps running until its position reaches the remove cut. When the task reads the first record at
    or after the cut (or its known position is already there when the cut is looked up), it pauses the partition at
    the end of that `put()`.
  - The commit stays at the first dropped record at or after the cut. Records before it are committed, so the
    framework does not deliver them again. Reason: if the add comes soon after the remove, but in a later `put()`,
    the dropped records can include records written after the add. A re-add moves the partition to the add time but
    never below the committed offset, so a commit past those records would lose them.
  - The ledger of the partition becomes `removed:<remove time>:<remove cut>` (see the ledger section), so another
    owner applies the same cut.
  - The lookups of one `put()` are one batch: first the end offsets, then the offsets for the remove times, then the
    offsets for the add times. A partition without a record at or after its time gets the end offset read before
    that time lookup. Read after it, the end offset could include records appended between the two calls, with a
    timestamp at or after the time: forwarded after a remove, skipped after an add. The remove times are asked
    before the add times, so that an add cut in the same interval is never below its remove cut. The lookups run at
    the start of `put()`, before the records of the batch are filtered.
  - If the lookup fails for a partition, that partition is held, as a pending add is: it is paused, its records are
    dropped and the commit stays at the first of them, and the ledger stays as it was before the remove (so a
    restart looks the cut up again). `put()` tries again every 10 seconds. When the lookup works, the partition moves
    back to the first held record, so no record before the remove cut is lost. Other partitions are not affected.
  - A partition that has no remove cut forwards nothing more after the remove: a partition whose add cut was not
    applied yet (pending), a partition without a known ledger, and a partition whose committed ledger is
    `removed:<t>` without a cut.
- **Add (`EARLIEST`, the default):** the task resumes the partitions. They continue at the committed offset, or at
  the consumer's reset policy if there is none (`earliest` in Kafka Connect, unless `fanin.start.timestamp` is set,
  see "Where reading starts" in `README.md`). A topic that was not on the list keeps the offset it had: its
  partitions are paused from `open()` on and read nothing. So the first add of an existing topic forwards its full
  history (tested in LocalIT L20). Exception: a partition whose committed ledger shows a remove is cut at
  the add time, as in `open()`. This is the case for `removed:<t>`, and for `allowed:<t>` when this task saw the
  topic as not allowed after it opened the partition (the remove tombstone can be gone). A partition whose ledger is
  not known yet is decided with the same rule when the lookup works.
- **Add (`LATEST`):** the partitions of the topic become "pending". The task looks up the first record at or after
  the add time (or the end if there is no such record) and moves the partition there. Older records are not
  forwarded.
- **Add after a remove (re-add):** always as `LATEST`, also with `EARLIEST`. Records written while the topic was
  removed are not forwarded (hard cut). If the partition has not reached its remove cut yet, the records before the
  remove cut are forwarded first: records with a timestamp before the remove time are forwarded, the records from the
  remove cut to the add cut are dropped, and records from the add cut on are forwarded. The partition moves to the add
  cut when it reads the first record at or after the remove cut. If the add cut is at or before the remove cut,
  nothing is dropped. This is the same for a re-add in the same interval as the remove and in a later one. A move
  never goes below the committed offset or the last forwarded offset.
- **Add then remove** within one interval: the net change is "removed". **Remove then add** within one interval:
  the net change is "added", marked as re-add. For a partition that was running, the task looks up both cuts (the
  remove cut at the remove time and the add cut at the add time) in the same `put()`.
- An add of a topic that is already allowed changes nothing. It does not move the add time.

Rules for pending partitions and moves:

- While a partition is pending (or its remove cut is not looked up yet), it stays paused and all its records are
  dropped. The commit of the partition stays at the first record that was dropped for this reason, so the move can
  still go back to it.
- If the lookup fails, the partition stays pending. `put()` tries again every 10 seconds. The offset lookups are per
  partition: a partition whose lookup fails (for example no DESCRIBE on its topic) stays pending alone, and the
  other partitions of the same batch are decided and forward.
- A retry of a decision that `open()` could not make uses the committed offset and ledger that `open()` saw, not
  the offset that this task committed for the paused partition since then.
- `put()` decides a move at its start and issues it with `context.offset` at its end. A move never goes below the
  committed offset of the partition, and never below the offset after the last record that this task forwarded.
  So a re-add in the same interval does not deliver records again that were forwarded before.
- Records of the current batch below the cut are dropped. This check comes before the check of the remove cut, so
  it also holds when the topic is removed again before the framework delivers the batch again after a
  `RetriableException`: records below the add cut (written while the topic was removed) are not forwarded although
  they are below the new remove cut.
- Resumes happen only at the end of a `put()` that completed. After a `RetriableException` the framework delivers the
  same batch again with all partitions paused (`WorkerSinkTask.deliverMessages`). A resume in that time would let
  the consumer fetch new records into the batch.
- If the offsets of a partition go back without a move of this task (the topic was deleted and created again, or
  its offsets were reset), the task clears its old cut position for that partition and logs an INFO line. Records
  of the new topic are then not dropped as "below the cut".

### The cut ledger in the committed offset metadata

A cut must give the same result after a rebalance, a restart and the compaction of the control topic. The add time
alone does not do this:

- A source record can have a timestamp before the add time although it was written after the add (event time,
  clock skew). A cut that is computed again at the add time can then move the partition past records that were
  allowed when they were written.
- A repeated add (for example a script that writes the whole list again) changes nothing in memory, but after
  compaction only the later add is left. A task that starts after compaction would cut at the later time.

So each partition carries the control state that the task applied, in the metadata of its committed offset
(`ControlLedger`):

- `fanin.ctl:v1:allowed:<add time>`: the cut for this add was applied, and the committed offset is at or after it.
- `fanin.ctl:v2:removed:<remove time>:<remove cut>`: the topic was removed at that time. Records below the remove
  cut offset are forwarded, records at or after it are not. The next add must cut. This form has its own prefix
  `v2`, so that the format change is visible in the metadata and a reader that knows only `v1` rejects it by its
  prefix. The build before the time-based remove (`5e38333`) reads it as no ledger under either prefix: its parser
  gives no ledger for `v1:removed:` followed by anything else than a number. The first build of the time-based
  remove (`8b46a87`) wrote the form as `fanin.ctl:v1:removed:<remove time>:<remove cut>`; that form is still read.
- `fanin.ctl:v1:removed:<remove time>` (no cut): the topic was removed at that time and no record after the committed
  offset is forwarded before the next add. The task writes this when it does not know a cut (the partition had no
  cut applied when the remove came, or the remove record was compacted away). A ledger written by the build before
  the time-based remove has this form too, and it keeps its old meaning. The next add must cut.

The parser is strict: times and cuts are decimal digits only (no sign, no blanks, at most the largest long). Any
other text gives no ledger, with one exception: a `removed` ledger with a valid time and a cut that is not valid is
read as `removed:<time>` without a cut, which forwards nothing more and cuts at the next add. All forms are read under
both prefixes.

A cut from a ledger is checked against the end offset of the partition when it is applied (`open()`, or the retry
of an `open()` that could not read the committed offsets). The end offset is read in the same end offset call as the
fallbacks of the time lookups. A cut after the end offset cannot come from this log: a foreign or damaged ledger, or
a log that was truncated (see "Limits"). It is capped at the end offset, with a WARN line. If that end offset lookup
fails, the partition is handled as if `open()` could not read its ledger: it stays paused, nothing is committed for
the task, and the retry reads the ledger again.

The remove cut is stored, and not computed again from the remove time after a restart, for two reasons. When no
record at or after the remove time existed at the lookup, the cut is the end offset of that moment; a lookup later
would give a later offset, and records with an old timestamp written in between would be forwarded by one owner and
dropped by the other. And a stored cut needs no broker call in `open()`. The add cut is not stored, because the
`allowed` ledger already makes sure that it is applied once.

`preCommit` returns each offset with this metadata:

- Topic not allowed, remove cut known: `removed:<remove time>:<remove cut>`.
- Topic not allowed, remove cut not looked up yet: the ledger from before the remove (`allowed:<add time>`), so that
  a restart looks the cut up again. The commit is held at the first held record.
- Topic not allowed, no cut: `removed:<time of the last remove that the task knows>`. If the remove record was
  compacted away and the committed ledger says `allowed:<t>`, the task writes `removed:<t>`. A topic that was never
  on the list gets no ledger.
- When the task turns a committed `allowed:<t>` into a removed ledger (the topic was removed since), it uses the
  remove time if the control topic still shows the remove, and `t` only when the remove record is gone. Before, it
  always used `t`. On Confluent Cloud with `5e38333`, after "remove A" and "add A again" with no commit in between,
  the commit made between the re-add and the framework's seek to the add cut carried
  `fanin.ctl:v1:removed:<first add time>`. The decisions do not use the time of a removed ledger, so the result was
  the same, but the time was misleading.
- Topic allowed again, but the partition has not reached its remove cut yet: `removed:<remove time>:<remove cut>`,
  so a restart forwards up to the remove cut and then cuts at the add time.
- Topic allowed, start position applied, offset at or after the cut: `allowed:<add time>`.
- Topic allowed, but pending, or the offset is still below the cut (the framework has not yet applied the move):
  the ledger stays as it was (or `removed:<time>` if the task saw a remove after the committed ledger).
- The offset is lowered to the first record dropped while pending, while the remove cut was not known or while the
  reader was behind, to a move that the framework has not applied yet, and, for a removed topic, to the first
  dropped record at or after the remove cut (without a cut: the first dropped record written at or after the remove
  time).
- A partition that is not assigned is left out (see "How the metadata reaches the commit").
- A pending partition whose ledger is known (the committed offset lookup worked, only its offset lookup failed) is
  returned with its committed ledger and an offset that is not past its first dropped record. Its commit does not
  move, and the other partitions commit normally. It is not left out of the map: the framework would then commit
  the value of `lastCommittedOffsets`, which after an assignment or a move has empty metadata (lines 739 and 673),
  and the ledger would be lost.
- If the committed ledger of an assigned partition is not known (the committed offset lookup of `open()` failed),
  `preCommit` returns an empty map, so nothing is committed for the task. A commit would replace the committed
  ledger with empty metadata, because the framework adds every assigned partition to the commit (see below). This
  lasts only until the committed offset lookup works again; it is one request for all partitions.
- While commits are held for either reason, `preCommit` logs a WARN line at most once per minute. It names the
  partitions and the error of their last lookup.

`open()` reads the committed offsets with their metadata and decides per partition of an allowed topic:

| Committed ledger | Cut at the add time? |
|---|---|
| `allowed:<t>` | No, the committed offset is kept. Exception: the control topic shows a remove of the topic and the current add time is not `t`. Then the remove cut is looked up at the remove time, and the partition is cut as a re-add (see the `removed:<t>:<cut>` row). |
| `removed:<t>:<cut>` | Yes (re-add). If the committed offset is before the remove cut, the partition is not moved yet: it forwards up to the remove cut and then moves to the add cut. Otherwise it moves to the add cut, but not below the committed offset. |
| `removed:<t>` | Yes (re-add). This also works with `EARLIEST` after the remove tombstone is gone. |
| none (first assignment, offset written without this feature, offset expired) | `LATEST`: yes. `EARLIEST`: only if the control topic shows a remove. |

And per partition of a topic that is not allowed:

| Committed ledger | Result |
|---|---|
| `removed:<t>:<cut>` | The cut of the ledger is used, without a time lookup (capped at the end offset, see above). If the committed offset is before the cut, the partition runs until it reaches the cut; otherwise it stays paused. |
| `allowed:<t>` and the control topic shows the remove | The task that applied the remove had not committed since. The remove cut is looked up at the remove time, as in `put()`. If no record at or after the remove time existed at the first lookup, this lookup can give a later end offset (see "Limits", "Source timestamps"). |
| `allowed:<t>` and the remove record is gone | No cut: the partition stays paused (the remove time is not known). |
| `removed:<t>`, or none | No cut: the partition stays paused. |

The exception in the first row does not compare the remove time with `t`, because the remove and the add can be
written by writers with different clocks. The current add time is the time of the first add after the last remove
(a repeated add does not change it). If it differs from `t`, the add of the ledger came before that remove.
Compaction keeps this true: a record is removed only when a newer record of the same key exists, so a remove that is
still in the topic is followed by all later records of that topic, including the first add after it.

A cut in `open()` moves a partition only if its committed offset is before the cut. A partition is never moved
below its committed offset.

If `open()` could not read the committed offsets, the tables above are applied when the retry reads them. An add of
the topic that comes before the retry does not change this: the remove cut of a `removed:<t>:<cut>` ledger is taken
whenever the task reads the ledger of a partition for the first time. (Before, such an add made the task skip the
ledger's remove cut and move to the add cut, and the records below the remove cut were lost.) A partition that is
decided without a move reads again the records that it dropped while it was not decided, so that they are filtered
with the decision and its commit is not held at them.

How the metadata reaches the commit (Kafka Connect 4.2.0, `WorkerSinkTask.java`):

- Line 472: the commit map starts as a copy of `lastCommittedOffsets` for the partitions.
- Lines 479 to 487: an offset from `preCommit` is used only if it is not greater than the current offset and the
  partition is assigned. The task returns only offsets of the current map, and never a greater offset.
- Line 494: the commit is skipped only if the whole map equals `lastCommittedOffsets`. `OffsetAndMetadata.equals`
  compares the metadata too, so a change of the ledger alone causes a commit.
- Line 739 (assignment) and line 673 (`rewind`, after a move): `lastCommittedOffsets` gets the position with empty
  metadata. The first commit after an assignment or a move is therefore never skipped: it writes the ledger and,
  after a move, the new offset.
- Line 305: after a successful commit `lastCommittedOffsets` holds the committed offsets with the ledger.
- Line 692: when partitions are revoked, the framework calls `preCommit` and commits synchronously, so the ledger is
  written before another task opens the partition.
- Lines 687 and 702: after a normal revoke (not "lost") the framework keeps the revoked partition in
  `currentOffsets`. When the task stops, `closeAllPartitions` passes it to `preCommit` again. Lines 479 to 487 ignore
  an offset for a partition that is not assigned. `preCommit` therefore leaves out every partition that is not in
  `context.assignment()`. Such a partition never blocks the commit of the others.

This corrects the earlier text of this document. The framework does not skip the commit after a move because the
offset is unchanged. The order is the problem: `iteration()` commits at line 245, before `poll()` applies the move
at line 346. A commit in that window holds the old offset. The ledger handles this: until the move is applied,
`preCommit` keeps the old ledger and the offset of the move as the upper limit, so a rebalance in that window
applies the cut again in `open()`.

### Lookups in open()

`open()` runs inside the consumer's rebalance callback. It makes one batch of at most four broker calls for all
its partitions, in this order: committed offsets with metadata; end offsets (for every partition with a time lookup
or with a remove cut from its ledger); offsets for times for the remove cuts that must be looked up; offsets for times
(one `ListOffsets` request with a timestamp per partition) for the add cuts and start timestamps. The end offsets are
read first, so that the fallback of a time lookup never includes a record appended after that lookup. The
remove-time call is made only when a committed ledger does not show a remove that the control topic shows. A lookup
of a remove cut in `put()` skips the committed offset call when no other partition of the same lookup needs it (a
pending add does). If `fanin.start.timestamp` and an add time both apply to a partition, the later time is used. The
four calls together have a time budget of 20 seconds. If the committed offset call fails or the budget is used up
before it returns, all the partitions of the batch become pending and stay paused, and `put()` tries again every 10
seconds. The three offset calls are per partition (`AdminLookup` reads the future of each partition of the one
`ListOffsets` request): only the partitions whose lookup fails become pending. So one topic without DESCRIBE,
or one deleted topic, does not stop the other topics of an eager assignment.

### Missing topics and wrong names

Only task 0 does these checks, so that the log has each line once.

- When a name is added that does not match `topics.regex` (or is not in `topics`), the task logs:
  `Control topic lists <name> but it does not match the subscription of this connector`.
- At most once per minute, `put()` checks the allowed names that are older than `fanin.control.missing.warn.ms`.
  For a name that is not assigned and not in the topic list of the cluster (`Admin.listTopics`, cached for one
  minute), it logs one WARN line:
  `Control topic lists <name> but no such topic exists for <n> min`.
  A task logs the line once per name. It comes again after the name was removed and added again, and after every
  task restart (a config update, a pause and resume that restarts the task, a rebalance of tasks), because the task
  keeps the "already logged" state only in memory.

### Feature off

With an empty `fanin.control.topic` the task creates no control consumer, pauses nothing, drops nothing and commits
the offsets as the framework gives them. All earlier tests pass unchanged.

### Code structure

`FanInSinkTask` copies records to the target topic, as before the control topic. It keeps the configuration, the
producer, the record conversion, the start timestamp and the lifecycle (`start`, `open`, `put`, `preCommit`, `close`,
`stop`). When `fanin.control.topic` is set, it creates a `ControlCoordinator` in `start()` and calls it at fixed
points. Without the setting there is no coordinator, and the task runs the same code as without this feature.

| Task method | Coordinator call |
|---|---|
| `start()` | `ControlCoordinator.start(...)`: reads the control topic to its end, then starts the reader thread |
| `open(partitions)` | `onOpen(partitions)`, in place of the start timestamp code (the coordinator applies the start timestamp too) |
| `put(records)` | `beforePut()`, then per record `accept(record)` (false: the record is dropped) and `forwarded(record)` after the send, then `afterPut(records)` |
| `preCommit(offsets)` | `preCommit(offsets)`, after the producer flush |
| `close(partitions)` | `onClose(partitions)` |
| `stop()` | `stop()`: closes the reader |

The control classes, all in `com.example.fanin`:

- `ControlCoordinator`: the entry point. Owns the reader and the `ControlState`. Applies the control changes at the
  start of `put()`, stops forwarding while the reader is behind, decides per record whether it is forwarded, issues
  the seeks, pauses and resumes partitions, and logs the drop summary and the missing-topic and subscription
  warnings. All calls of the `SinkTaskContext` are here, on the task thread.
- `ControlCuts`: decides the start position (add cut, start timestamp) and the remove cut of partitions, with the
  broker lookups in `open()` and the retries in `put()`. It returns the moves for `open()` and puts the seeks of
  running partitions into `ControlPartitions`; it makes no consumer calls.
- `ControlCommits`: the offsets and ledgers that `preCommit()` returns, the commit holds, and the WARN about held
  commits.
- `ControlPartitions`: the shared state of the three classes above: the control list as the task applies it and what
  the task knows about each assigned partition (pending decisions, cuts, floors, holds, positions, seeks). It has no
  broker or consumer calls.
- `ControlReader` and `ControlState`: read the control topic on a background thread and hold the list and its changes.
- `ControlLedger`: the ledger format in the committed offset metadata.
- `StartOffsets.Lookup` and `AdminLookup`: the broker lookups (committed offsets, offsets for times, end offsets,
  topic names).

The control classes log under the logger of `FanInSinkTask`, so log lines and log configuration are the same as when
all of this code was in the task. The test hooks stay on the task: `createControlReader`, `createStartLookup`, `warn`,
`clock` and `controlState()`.

## Limits

- **Remove delay.** The task applies a remove at its next `put()` after the reader read the control record: at most
  `fanin.control.max.lag.ms` plus one `put()` interval after the remove time, in normal operation about one `put()`
  interval (measured on Confluent Cloud: 9 to 49 seconds on an idle connector). The delay no longer decides which
  records are forwarded: the remove cut is found by the remove time, so records written before the remove are
  forwarded and records at or after the cut are dropped, however late the task reads them. What remains: records at
  or after the remove cut that the task already forwarded before it applied the remove stay forwarded. These are the
  records that were written after the remove time and that the task read before it applied the remove, so at most
  the records of that delay. When the consumer is behind by more than the delay, there are none.
- **Source timestamps (add and remove).** Both cuts use the record timestamps of the source topic (`ListOffsets` by
  time returns the first offset in log order with a timestamp at or after the given time). The timestamps come from
  the producer clock or from the event time that the producer sets, and the add and remove times come from the clock
  of the writer of the control record. So:
  - Add: a source record with a timestamp later than the add time that was written before the add is forwarded. If
    one record that was written while the topic was removed has a timestamp after the new add time (a producer clock
    ahead, or a wrong event time), the add cut lands on that record, and every later record of the partition is
    forwarded, also the other records written while the topic was removed.
  - Remove: if one record written before the remove has a timestamp at or after the remove time (a producer clock
    ahead), the remove cut lands on that record, and it and every later record are dropped, also records written
    before the remove. A record after the remove cut with a timestamp before the remove time (a producer clock behind,
    or an old event time) is dropped too: the cut is an offset, not a filter per record.
  - Remove without a record at or after the remove time: the cut is the end offset read just before the time
    lookup. Records written after that moment are dropped, whatever their timestamp. For an add without a record at
    or after the add time, the add cut is that end offset too, so records appended after it are forwarded. If the remove is not yet in the
    committed ledger when the partition gets a new owner, the new owner looks the cut up again and can get a later
    end offset; records with an old timestamp written in between are then forwarded by the new owner although the
    old owner dropped them. Nothing is lost or forwarded twice by this.
  - The ledger does not change any of this; it only makes sure that a cut is applied once and that every owner
    applies the same remove cut.
- **Several changes before the remove cut is reached.** A remove, add and remove again of a topic while a partition
  has not reached the cut of the first remove (a long consumer lag): the partition keeps the first remove cut, and
  the records of the short time between the add and the second remove are not forwarded. An add and a remove in one
  interval (net change "removed"), or a remove of a partition whose add cut was not applied yet (lookup failing):
  nothing more of the partition is forwarded, also not the records written between that add and the remove.
- **No ledger.** The ledger protects only partitions with a committed offset written by this version. Without it
  (first assignment, an upgrade from a version without the ledger, or an expired offset), the add-time rule
  applies, with the risks of the point above. After compaction, a repeated add then gives the later add time.
- **Ledger cut after the end of the log.** A cut from the ledger that is after the end offset of the partition is
  capped at the end offset (WARN line). This is right for a foreign or damaged ledger. For a truncated log (topic
  deleted and created again with the same name, or an unclean leader election) it is a guess: the records of the
  new log below its end offset are forwarded, whatever their timestamp, and later records are dropped until the next
  add. A cut that is below the end offset of a truncated log cannot be detected, and the old cut is used.
- **Going back to an older build.** The remove cut is written as `fanin.ctl:v2:removed:<t>:<cut>`. A build before
  `v2` (`8b46a87` and older) reads this as no ledger, so the remove cut is lost, and so is the fact that the topic
  was removed: the partition is treated as one without a ledger (the "No ledger" point above). For `8b46a87` this is
  worse than before, because it could read its own `v1` form with a cut. `allowed:<t>` and `removed:<t>` are still
  written under `v1` and are read by older builds as before.
- **First add with `EARLIEST` (the default).** The first add of a topic that already has records forwards all of
  them, from its committed offset or, without one, from the beginning of the topic (the consumer's reset policy).
  For a large topic this is a backlog: set `fanin.start.timestamp` to limit it, or use `LATEST`. Only the first add
  works like this. A re-add after a remove is always a hard cut at the add time, also with `EARLIEST`: records
  written while the topic was removed are not forwarded.
- **Offset retention.** Committed offsets, and with them the ledger, expire when the consumer group has had no
  member for `offsets.retention.minutes` (broker default 7 days). A paused partition keeps its offset while the
  connector runs, because the framework commits the offsets of all assigned partitions. After a longer stop of the
  connector, partitions start as on a first assignment: `LATEST` topics at their add time, `EARLIEST` topics at the
  beginning of the topic (or at `fanin.start.timestamp`), so their history is forwarded again.
- **Remove and re-add while no task owns the partition.** If a topic is removed and added again while the connector
  is stopped, and compaction has removed the tombstone before the next start, the task sees only the add. The
  committed ledger `allowed:<old add time>` then keeps the committed offset, and the records written while the topic
  was removed are forwarded.
- **Wrong consumer group.** The ledger is read from the group in `fanin.start.consumer.group` (default
  `connect-<connector name>`). If `open()` does not find that group, although the task's own consumer is a member
  of its group at that time, the name is wrong (a typo, or `consumer.override.group.id`). The task then logs one WARN
  line per task life that names the setting. It keeps the offset and ledger that its own `preCommit` returned for
  each partition and uses them in place of the committed ones, so a later rebalance in the same task does not apply
  a cut again. A new task (after a restart) has no such memory: each partition then starts at the add time of its
  topic once more, and the start timestamp is not applied. The task does not fail at start for this, because at
  `start()` its consumer has not joined the group yet, so a new connector's group does not exist yet and a missing
  group is not a reliable sign of a wrong name. Failing in `open()` was not chosen either: a failed custom connector
  task on Confluent Cloud stays failed until a config update restarts it.
- **Duplicates.** A move never goes below the committed offset or the last forwarded offset. Duplicates are still
  possible in the normal at-least-once cases: a task that fails after a send and before the commit, and a batch that
  the framework delivers again after a `RetriableException` (the records of that batch that were sent before the
  error are sent again).
- **Lookups.** A partition whose offset lookup keeps failing (for example the API key has no DESCRIBE on its
  topic) stays paused and its commit does not move, but only that partition; the other partitions forward and
  commit. A WARN line once per minute names it. If the committed offset lookup of the consumer group fails, no
  offsets are committed for the task until it works again, because the ledger of the new partitions is not known.
  The consumer lag grows but nothing is lost. If that lookup fails for good (for example no DESCRIBE on the consumer
  group), the task commits nothing, and after a restart it reads again from the last commit.
- With `tasks.max` > 1, every task reads the whole control topic. Each task applies a change when its own `put()`
  runs, so the tasks apply a change at slightly different times. The cut is the same for all tasks.
- A topic with no assigned partition on a task is handled when the partitions are assigned (`open()`).
- Records of a topic that is not allowed are still fetched until the pause takes effect (for a removed topic: until
  the partition reaches its remove cut), and they count in the consumer lag of the paused partitions. The lag of a removed topic grows while it is removed.
- The missing-topic check sees only topics that the API key can describe.
- **Control topic deleted and created again.** The reader is not told. Its position is past the end of the new
  topic, so the consumer resets to the beginning (`auto.offset.reset=earliest`) and applies the records of the new
  topic on top of the state it has. A topic that was allowed in the old control topic and is not in the new one stays
  allowed until a tombstone for it is written. The task does not detect this (the consumer API gives no topic ID).
  After you create the control topic again, restart the tasks (for example with a config update), so that each task
  reads the new topic from the start.

## Deviations from the plan

1. **Add starts at the add time, not at the end offset.** The plan said "set `context.offset(tp, endOffset)`".
   The end offset is different for each task and each moment. The add time is in the control record and is the
   same for every task. If no record is at or after the add time, the partition starts at its end, as in the plan.
2. **`Changes` has a third set `readded`** instead of a flag, so one drain can hold several re-added topics. It also
   holds the add and remove times.
3. **`ControlState.removedAt()`** is added. A task that starts uses it to apply the re-add rule to topics that the
   control topic history shows as removed.
4. **`Lookup.topicNames()`** is added for the missing-topic check. `Lookup.committedWithMetadata`,
   `Lookup.offsetsForTimes` (one batched request) and `Lookup.deadline` are added for the ledger and the time
   budget. The defaults keep the old lookups working.
5. **Topic listing only when needed.** The task lists topics only when a name is old enough to warn, not every
   minute.
6. **One more WARN** for a name that does not match the subscription, because such a name can never be forwarded.
7. **`docs/` is in `.gitignore`** in this repository. This file is added with `git add -f` so that it goes with the
   feature. `docs/PR-DRAFT.md` stays local and is not committed.
8. **LocalIT runs on a second stack.** A stack `fanin-it` (ports 29092 and 8083) was in use by other work. The compose
   file and `scripts/local-it.sh` take `IT_PROJECT`, `IT_KAFKA_PORT` and `IT_CONNECT_PORT`, and LocalIT takes
   `-Dit.bootstrap` and `-Dit.connect`. The defaults are unchanged.
9. **Review fixes (ledger design).** The ledger design of the review was possible as specified. The Connect 4.2.0
   source confirms that metadata from `preCommit` reaches the commit (see "How the metadata reaches the commit").
   Additions that the design did not name:
   - `preCommit` commits nothing while the committed ledger of an assigned partition is unknown, because the
     framework would otherwise commit empty metadata for it.
   - The commit is held at the first record dropped while pending or while the reader was behind, and the reader
     recovery moves the partition back to that record. Without this, records of an allowed topic that were dropped
     in that time would be lost.
   - An allowed ledger is not trusted if the control topic shows a remove after its add time.
   - A partition that `open()` could not decide is decided from the ledger when the lookup works again. A
     partition of a topic that was added while the task ran is always cut.
   - The time budget for the lookups is a constant (20 s), not a setting.

10. **Fixes from the second review.**
    - `preCommit` leaves out partitions that are not assigned.
    - The offset lookups are per partition, and a pending partition holds only its own commit. An unknown
      ledger (failed committed offset lookup) still holds all commits of the task, because the framework would
      otherwise commit empty metadata for it; this is bounded by one group lookup and named in a WARN line.
    - A live add with `EARLIEST` uses the same ledger rule as `open()`.
    - A consumer group that is not found gives one WARN per task, and the task uses its own last commits in place of
      the committed offsets. The task does not fail; see "Limits", "Wrong consumer group".
    - `fanin.control.max.lag.ms` has a minimum of 17000. The formula is `CHECK_MS + 2 x POLL_TIMEOUT +
      REQUEST_TIMEOUT`, one poll more than the review proposed, because after the end offset request the reader
      needs one more poll to reach that end offset when new records came in.
    - The remove-after-add check compares add times, not a remove time with an add time (writers with different
      clocks). The commit of a removed topic stays at the first dropped record written at or after the remove time.
    - A deleted and recreated control topic is documented, not detected (the consumer API gives no topic ID).

11. **Time-based remove** (chosen after the second review). Before, the task dropped every record of
    a removed topic that it read after it applied the remove, also records written before the remove time that
    were still in the consumer lag. Under load (a lag of minutes) a remove dropped data written while the topic was
    allowed. Now the remove has a cut by time, like the add: records below the remove cut are forwarded, records at
    or after it are dropped, and the partition is paused when it reaches the cut. The ledger stores the cut
    (`removed:<time>:<cut>`). See "Applying changes" and the ledger section.

12. **Default `fanin.control.add.start=EARLIEST`** (chosen as the safer default; `LATEST` remains available). Before,
    the default was `LATEST`. The unit tests that were written for `LATEST` now set it in their configuration, with
    their assertions unchanged. LocalIT L17 and L18 set `LATEST`; L19 runs with the default; L20 tests the default.

## Test evidence

### Unit tests

`mvn test`: 188 tests, 0 failures (70 before this feature, 116 before the first review fixes, 146 before the second
review fixes, 159 before the time-based remove, 172 before the review of the time-based remove, 187 before the
`EARLIEST` default).

Default `EARLIEST` (2026-11-27), one new test:

- `FanInSinkConfigTest.addStart_defaultEarliest` (was `addStart_defaultLatest`): the default is `EARLIEST`, and
  `latest` is accepted. It failed before the default was changed.
- `FanInSinkTaskControlTest.addStartNotSet_firstAddKeepsTheCommittedPosition_readdCuts` (new): without the setting, a
  first add resumes at the committed offset without a move, and a re-add after a remove moves to the add cut. It
  fails when the setting is `LATEST`.
- The test classes that were written for `LATEST` set it in their `props()` helper: `FanInSinkTaskControlTest`,
  `FanInSinkTaskRemoveCutTest`, `RemoveCutEdgeCasesTest`, `ControlCutStabilityTest`, `ControlLookupRetryAndClockTest`
  and `ControlCommitAndRebalanceTest`. A test of `EARLIEST` sets it after the helper, as before. Without this, 13
  tests failed with the new default. No assertion was changed.

Test classes named after review rounds were renamed by subject (methods and assertions unchanged):
`ReviewC2Test` is now `ControlCommitAndRebalanceTest`, `ReviewC2ReaderTest` is `ControlMaxLagMinimumTest`,
`FanInSinkTaskControlReviewTest` is `ControlCutStabilityTest`, and `FanInSinkTaskControlC2Test` is
`ControlLookupRetryAndClockTest`. Three methods of `FanInSinkTaskControlTest` lost their `review_` prefix. The
methods of `RemoveCutEdgeCasesTest` lost their review prefixes and are named after their scenario. The text below
uses the new names.

Review of the time-based remove, branch `feature/control-topic-fix3`. 15 new tests:

- `RemoveCutEdgeCasesTest` (13), the tests of that review, with the assertions kept:
  - `redeliveryAfterGap_secondRemove_dropsRecordsWrittenWhileRemoved`: a `RetriableException` after the
    re-added partition entered its gap, then a second remove before the redelivery. Records 8 and 9 (written while
    the topic was removed) were forwarded; now they are not.
  - `openLookupFailed_readdBeforeRetry_forwardsRecordsBelowTheRemoveCut`: `open()` cannot read the ledger
    `removed:200:8`, the re-add comes before the retry. The partition moved to the add cut 12 and lost 5 to 7. Now
    5 to 7 are forwarded, then it moves to 12 (two assertions added for this).
    `compactedRemove_openLookupFailed_firstAddBeforeRetry_keepsTheRemoveCut` (new): the same with the remove
    record compacted away and a first add (`LATEST`). `openLookupFailed_retryBeforeReadd_keepsTheRemoveCut`: control case,
    green before and after.
  - `endOffsetFallback_removeCut_dropsRecordsAppendedBetweenTheLookups` (remove cut) and
    `endOffsetFallback_addCut_keepsRecordsAppendedBetweenTheLookups` (add cut, new): records with a time at or
    after the cut time are appended between the two lookups. The review's setup gave the end offset 10 to every
    call, which only models the old order of the calls. The fake lookup can now append records after its next call
    (`Lookup.afterNextOffsetCall`), so the setup models the window for either order; the assertion is unchanged.
  - `ledgerWithHugeCut_cutIsCappedAtTheEndOffset`: a ledger cut of `Long.MAX_VALUE` is capped at the end
    offset 6. New: `ledgerCutEndLookupFails_partitionWaitsAndRetries`, `ledgerCutBelowTheEnd_isNotCapped`.
  - `readd_noCommitBetween_ledgerCarriesTheRemoveTimeThenTheReaddState` and
    `readd_commitBetween_ledgerCarriesTheRemoveCutThenTheReaddState` (new): the ledger through remove, re-add and the
    forwarded records. The first failed before the fix with `fanin.ctl:v1:removed:100` (the first add time), as
    seen on Confluent Cloud.
  - `removeLookupFails_thenReaderStalls_thenRecovers_forwardsEachRecordOnce`,
    `idle_endCutReachedByForwardedRecords_staysUnpaused`: control cases of the review, green before and after.
- `FanInSinkTaskRemoveCutTest`: `ledger_v1FormsOfEarlierBuilds_roundTrip`, `ledger_parserIsStrict`.

These tests failed before their fix and pass after it: the redelivery test, the two tests where the re-add or first
add comes before the retry, the two end-offset fallback tests, the huge ledger cut test and the converted-ledger
test. Changed tests:

- `FanInSinkTaskControlTest.readdInOneInterval_cutsATopicThatWasNeverPaused`: the time queries are now `[200, 300]`
  (was `[300, 200]`), because the remove time is now asked first.
- `FanInSinkTaskRemoveCutTest.ledger_removedWithCut_roundTrip`: the cut form is now written under `v2`, and
  `removed:200:x` and `removed:200:-1` are now read as `removed:200` without a cut (was no ledger).
- `FanInSinkTaskRemoveCutTest.removeCut_rebalanceBeforeTheCut_newOwnerAppliesTheSameCut`,
  `removeCut_restartAfterReadd_forwardsToTheCutThenSkipsToTheAddTime` and the three `open()` lookup failure tests
  above: setup only, the end offset is set to 20. The fake answered end offset 0, a log that ends before the ledger cut 8, which the cap now
  corrects.

Time-based remove, 13 new tests in `FanInSinkTaskRemoveCutTest`:
`removeCut_forwardsRecordsBeforeTheRemoveTime`, `removeCut_dropsRecordsAtOrAfterTheRemoveTime`,
`removeCut_pausesOnceThePositionReachesTheCut`, `removeCut_positionAlreadyPastTheCut_pausesAtOnce`,
`removeCut_noRecordAtOrAfterTheRemoveTime_cutsAtTheEndOffset`, `removeCut_lookupFails_holdsAndDoesNotDrop`,
`removeThenReaddSameInterval_forwardsBeforeR_dropsBetween_forwardsAfterT`,
`removeThenReaddLaterInterval_forwardsBeforeR_dropsBetween_forwardsAfterT`,
`removeCut_restartBeforeTheCutIsReached_doesNotLoseOrDuplicate`, `removeCut_rebalanceBeforeTheCut_newOwnerAppliesTheSameCut`,
`removeCut_newOwnerWithoutTheRemoveInTheLedger_looksTheCutUpByTime`,
`removeCut_restartAfterReadd_forwardsToTheCutThenSkipsToTheAddTime`, `ledger_removedWithCut_roundTrip`. Eleven of
them failed on `15b0577` (the ledger format alone); `removeCut_positionAlreadyPastTheCut_pausesAtOnce` and
`ledger_removedWithCut_roundTrip` passed there, because the old code also paused at once and the format was already
in that commit. The fake lookup of the tests can now answer per timestamp (`Lookup.at`), so a test can give the
remove time and the add time different offsets.

Changed tests (they encoded the old remove):

- `ControlLookupRetryAndClockTest.reAddRightAfterRemove_recordsAfterTheAddTimeAreNotLost`: remove cut at offset 7. Offset
  6 (time 150, before the remove time 200) is now forwarded: forwarded `in.a@5, in.a@6, in.a@8` (was 5 and 8). The
  ledger is `removed:200:7` (was `removed:200`). The commit at 7 is unchanged.
- `ControlLookupRetryAndClockTest.restart_removeTimeFromASlowClock_stillCuts` and
  `FanInSinkTaskControlTest.restart_allowedLedger_butARemoveAfterIt_cuts`: `open()` no longer moves the partition to
  the add cut at once, because the committed offset is before the remove cut. The tests now assert no move in
  `open()`, the forwarded record before the remove cut, and the move to the add cut when the cut is reached.
- `FanInSinkTaskControlTest.readdInOneInterval_cutsATopicThatWasNeverPaused`: the lookups are now `[300, 200]` (was
  `[300]`); the remove cut is set in the setup.
- `FanInSinkTaskControlTest.preCommit_writesAllowedAndRemovedLedger`: the ledger is `removed:200:7` (was
  `removed:200`).
- `FanInSinkTaskControlTest.put_dropsRecordsOfRemovedTopic` and
  `ControlCutStabilityTest.readdInOneInterval_recordAfterCutForwardedThenSeekBack`: setup only (a remove cut
  before the records). Assertions unchanged.

Mutation checks: without the pause at the end of `put()`, 6 of the new tests fail; with a ledger without the cut, 6
tests fail; without the move back to the held records after a failed lookup,
`removeCut_lookupFails_holdsAndDoesNotDrop` fails; without the forward-to-the-remove-cut rule for a re-add, 5 tests
fail; with the old "drop all after a remove", 10 tests fail.

Second review, 13 new tests:

- `ControlCommitAndRebalanceTest` (5, the reviewer's tests):
  `stopAfterAnEarlierRevoke_finalCommitMustNotBeSkipped` (final commit),
  `onePendingPartition_mustNotHoldCommitsOfOtherPartitionsForever` (pending partition),
  `earliest_liveReaddAfterTombstoneGone_mustCut` and `earliest_liveReaddAfterTombstoneGone_allowedLedger_mustCut`
  (live add with `EARLIEST`), `groupNotFound_rebalanceMustNotRewindAgain` (consumer group not found). All 5 failed on
  `ad3d50f`. In `earliest_liveReaddAfterTombstoneGone_allowedLedger_mustCut` the record now arrives in the `put()`
  that applies the add. The reviewer delivered it in the `put()` after the seek to offset 60. The framework applies that seek before
  the next `put()`, so a record below it there means a recreated topic, and
  `ControlCutStabilityTest.recreatedTopic_staleDropBelow_dropsAllowedRecords` asserts the opposite for that
  case. The assertion is unchanged.
- `ControlMaxLagMinimumTest` (1, the reviewer's test, adapted): `fanin.control.max.lag.ms=1000` is now rejected with a
  message that names the minimum, the minimum is accepted, and a healthy reader on a `MockConsumer` stays below the
  minimum for 7 seconds. The reviewer's assertion (a healthy reader is never more than 1000 ms behind) does not match
  the design: the reader measures every 5 seconds.
- `AdminLookupTest` (2): one `ListOffsets` request, and a partition that fails with an authorization error does not
  fail the other partition.
- `ControlLookupRetryAndClockTest` (5): `eagerBatch_oneFailingTopicDoesNotStopTheOthers` (pending partition, eager assignment),
  `openRetry_usesTheCommittedOffsetThatOpenSaw`, `restart_removeTimeFromASlowClock_stillCuts`,
  `restart_removeBeforeTheLedgerAdd_clockAhead_doesNotCut`, `reAddRightAfterRemove_recordsAfterTheAddTimeAreNotLost`.
  The last three failed on `b36df83` (before the trace-minor fix). `openRetry_...` fails when the retry reads the
  committed offset again.

No earlier test was changed.

First review:

- `FanInSinkConfigTest` (8): `controlTopic_emptyMeansOff`, `addStart_defaultEarliest` (was `addStart_defaultLatest`), `addStart_rejectsUnknown`,
  `timeouts_haveDefaultsAndRejectNegative`, `controlConsumerConfigs_usesTargetCredentials`,
  `controlTopic_mustNotMatchTheSubscription`, `controlTopicEqualToTargetTopic_isRejected`,
  `maxLag_defaultAndReadCommitted`.
- `ControlStateTest` (8): the 7 earlier tests and `drain_carriesAddAndRemoveTimes`.
- `ControlReaderTest` (13): one control partition now. New: `reader_loadInitial_twoPartitionsFail`,
  `reader_recordWithoutTimestamp_skipped` (replaces `reader_recordWithoutTimestamp_usesClock`, which tested the old
  behaviour), `reader_keyWithWhiteSpace_usedAsIs`, `reader_background_transientErrorIsRetried`,
  `reader_background_reportsCaughtUpTime`, `reader_background_partitionAddedLater_fails`.
  `reader_background_failureIsReported` is now `reader_background_authorizationFailureIsReported`.
- `FanInSinkTaskControlTest` (40): the 25 earlier tests, unchanged, 3 review tests of reviewer B
  (`openLookupFails_retryMustNotRewindBelowCommitted`, `compactedRedundantAdd_mustNotMoveTheCutForward`,
  `removeThenAddInOneInterval_seeksBackOverForwardedRecords`), and 12 new tests:
  `preCommit_writesAllowedAndRemovedLedger`, `preCommit_beforeTheFrameworkAppliedTheSeek_keepsTheOldLedgerAndOffset`,
  `preCommit_issuedSeekBackward_commitsNotPastIt`, `preCommit_ledgerNotKnown_commitsNothing`,
  `restart_removedLedger_cutsAlsoWithEarliestAfterTheTombstoneIsGone`, `restart_allowedLedger_butARemoveAfterIt_cuts`,
  `notAllowed_withAllowedLedger_writesRemoved`, `readerBehind_dropsAllAndPausesAll_thenResumesAndReadsAgain`,
  `readerBehind_newPartitionsArePausedInOpen`, `resume_waitsUntilPutCompletes`, `open_oneBatchOfLookups`,
  `open_lookupsOverTheTimeBudget_pauseAndRetry`.
- `ControlCutStabilityTest` (7): the tests of reviewer A.

All 11 review tests and the config review test failed on commit `196b2f5` and pass now. Three of them now set the
ledger on the committed offset, because the fix depends on it: `restart_recordsWithOlderTimestamps_mustNotSkipCommittedLag`,
`repeatedAddAfterCompaction_mustGiveTheSameCut` and `compactedRedundantAdd_mustNotMoveTheCutForward`. In
`rebalanceAfterAdd_recordsWithOlderTimestamps_mustNotSkipCommittedLag` the committed offset now comes from
`preCommit`, as in the framework. The assertions are unchanged.

Mutation checks: without the commit hold, `preCommit_issuedSeekBackward_commitsNotPastIt` and
`readerBehind_dropsAllAndPausesAll_thenResumesAndReadsAgain` fail. Without the reader lag check, both `readerBehind_*`
tests fail. With a resume at the start of `put()`, `resume_waitsUntilPutCompletes` fails. Without the
"ledger not known" rule, `preCommit_ledgerNotKnown_commitsNothing` fails.

### Local integration tests L17 to L20

Default `EARLIEST` and the renamed test classes, 2026-11-27:
`IT_PROJECT=fanin-it3 IT_KAFKA_PORT=49092 IT_CONNECT_PORT=18103 TESTS='LocalIT#l17*+l18*+l19*+l20*' scripts/local-it.sh`,
4 tests, 0 failures, 104.5 s. `scripts/build.sh` before it: 188 unit tests, 0 failures. L17 and L18 now set
`fanin.control.add.start=LATEST`, because their expected counts assume it (L17 step 2: the older records of b stay
behind; L18: the add-time rule without a ledger). L19 runs with the default and gives the same result. New L20
tests the default: topic a gets 10 records before the connector starts and is not on the list; then it is added.

```
RESULT L17/2 PASS: add b, then 10 new records on b: target {a={s1=10}, b={s2=10}}
RESULT L17/4 PASS: add a again, then 10 records on a: target {a={s1=10, s4=10}, b={s2=10, s3=5}} (none of the 10 sent while a was removed; c never forwarded)
RESULT L18/2 PASS: repeated add and restart, 10 records with old timestamps written while paused: target {a={s1=10, s2=10}} (no records skipped)
RESULT L19/2 PASS: paused, 10 records, remove a, 10 records, resume: target {a={s1=10, s2=10}} (the 10 written before the remove arrived, the 10 after did not); committed ledger {0=fanin.ctl:v2:removed:1795765547236:10, 1=fanin.ctl:v2:removed:1795765547236:10}
RESULT L20/1 PASS: 10 records on a before its first add, add a, 10 more: target {a={s1=10, s2=10}} (the full history arrived)
RESULT L20/2 PASS: remove a, 10 records, add a again, 10 records: target {a={s1=10, s2=10, s4=10}} (none of the 10 sent while a was removed)
```

The same 4 tests also passed right after the default change, before the classes were renamed (104.3 s).

After the review of the time-based remove, commit `84a6f7a`:
`IT_PROJECT=fanin-it3 IT_KAFKA_PORT=49092 IT_CONNECT_PORT=18103 TESTS='LocalIT#l17*+l18*+l19*' scripts/local-it.sh`,
3 tests, 0 failures, 64.4 s. `scripts/build.sh` before it: 187 unit tests, 0 failures. L17 and L18 give the same
result lines as before. L19 now expects the remove cut under `fanin.ctl:v2:`:

```
RESULT L19/1 PASS: a allowed, 10 records forwarded
RESULT L19/2 PASS: paused, 10 records, remove a, 10 records, resume: target {a={s1=10, s2=10}} (the 10 written before the remove arrived, the 10 after did not); committed ledger {0=fanin.ctl:v2:removed:1795762434217:10, 1=fanin.ctl:v2:removed:1795762434217:10}
```

Time-based remove, `TESTS='LocalIT#l17*+l18*+l19*'` on the same stack, commit `efb2aae`: 3 tests, 0 failures,
64.6 s. L17 and L18 give the same result lines as before (in L17 the records of a are written after the remove, so
they are at or after the remove cut). New L19: topic a with 2 partitions.

```
RESULT L19/1 PASS: a allowed, 10 records forwarded
RESULT L19/2 PASS: paused, 10 records, remove a, 10 records, resume: target {a={s1=10, s2=10}} (the 10 written before the remove arrived, the 10 after did not); committed ledger {0=fanin.ctl:v1:removed:1795760836740:10, 1=fanin.ctl:v1:removed:1795760836740:10}
```

L19 pauses the connector, writes 10 records (`s2`), then the remove, then 10 more records (`s3`), and resumes. The
task applies the remove after the resume, while the `s2` records are still in the consumer lag. L19 was not run
against the code before this change.

Earlier runs:

`IT_PROJECT=fanin-it2 IT_KAFKA_PORT=39092 IT_CONNECT_PORT=18093 TESTS='LocalIT#l17*+l18*' scripts/local-it.sh`
(Kafka 4.2.0, one Connect worker). Run again on commit `84f8322` (second review fixes) with the same result lines,
2 tests, 0 failures, 54 s. The first run below was on commit `6fb5489` plus the new L18 test. L17: topics `a`, `b`, `c` with 2
partitions, control topic with `a` allowed. L18: topic `a` with 2 partitions.

```
RESULT L17/1 PASS: allow a only, 10 records on each of a, b, c: target {a={s1=10}}
RESULT L17/2 PASS: add b, then 10 new records on b: target {a={s1=10}, b={s2=10}}
RESULT L17/3 PASS: remove a, then 10 records on a and 5 on b: target {a={s1=10}, b={s2=10, s3=5}}
RESULT L17/4 PASS: add a again, then 10 records on a: target {a={s1=10, s4=10}, b={s2=10, s3=5}} (none of the 10 sent while a was removed; c never forwarded)
RESULT L18/1 PASS: 10 records forwarded, committed ledger {0=fanin.ctl:v1:allowed:1795715495512, 1=fanin.ctl:v1:allowed:1795715495512}
RESULT L18/2 PASS: repeated add and restart, 10 records with old timestamps written while paused: target {a={s1=10, s2=10}} (no records skipped)
```

L18 pauses the connector, writes 10 records with a source timestamp one hour before the add time, writes the same
add again, restarts the connector and resumes it. All 20 records arrive. The run of L18 against the old code
(`196b2f5`) did not reach the test: the Connect container of that temporary worktree did not load the plugin.

### Confluent Cloud (test prefix `demo-c`, before the review fixes)

This run used commit `e67bced`, before the review fixes. The sections below repeat it with the new code.

A Confluent Cloud test environment (Basic cluster, Azure germanywestcentral), 2026-11-26.

Names in this and the following Confluent Cloud sections: source topics are shown with the default naming of
`scripts/cloud-test/control/` (prefix `src.`, no suffix); the runs used a longer prefix and a suffix. Connector
IDs are shown as `<connector-id>`. All other parts of the log lines, and all numbers, are as measured.

- Plugin `demo-p-fanin-plugin-ctl-e67bced`, the test plugin built from commit `e67bced`.
- Connector `fanin-demo-c` (the test connector), runtime 4.2, Java 21, `tasks.max=1`,
  `topics.regex=^src\.demo-c-.+$`, target `demo-c-fanin-out`,
  `fanin.control.topic=demo-c-control`, `fanin.control.missing.warn.ms=120000`, `fanin.timestamp.mode=COPY`.
  The connector config was created by a script that reads the credentials from a mode-600 file and writes the
  secrets only to a mode-600 temporary file.
- Topics: `demo-c-control` (1 partition, compacted), `demo-c-fanin-out` (6 partitions), and the order topics
  `src.demo-c-{a,b,c}` (3 partitions) created before the connector.
- Each producer run: a test-data generator writes 60 records with one fixed JSON payload to `src.demo-c-<x>` and
  registers the value schema.
- Control records were written with a small `confluent_kafka` producer (key = topic name, value `1` or null).
- Counts come from a read of the whole target topic, grouped by the header `fanin.source.topic`.

The control consumer needs no consumer-group ACL: it uses `assign()` without `group.id`. The task started with
`Read 1 records of control topic demo-c-control: 1 allowed topics [src.demo-c-a]` and
`Control topic: paused 6 assigned partitions of topics that are not allowed`.

| Step | Action | Source end offsets a / b / c | Target records a / b / c | Expected |
|---|---|---|---|---|
| 1 | allow a only, then runs a, b, c | 60 / 60 / 60 | 60 / 0 / 0 | only a |
| 2 | add b, then run b | 60 / 120 / 60 | 60 / 60 / 0 | only the new 60 of b |
| 3 | remove a, then run a | 120 / 120 / 60 | 60 / 60 / 0 | nothing more of a |
| 4 | add a again, then run a | 180 / 120 / 60 | 120 / 60 / 0 | only the 60 after the add |

The source offsets of a in the target after step 4 (all on partition 1): 0 to 59 (step 1) and 120 to 179 (step 4).
Offsets 60 to 119 (written while a was removed) are not in the target.

Connector log lines (`confluent connect logs <connector-id> --level "INFO|WARN|ERROR" --search-text "Control topic"`):

```
17:11:53.473Z Control topic: add src.demo-c-b (record time 1795713112870, demo-c-control-0@1)
17:12:42.700Z Control topic: added src.demo-c-b, it starts at its add time 1795713112870
17:12:42.819Z Control topic: moved and resumed 3 partitions: {src.demo-c-b-1=60, ...-0=0, ...-2=0}
17:13:43.522Z Control topic: remove src.demo-c-a (record time 1795713222794, demo-c-control-0@2)
17:14:24.626Z Control topic: removed src.demo-c-a, paused 3 assigned partitions
17:14:24.629Z Control topic: dropped 6 records of topics that are not allowed in the last 1361 s
17:15:33.929Z Control topic: add src.demo-c-a (record time 1795713333224, demo-c-control-0@3)
17:15:42.833Z Control topic: added src.demo-c-a again, it starts at its add time 1795713333224
17:15:42.958Z Control topic: moved and resumed 3 partitions: {src.demo-c-a-0=0, ...-1=120, ...-2=0}
17:17:27.101Z Control topic: add src.demo-c-typo (record time 1795713446507, demo-c-control-0@4)
17:17:42.971Z Control topic: added src.demo-c-typo, it starts at its add time 1795713446507
17:19:42.999Z Control topic lists src.demo-c-typo but no such topic exists for 2 min
```

Step 5 (missing topic): the name `src.demo-c-typo` was added at 17:17:27Z. The WARN line came
at 17:19:42Z, once (level WARN). No other WARN or ERROR line of the connector code was in the log.

The line "dropped 6 records" shows the drop in `put()`: 6 records of a were in the batch that also applied the
remove.

Time to apply a change: the background thread read each control record within 1 second. The task applied it
9 to 49 seconds later. When no records arrive, the framework calls `put()` only when the consumer poll returns,
which can take up to the offset commit interval. Every batch first applies the changes that the background thread
has read. Records written between the remove time and that moment are still forwarded (see "Limits", "Remove
delay").

### Cloud re-test after review fixes (2026-11-26)

This run used commit `6f691b1` (cut ledger, reader health, safe retries). Same environment and cluster as above.

Setup:

- `scripts/build.sh`: 146 tests, 0 failures. Archive `example-fanin-connector-0.1.0.zip` (68 KB), it contains
  `ControlLedger.class`.
- Plugin `demo-p-fanin-plugin-ctl-6f691b1`, the test plugin built from commit `6f691b1`:
  `confluent connect custom-plugin create demo-p-fanin-plugin-ctl-6f691b1 --plugin-file target/components/packages/example-fanin-connector-0.1.0.zip --connector-class com.example.fanin.FanInSinkConnector --connector-type sink --cloud azure --sensitive-properties fanin.target.api.secret`
- A custom connector cannot change its plugin in place, so the connector of the run above was deleted. The topics
  `demo-c-control`, `demo-c-fanin-out` and `src.demo-c-{a,b,c}` were deleted and created
  again (control: 1 partition, `cleanup.policy=compact`; target: 6 partitions; order topics: 3 partitions; data
  topics with `max.message.bytes=6291456`). The subjects `src.demo-c-{a,b,c}-value` were
  deleted (soft, then permanent) with `confluent schema-registry schema delete --subject ... --version all
  [--permanent]` with a user login. The Schema Registry API key of the test-data generator got
  `40301 User is denied operation Delete on Subject`. The generator registered the subjects again.
- The add record for a was written before the connector was created.
- Connector `fanin-demo-c` on this plugin, same config as before (created by the same script, secrets only in a
  mode-600 temporary file) plus
  `fanin.control.max.lag.ms=60000`. Provisioning took 3.5 minutes (18:03:33Z to 18:07:02Z).
- The old plugin `demo-p-fanin-plugin-ctl-e67bced` was deleted at the end. No connector in the cluster used it.

The four steps (the earlier form of `scripts/cloud-test/control/scenario.sh`; each producer run writes 60 records to
partition 1):

| Step | Action | Source end offsets a / b / c | Target records a / b / c | Expected |
|---|---|---|---|---|
| 1 | allow a only, then runs a, b, c | 60 / 60 / 60 | 60 / 0 / 0 | only a |
| 2 | add b, then run b | 60 / 120 / 60 | 60 / 60 / 0 | only the new 60 of b |
| 3 | remove a, then run a | 120 / 120 / 60 | 60 / 60 / 0 | nothing more of a |
| 4 | add a again, then run a | 180 / 120 / 60 | 120 / 60 / 0 | only the 60 after the add |

Source offsets of a in the target after step 4 (all on partition 1): 0 to 59 and 120 to 179. Offsets 60 to 119
(written while a was removed) are not in the target.

**Ledger on Confluent Cloud.** `AdminClient.list_consumer_group_offsets` (confluent_kafka) on the group
`connect-<connector-id>` with the connector's API key worked. No extra ACL was needed. Committed offsets of
partition 1 with their metadata (partitions 0 and 2 are empty, offset 0, same metadata as partition 1):

| After | a-1 | b-1 | c-1 |
|---|---|---|---|
| step 1 | 60 `fanin.ctl:v1:allowed:1795716212308` | 0, no metadata | 0, no metadata |
| step 2 | 60 `fanin.ctl:v1:allowed:1795716212308` | 120 `fanin.ctl:v1:allowed:1795716539763` | 0, no metadata |
| step 3 | 61 `fanin.ctl:v1:removed:1795716650361` | 120 `fanin.ctl:v1:allowed:1795716539763` | 0, no metadata |
| step 4 | 180 `fanin.ctl:v1:allowed:1795716775231` | 120 `fanin.ctl:v1:allowed:1795716539763` | 0, no metadata |

c was never on the list, so it has no ledger. In step 3 one record of a (offset 60) was in the batch that applied
the remove. It was dropped and the commit moved past it (61). Then the partitions of a were paused.

**Restart and repeated add.**

1. `confluent connect cluster pause` and `resume`, then a repeated add of b (control offset 4, record time
   1795717115205), then a producer run of b. Result: target a / b = 120 / 120 (b source end 180). The ledger of b stayed
   `allowed:1795716539763` (the first add time). The 60 new records of b had timestamps after the repeated add,
   so this run alone does not show a skip.
2. Pause, write 10 records to b partition 1 with timestamp 1795712939763 (one hour before the first add of b),
   repeated add of b (control offset 6), resume. Result: target a / b = 120 / 130 (b source end 190): all 10
   records arrived. a did not change.
3. Pause, write 10 more such records, repeated add of b (control offset 7), config update
   `confluent connect cluster update <connector-id> --config fanin.control.missing.warn.ms=120001`, resume. Result:
   target a / b = 120 / 140 (b source end 200). The ledger after it: a-1 180 `allowed:1795716775231`, b-1 200
   `allowed:1795716539763`. The setting was set back to 120000 afterwards (one more task restart).

Pause and resume on Confluent Cloud do not restart the task. The log shows only "Handling connector-only config
update by restarting connector" and "Setting connector ... state to PAUSED", and no new "Read N records of control
topic" line. So runs 1 and 2 tested the running task, not `open()`. A config update made while the connector is
paused is applied only at the resume. In run 3 the log shows at 18:31:19.895Z "target state change", at
18:31:19.950Z "Stopping task `<connector-id>-0`", and at 18:31:21.126Z the new task:

```
Read 8 records of control topic demo-c-control: 3 allowed topics [src.demo-c-typo, src.demo-c-a, src.demo-c-b]
Setting offset for partition src.demo-c-b-1 to the committed offset FetchPosition{offset=200, ...}
Setting offset for partition src.demo-c-a-1 to the committed offset FetchPosition{offset=180, ...}
```

The old task had forwarded the 10 records in the short time between the resume and the stop, so the new task
started at 200. The new task moved no partition (no "start at the add time" line after the restart): the ledger
kept the committed offsets although the control topic holds four adds of b and two adds of a. This run does not
show a restart with records still waiting that have timestamps before a later add. Local test L18 covers that.

**Log lines.** `confluent connect logs <connector-id> --level "INFO|WARN|ERROR" --search-text "Control topic"
--start-time 2026-11-26T18:00:00Z --end-time ...`:

```
18:06:42.682Z Control topic: add src.demo-c-a (record time 1795716212308, demo-c-control-0@0)
18:06:42.683Z Read 1 records of control topic demo-c-control: 1 allowed topics [src.demo-c-a]
18:06:42.685Z Control topic demo-c-control: 1 allowed topics, added topics start at LATEST
18:06:44.466Z Control topic: 3 partitions start at the add time of their topic: {src.demo-c-a-0=0, -1=0, -2=0}
18:09:00.477Z Control topic: add src.demo-c-b (record time 1795716539763, demo-c-control-0@1)
18:09:42.000Z Control topic: added src.demo-c-b, it starts at its add time 1795716539763
18:09:42.265Z Control topic: 3 partitions start at the add time of their topic: {src.demo-c-b-1=60, -0=0, -2=0}
18:09:42.275Z Control topic: moving 3 partitions: {src.demo-c-b-1=60, -0=0, -2=0}
18:10:51.045Z Control topic: remove src.demo-c-a (record time 1795716650361, demo-c-control-0@2)
18:11:31.479Z Control topic: removed src.demo-c-a, pausing 3 assigned partitions
18:11:31.481Z Control topic: dropped 1 records in the last 288 s (topics not allowed, start position not yet applied, or the reader behind)
18:12:55.841Z Control topic: add src.demo-c-a (record time 1795716775231, demo-c-control-0@3)
18:13:42.292Z Control topic: added src.demo-c-a again, it starts at its add time 1795716775231
18:13:42.527Z Control topic: 3 partitions start at the add time of their topic: {src.demo-c-a-0=0, -1=120, -2=0}
18:13:42.528Z Control topic: moving 3 partitions: {src.demo-c-a-0=0, -1=120, -2=0}
18:18:36.020Z Control topic: add src.demo-c-b (record time 1795717115205, demo-c-control-0@4)
18:20:57.592Z Control topic: add src.demo-c-typo (record time 1795717256934, demo-c-control-0@5)
18:21:11.557Z Control topic: added src.demo-c-typo, it starts at its add time 1795717256934
18:23:11.594Z WARN Control topic lists src.demo-c-typo but no such topic exists for 2 min
18:28:34.885Z Control topic: add src.demo-c-b (record time 1795717714303, demo-c-control-0@7)
18:32:20.748Z WARN Control topic lists src.demo-c-typo but no such topic exists for 11 min
```

The task applied each change 14 to 47 seconds after the reader read it.

Missing topic: `src.demo-c-typo` was added at 18:20:57Z with
`fanin.control.missing.warn.ms=120000` (already in the config, no recreate needed). The WARN came at 18:23:11Z.
It came again at 18:32:20Z from the new task after the restart of run 3: the "once" rule holds only for the life
of a task.

All WARN and ERROR lines from 18:03Z to 18:34Z (`--level "ERROR|WARN"`, 17 WARN and 28 ERROR lines):

- From the connector code: only the two missing-topic WARN lines above. There was no line "the reader has not read
  the control topic to its end".
- From the Confluent Cloud worker, at start: "One or more plugins are missing ServiceLoader manifests ...", the
  deprecated `internal.key.converter` warning, "Validation failed for class
  io.confluent.telemetry.provider.KafkaClientProvider ..." and "Ignoring redefinition of existing telemetry label
  connect.version".
- 28 ERROR lines after each of the three resumes, every 15 seconds until the resume took effect:
  `Uncaught exception in REST call to /connector-plugins/com.example.fanin.FanInSinkConnector/config/validate`
  with `java.lang.NoSuchMethodError: 'org.apache.kafka.common.config.Config
  org.apache.kafka.connect.connector.Connector.defaultValidate(java.util.Map)'`. The calls use
  `?skipConnectorValidations=true` and return HTTP 500. The worker reports Kafka version `7.9.2-ce`. The
  `defaultValidate` method does not exist in Apache Kafka `connect-api`, which the plugin compiles against
  (`provided` scope, not in the archive). After 2 to 3 minutes a validate call without
  `skipConnectorValidations` returned 200 and the resume took effect. The connector code is not in the stack.
  Effect: each resume took 2 to 2.5 minutes, and the task status stayed `PAUSED ['STOPPED']` until then. A pause
  took effect within 15 seconds and logged no ERROR.

### Cloud re-test after the second review round

This run used commit `5e38333` (159 tests, 0 failures in `scripts/build.sh`). Same environment and cluster.

Setup:

- Plugin `demo-p-fanin-plugin-ctl-5e38333`, the test plugin built from commit `5e38333` (same `custom-plugin create` flags as above).
- The connector of the run above was deleted. Topics `demo-c-control`, `demo-c-fanin-out`,
  `src.demo-c-{a,b,c}` and the three `-value` subjects deleted and created again as in the
  run above. The control topic got `segment.ms=600000` and `min.compaction.lag.ms=0` in addition, so that
  compaction could be tested (see below). `min.cleanable.dirty.ratio` was not accepted and stayed 0.5.
- Connector `fanin-demo-c` on this plugin: same config as the run above plus
  `consumer.override.fetch.min.bytes=52428800` and `consumer.override.fetch.max.wait.ms=3000`. With these settings
  a fetch waits up to 3 seconds, so a task that runs only a few milliseconds before it is stopped cannot fetch
  records (in the run above the old task forwarded the waiting records in the 55 ms between the resume and its
  stop). The task log shows `fetch.min.bytes = 52428800` and `fetch.max.wait.ms = 3000` for
  `connector-consumer-<connector-id>-0`.
- The old plugin `demo-p-fanin-plugin-ctl-6f691b1` was deleted at the end. No connector in the cluster used it.

The four steps, with one change: after step 2 the add of b was written again (control offset 2, record time
1795721271310), after the last run of b.

| Step | Action | Source end offsets a / b / c | Target records a / b / c | Expected |
|---|---|---|---|---|
| 1 | allow a only, then runs a, b, c | 60 / 60 / 60 | 60 / 0 / 0 | only a |
| 2 | add b, run b, then add b again | 60 / 120 / 60 | 60 / 60 / 0 | only the new 60 of b |
| 3 | remove a, then run a | 120 / 120 / 60 | 60 / 60 / 0 | nothing more of a |
| 4 | add a again, then run a | 180 / 120 / 60 | 120 / 60 / 0 | only the 60 after the add |

Source offsets of a in the target after step 4 (partition 1): 0 to 59 and 120 to 179.

Committed offsets of partition 1 with metadata (`ledger.py`, AdminClient `list_consumer_group_offsets` on
`connect-<connector-id>`):

| After | a-1 | b-1 | c-1 |
|---|---|---|---|
| step 1 | 60 `fanin.ctl:v1:allowed:1795720868009` | 0, no metadata | 0, no metadata |
| step 2 | 60 `fanin.ctl:v1:allowed:1795720868009` | 120 `fanin.ctl:v1:allowed:1795721160877` | 0, no metadata |
| step 3 | 60 `fanin.ctl:v1:removed:1795721272161` | 120 `fanin.ctl:v1:allowed:1795721160877` | 0, no metadata |
| step 4 | 180 `fanin.ctl:v1:allowed:1795721382479` | 120 `fanin.ctl:v1:allowed:1795721160877` | 0, no metadata |

The repeated add of b did not change the ledger of b. In step 3 the whole run of a (60 records) came in the batch
that applied the remove: `dropped 60 records in the last 290 s`. The commit of a-1 stayed at 60.

**Real task restart with records waiting.**

1. 19:58:22Z `confluent connect cluster pause <connector-id>`. Paused at 19:58:40Z.
2. 19:58:40Z: 10 records written to `src.demo-c-b` partition 1 with timestamp
   1795717560877 (one hour before the first add of b). Source end of b: 130. Committed b-1: 120
   `fanin.ctl:v1:allowed:1795721160877`.
3. 19:58:43Z `confluent connect cluster update <connector-id> --config fanin.control.missing.warn.ms=120001`.
4. 19:59:15Z `confluent connect cluster resume <connector-id>`. RUNNING at 20:01:29Z.

Log of the restart:

```
20:01:27.269Z Connector <connector-id> target state change
20:01:27.366Z Tasks [<connector-id>-0] configs updated
20:01:27.369Z Stopping task <connector-id>-0
20:01:30.373Z Starting task <connector-id>-0
20:01:30.964Z Read 7 records of control topic demo-c-control: 2 allowed topics [src.demo-c-a, src.demo-c-b]
20:01:31.364Z Setting offset for partition src.demo-c-b-1 to the committed offset FetchPosition{offset=120, ...}
20:01:31.364Z Setting offset for partition src.demo-c-a-1 to the committed offset FetchPosition{offset=180, ...}
```

The new task started b-1 at 120, so the 10 waiting records (offsets 120 to 129) were read by the new task. It
moved no partition. Result at 20:03:00Z: target a / b / c = 120 / 70 / 0, all 10 records arrived, a unchanged.
Committed b-1: 130 `fanin.ctl:v1:allowed:1795721160877`. The setting was set back to 120000 afterwards.

What this run does not show: the control topic was not compacted. A dump at 20:03Z still showed all 7 records,
also the first add of b at offset 1. Segment 1 started at 19:21:08Z, records were written at 19:31:49Z and
19:58:22Z (after `segment.ms`), and nothing was removed up to 20:03Z. So the task read the first add time of b
(1795721160877), and the add-time rule alone would also have kept the committed offset: `ListOffsets` at that
time gives offset 60 on b-1, below the committed 120. The case where only the ledger keeps the records (the first
add compacted away, the repeated add 1795721271310 later than all records of b-1) needs compaction, which the
broker did not do in 42 minutes. Local test L18 covers it.

**Missing topic.** `src.demo-c-typo` added at 19:31:49Z, WARN at 19:34:43Z
(`Control topic lists src.demo-c-typo but no such topic exists for 2 min`). Removed again at
19:58:22Z.

**WARN and ERROR lines** (`confluent connect logs <connector-id> --level "ERROR|WARN"`, 19:21Z to 20:05Z: 18 WARN, 12
ERROR):

- From the connector code: only the missing-topic WARN above. No "held commits" WARN (`Control topic: no offsets
  are committed for this task ...` or `Control topic: the start position of ... is not decided ...`), and no
  `Control topic: the reader has not read the control topic to its end` WARN.
- From the worker: the start-up WARN lines listed in the run above, and at 19:49:00Z two producer WARN lines for
  the platform topic `<connector-id>-app-logs-0` (`NOT_LEADER_OR_FOLLOWER`, retried).
- 12 ERROR lines `Uncaught exception in REST call to /connector-plugins/com.example.fanin.FanInSinkConnector/config/validate`
  with `java.lang.NoSuchMethodError: ... Connector.defaultValidate(java.util.Map)`, as in the run above: 10 between
  the resume and 20:01:27Z, and 2 during the config update at 20:03Z to 20:04Z that set the value back.

Control topic lines of the run:

```
19:23:43.547Z Read 1 records of control topic demo-c-control: 1 allowed topics [src.demo-c-a]
19:23:45.072Z Control topic: 3 partitions start at the add time of their topic: {src.demo-c-a-0=0, -1=0, -2=0}
19:26:01.482Z Control topic: add src.demo-c-b (record time 1795721160877, demo-c-control-0@1)
19:26:42.909Z Control topic: added src.demo-c-b, it starts at its add time 1795721160877
19:26:43.134Z Control topic: moving 3 partitions: {src.demo-c-b-1=60, -0=0, -2=0}
19:27:51.962Z Control topic: add src.demo-c-b (record time 1795721271310, demo-c-control-0@2)
19:27:52.763Z Control topic: remove src.demo-c-a (record time 1795721272161, demo-c-control-0@3)
19:28:33.692Z Control topic: removed src.demo-c-a, pausing 3 assigned partitions
19:28:33.695Z Control topic: dropped 60 records in the last 290 s (topics not allowed, start position not yet applied, or the reader behind)
19:29:43.063Z Control topic: add src.demo-c-a (record time 1795721382479, demo-c-control-0@4)
19:29:43.151Z Control topic: added src.demo-c-a again, it starts at its add time 1795721382479
19:29:43.343Z Control topic: moving 3 partitions: {src.demo-c-a-0=0, -1=120, -2=0}
19:31:49.613Z Control topic: add src.demo-c-typo (record time 1795721509007, demo-c-control-0@5)
19:34:43.404Z WARN Control topic lists src.demo-c-typo but no such topic exists for 2 min
```

### Rebalance behaviour (test prefix `demo-r`, 2026-11-27)

Question: what happens to the forwarding of an allowed topic when topics that match `topics.regex` but are not on
the control list are created or deleted? Run on Confluent Cloud, 06:11Z to 06:38Z, same Basic test cluster,
plugin `demo-p-fanin-plugin-ctl-5e38333` (commit `5e38333`, not the time-based remove of later commits).

Setup:

- Topics: `demo-r-control` (1 partition, `cleanup.policy=compact`), `demo-r-fanin-out` (1 partition,
  `message.timestamp.type=LogAppendTime`, so that the target timestamp is the time the forwarded record was
  written), `src.demo-r-allowed` (1 partition). No other connector's `topics.regex` matches
  `demo-r-`.
- Control record: add `src.demo-r-allowed` at 06:11:50Z.
- Connector `fanin-demo-r`, `tasks.max=1`, `topics.regex=^src\.demo-r-.+$`,
  `fanin.control.topic=demo-r-control`, `fanin.timestamp.mode=COPY`, no consumer overrides. RUNNING at 06:15:00Z.
- The task consumer (`connector-consumer-<connector-id>-0`, from the `ConsumerConfig values` log line):
  `partition.assignment.strategy = [RoundRobinAssignor, RangeAssignor, CooperativeStickyAssignor]`,
  `group.protocol = classic`, `metadata.max.age.ms = 300000`, `session.timeout.ms = 45000`,
  `max.poll.interval.ms = 300000`.
- Assignor in use: AdminClient `describe_consumer_groups` on `connect-<connector-id>` gave
  `state STABLE, type CLASSIC, assignor 'roundrobin', members 1`. The join lines show `protocol='roundrobin'`. So
  the assignment is eager: every rebalance revokes all partitions, also the allowed one.
- A producer wrote 200 records/s into the allowed topic, value `{"seq": n, "ts": <produce ms>}`. A reader of
  `demo-r-fanin-out` (assign, no commits) stored seq, produce time and target append time of each record. Gap =
  the longest time between two forwarded records. Latency = target append time minus produce time.

Scripts: `mkconn.py` (creates the connector, secrets only in a mode-600 temp file), `prod.py`, `mon.py`, `ana.py`
(gap and latency per window), now in `scripts/cloud-test/control/`; the step scripts of this run (`step.sh`,
`step4.sh`, `grp.py`) are not kept. Topics were created and deleted with `confluent kafka topic create <name> --partitions 1` and
`confluent kafka topic delete <name> --force`, 10 calls in parallel for the 200 topics.

Results:

| Step | Action | Rebalances | Time to rebalance | Gap of the allowed topic | Max latency | Records lost / twice |
|---|---|---|---|---|---|---|
| 1 | baseline 3 min (06:15:05Z to 06:18:05Z, 35999 records) | 0 | - | 373 ms | 496 ms (p50 121 ms) | 0 / 0 |
| 2 | create 1 non-listed topic `...demo-r-nl1...`, 5 records | 1 | under 1 s after the create returned | 310 ms | 425 ms | 0 / 0 |
| 3 | delete that topic | 2 | 3 min 27 s after the delete | 60513 ms | 60644 ms | 0 / 0 |
| 4a | create 200 non-listed topics (06:25:06Z to 06:26:24Z) | 2 | 1st after 12 s (40 topics), 2nd after 3 min 28 s (160 topics) | 1052 ms, 2349 ms | 1196 ms, 2486 ms | 0 / 0 |
| 4b | delete the 200 topics (06:30:01Z to 06:31:14Z) | 1 | 2 min 13 s after the first delete, 60 s after the last | 60506 ms | 60638 ms | 0 / 0 |

Over the whole run up to the control check (06:15:05Z to 06:35:39Z, 246800 records, seq 34238 to 281037): 0
missing, 0 twice. Gaps over 1 s: 60513 ms (step 3), 1052 ms and 2349 ms (step 4a), 2034 ms at 06:29:49Z, 60506 ms
(step 4b); sum 126454 ms. The 2034 ms gap at 06:29:49Z has no rebalance line in the log; its cause is not known.

**Step 2, create.** The task did not wait for `metadata.max.age.ms`. Log:

```
06:18:13.255Z Request joining group due to: cached metadata has changed from (version3: {src.demo-r-allowed=[NO_RACKS]}) at the beginning of the rebalance to (version5: {src.demo-r-allowed=[NO_RACKS], src.demo-r-nl1=[NO_RACKS]})
06:18:13.263Z Revoke previously assigned partitions src.demo-r-allowed-0
06:18:13.273Z Successfully joined group with generation Generation{generationId=2, ..., protocol='roundrobin'}
06:18:13.292Z Adding newly assigned partitions: src.demo-r-allowed-0, src.demo-r-nl1-0
06:18:13.295Z Found no committed offset for partition src.demo-r-nl1-0
06:18:13.301Z Setting offset for partition src.demo-r-allowed-0 to the committed offset FetchPosition{offset=71864, ...}
```

Revoke to assign took 29 ms. The new topic stayed paused: 0 of its 5 records reached the target, and its committed
offset was `src.demo-r-nl1-0 offset=0 metadata=None` (`ledger.py`). `open()` wrote no
`Control topic` line for this rebalance (no partition of an allowed topic needed a move), and no line with lookup
timing exists in this build.

**Step 3, delete.** The deleted partition stays assigned until the consumer refreshes its metadata. In that time
every offset commit fails for the deleted partition. The commit of the allowed partition in the same request went
through: after the stop its committed offset was 131864, the offset in the last failed commit.

```
06:20:43.088Z WARN Offset commit failed on partition src.demo-r-nl1-0 at offset 0: This server does not host this topic-partition.
06:20:43.100Z ERROR WorkerSinkTask{id=<connector-id>-0} Commit of offsets threw an unexpected exception for sequence number 7: {src.demo-r-allowed-0=OffsetAndMetadata{offset=101831, leaderEpoch=null, metadata='fanin.ctl:v1:allowed:1795759910338'}, src.demo-r-nl1-0=OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''}}
  org.apache.kafka.clients.consumer.RetriableCommitFailedException: Offset commit failed with a retriable exception. You should retry committing the latest consumed offsets.
  Caused by: org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.
```

The same pair came at 06:21:43Z and 06:22:43Z. Then the rebalance:

```
06:23:13.269Z Request joining group due to: cached metadata has changed from (version6: {...allowed...=[NO_RACKS], src.demo-r-nl1=[NO_RACKS]}) at the beginning of the rebalance to (version7: {...allowed...=[NO_RACKS], src.demo-r-nl1=[]})
06:23:13.275Z Revoke previously assigned partitions src.demo-r-allowed-0, src.demo-r-nl1-0
06:23:13.277Z The pause flag in partitions [src.demo-r-nl1-0] will be removed due to revocation.
06:23:13.285Z WARN Offset commit failed on partition src.demo-r-nl1-0 at offset 0: This server does not host this topic-partition.
   ... the same WARN about once per second ...
06:24:13.286Z ERROR WorkerSinkTask{id=<connector-id>-0} Commit of offsets threw an unexpected exception for sequence number 10: {...allowed...-0=OffsetAndMetadata{offset=131864, ...}, ...nl1...-0=OffsetAndMetadata{offset=0, ...}}
  org.apache.kafka.common.errors.TimeoutException: Timeout of 60000ms expired before successfully committing offsets
06:24:13.299Z Successfully joined group with generation Generation{generationId=3, ...}
06:24:13.351Z Request joining group due to: cached metadata has changed from (version8: {...allowed...=[NO_RACKS], src.demo-r-nl1=[]}) ... to (version9: {...allowed...=[NO_RACKS]})
06:24:13.361Z Adding newly assigned partitions: src.demo-r-allowed-0
06:24:13.600Z Revoke previously assigned partitions src.demo-r-allowed-0
06:24:13.615Z Successfully joined group with generation Generation{generationId=4, ...}
06:24:13.633Z Adding newly assigned partitions: src.demo-r-allowed-0
06:24:13.637Z Setting offset for partition src.demo-r-allowed-0 to the committed offset FetchPosition{offset=131864, ...}
```

The worker commits the revoked partitions synchronously in the revoke callback. The commit of the deleted partition
retries until the 60 s timeout (`default.api.timeout.ms`), and the task forwards nothing in that time. The last
record before the stop was produced at 06:23:13.129Z, the next one was written to the target at 06:24:13.778Z.
Nothing was lost or delivered twice. Committed offset of the allowed partition after the stop: 131864
`fanin.ctl:v1:allowed:1795759910338`.

**Step 4a, 200 creates.** No partition count problem: 508 partitions were in use before the run, the 200 topics had
1 partition each. One create (`m131`) failed in the parallel run and was repeated at 06:26:24Z. The consumer saw
the new topics in two rebalances: at 06:25:18.720Z with the first 40 topics (revoke 06:25:18.730Z, assign
06:25:18.745Z), and at 06:28:46.876Z with the other 160 after the metadata refresh (revoke 06:28:46.906Z, assign
06:28:47.055Z, allowed partition position set at 06:28:47.267Z). The rebalance itself took 15 ms and 149 ms; the
forwarding gaps were 1052 ms and 2349 ms. Each of the 200 partitions got `Found no committed offset for partition
...`. No `Control topic` line, no WARN or ERROR in the windows 06:25:02Z to 06:26:53Z and 06:28:42Z to 06:29:33Z.

**Step 4b, 200 deletes.** As in step 3, but with one rebalance: commits failed from 06:30:13Z (68 WARN `Offset
commit failed on partition src.demo-r-mNNN-0 at offset 0: This server does not host this
topic-partition.` and 4 ERROR `Commit of offsets threw an unexpected exception for sequence number N: {...}` from
06:30:13Z to 06:33:14Z). Rebalance:

```
06:32:14.789Z Request joining group due to: cached metadata has changed from (version13: {...}) ...
06:32:14.801Z Revoke previously assigned partitions src.demo-r-allowed-0, ...
06:33:14.963Z Successfully joined group with generation Generation{generationId=7, ...}
06:33:14.983Z Adding newly assigned partitions: src.demo-r-allowed-0
06:33:14.985Z Setting offset for partition src.demo-r-allowed-0 to the committed offset FetchPosition{offset=240166, ...}
```

Again 60 s without forwarding (gap 60506 ms). The stop did not get longer with 200 deleted partitions than with one.

**Control list changes cause no rebalance.** Remove of the allowed topic at 06:35:39Z, add again at 06:36:05Z:

```
06:35:40.140Z Control topic: remove src.demo-r-allowed (record time 1795761339234, demo-r-control-0@1)
06:35:40.143Z Control topic: removed src.demo-r-allowed, pausing 1 assigned partitions
06:35:40.154Z Control topic: dropped 1 records in the last 1256 s (topics not allowed, start position not yet applied, or the reader behind)
06:36:05.974Z Control topic: add src.demo-r-allowed (record time 1795761365213, demo-r-control-0@2)
06:36:14.986Z Control topic: added src.demo-r-allowed again, it starts at its add time 1795761365213
06:36:15.111Z Control topic: 1 partitions start at the add time of their topic: {src.demo-r-allowed-0=286280}
06:36:15.116Z Control topic: moving 1 partitions: {src.demo-r-allowed-0=286280}
```

No `Request joining group`, `Revoke` or `Adding newly assigned` line from 06:35:00Z to 06:37:00Z, and the group
stayed at generation 7. The records produced while the topic was removed (seq 281038 to 286279, less the ones
forwarded before 06:35:40.14Z) were not forwarded, as designed. Forwarding started again at seq 286280 (produced
06:36:05.217Z, the add time) at 06:36:15.138Z, about 10 s after the add.

After the control check the committed offset of the allowed partition was `offset=281236
metadata='fanin.ctl:v1:removed:1795759910338'` (read about 06:37:10Z). The time in this ledger is the first add time
(06:11:50Z), not the remove time 1795761339234, and the offset is at the remove, not at the re-add cut 286280.
This was not looked into further in this run.

**Cooperative assignor.** `consumer.override.partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor`
was refused when the connector was created again:

```
Error: failed to create custom connector: failed to validateCustomConnectorConfigs: failed to validate connector configs: Unsupported use of override connector config key
```

So a custom connector on Confluent Cloud cannot choose the assignor, and steps 2 and 3 could not be repeated with a
cooperative assignment. The task uses `roundrobin` (eager).

**WARN and ERROR lines.** From the connector code: none in the saved windows. No held-commits WARN, no
`the reader has not read the control topic to its end` WARN. All WARN and ERROR lines came from the Kafka consumer
and `WorkerSinkTask` (the commit failures above). The connector logs can no longer be read after the connector is
deleted, so only the windows saved during the run were checked: 06:18:02Z to 06:19:13Z, 06:19:42Z to 06:24:33Z,
06:25:02Z to 06:26:53Z, 06:28:42Z to 06:29:33Z, 06:25:00Z to 06:34:30Z (WARN and ERROR only), 06:35:00Z to 06:37:00Z.

**What this means.**

- A new matching topic costs one eager rebalance of the task, with a forwarding gap of a few hundred ms to about
  2 s. The topic stays paused and nothing of it is forwarded.
- A deleted matching topic that the task still has assigned stops all forwarding of the task for 60 s, plus up to
  `metadata.max.age.ms` (5 min) of failing commits before that. This comes from the worker (sync commit of revoked
  partitions in the revoke callback), not from the control topic code. It happens for every delete of an assigned
  topic, listed or not. A bulk delete gave one 60 s stop, not one per topic.
- Changes of the control list do not rebalance.

Cleanup: the connector was deleted at 06:37:17Z; topics `src.demo-r-allowed`,
`demo-r-control`, `demo-r-fanin-out` deleted at 06:37:39Z; `nl1` and the 200 `m` topics were deleted in steps 3
and 4b. The consumer group `connect-<connector-id>` was not deleted. The other connectors and Flink jobs in the
cluster were RUNNING before and after the run.

### Cloud test of the time-based remove (2026-11-27)

This run used commit `8b46a87` (172 tests, 0 failures in `scripts/build.sh`). Same environment and cluster.

Setup:

- Plugin `demo-p-fanin-plugin-ctl-8b46a87`, the test plugin built from commit `8b46a87` (same `custom-plugin create` flags as above).
- The connector of the second re-test was deleted. Topics `demo-c-control` (1 partition, compacted, default segment settings),
  `demo-c-fanin-out`, `src.demo-c-{a,b,c}` and the three `-value` subjects deleted and
  created again.
- Connector `fanin-demo-c` on this plugin, same config as in the first cloud re-test
  (with `fanin.control.max.lag.ms=60000`, without the fetch overrides of the second re-test).
- The old plugin `demo-p-fanin-plugin-ctl-5e38333` is kept: `fanin-demo-r` uses it.
- The other connectors in the cluster were RUNNING before (06:31Z) and after (06:52Z) the test.

**Regression (the four steps).**

| Step | Action | Source end offsets a / b / c | Target records a / b / c | Expected |
|---|---|---|---|---|
| 1 | allow a only, then runs a, b, c | 60 / 60 / 60 | 60 / 0 / 0 | only a |
| 2 | add b, then run b | 60 / 120 / 60 | 60 / 60 / 0 | only the new 60 of b |
| 3 | remove a, then run a | 120 / 120 / 60 | 60 / 60 / 0 | nothing more of a |
| 4 | add a again, then run a | 180 / 120 / 60 | 120 / 60 / 0 | only the 60 after the add |

a in the target after step 4 (partition 1): offsets 0 to 59 and 120 to 179. Committed ledger of a-1: after step 1
60 `fanin.ctl:v1:allowed:1795761165085`, after step 3 60 `fanin.ctl:v1:removed:1795761641574:60`, after step 4
180 `fanin.ctl:v1:allowed:1795761754525`. b-1 after step 3: 120 `fanin.ctl:v1:allowed:1795761531102`. c has no
ledger. In step 3 the remove cut of a-1 is 60, its end offset at the remove: the run of a started 30 seconds
after the remove, so all its records are at or after the cut.

**Remove with records waiting in the lag (as LocalIT L19).** Commands (now `scripts/cloud-test/control/remove.sh`, records
written with a small `confluent_kafka` producer to partition 1, timestamp = write time):

1. 06:44:33Z `confluent connect cluster pause <connector-id>`. Committed a-1: 180 `allowed:1795761754525`.
2. 06:44:51Z: 10 records to a-1, offsets 180 to 189, timestamp 1795761891194.
3. 06:44:53Z: remove of a (control offset 4, record time 1795761893979).
4. 06:44:56Z: 10 records to a-1, offsets 190 to 199, timestamp 1795761896770.
5. 06:45:01Z `confluent connect cluster resume <connector-id>`.

Log order (worker and task lines):

```
06:44:36.182Z Setting connector <connector-id> state to PAUSED
06:44:54.671Z Control topic: remove src.demo-c-a (record time 1795761893979, demo-c-control-0@4)
06:45:36.193Z Control topic: removed src.demo-c-a at 1795761893979; its 3 assigned partitions forward their records written before that time and are then paused
06:45:36.233Z Control topic: 3 partitions of removed topics forward their records below the remove cut (the first offset at or after the remove time) and are then paused: {src.demo-c-a-0=0, src.demo-c-a-1=190, src.demo-c-a-2=0}
06:47:09.021Z Setting connector <connector-id> state to STARTED
06:47:09.285Z Control topic: dropped 10 records in the last 622 s (topics not allowed, start position not yet applied, or the reader behind)
```

The task applied the remove (cut 190 on a-1) at 06:45:36Z, while the connector was still paused: a paused sink task
still calls `put()` with empty batches. The connector consumed nothing until it started again at 06:47:09Z, and the
committed offset of a-1 was 180 before the test, so offsets 180 to 199 were waiting in the lag when the cut was
applied. The first batch after the start forwarded 180 to 189 and dropped 190 to 199.

Result at 06:48:45Z: target a / b / c = 130 / 60 / 0. a in the target (partition 1): 0 to 59, 120 to 189. Committed
a-1: 190 `fanin.ctl:v1:removed:1795761893979:190`.

**Re-add.** 06:48:56Z add of a (control offset 5, record time 1795762136412), 06:49:42Z 10 records to a-1 (offsets
200 to 209). Log: `added src.demo-c-a again, it starts at its add time 1795762136412` and
`moving 3 partitions: {src.demo-c-a-0=0, -1=200, -2=0}` at 06:49:36Z. Result at 06:50:58Z: target a / b / c
= 140 / 60 / 0, a in the target: 0 to 59, 120 to 189, 200 to 209. Offsets 190 to 199 (written after the remove) are
not in the target. Committed a-1: 210 `fanin.ctl:v1:allowed:1795762136412`.

**WARN and ERROR lines** (`confluent connect logs <connector-id> --level "ERROR|WARN"`, 06:32Z to 06:52Z: 14 WARN, 9
ERROR):

- From the connector code: none. No missing-topic WARN (no typo name in this run), no "held commits" WARN, no
  reader-lag WARN.
- From the worker: the start-up WARN lines listed in the first cloud re-test (ServiceLoader manifests, deprecated
  `internal.key.converter`, telemetry `KafkaClientProvider` validation, `Ignoring redefinition of existing
  telemetry label connect.version`).
- 9 ERROR lines from 06:45Z to 06:47Z, during the resume: `Uncaught exception in REST call to
  /connector-plugins/com.example.fanin.FanInSinkConnector/config/validate` with `java.lang.NoSuchMethodError: ...
  Connector.defaultValidate(java.util.Map)`, as in the earlier runs.

### Topic delete: 60 s stop of the task (test prefix `demo-r2`, 2026-11-27)

Result: a deleted topic that matches the regex stops all forwarding of the task for about 60 s. This is Kafka Connect
behaviour, not caused by the control topic. Scripts: `scripts/cloud-test/control/` (`gap-setup.sh`, `gap-run.sh`,
`prod.py`, `mon.py`, `ana.py`), see its `README.md`.

Measured with plugin `demo-p-fanin-plugin-ctl-5e38333` (commit `5e38333`), eager roundrobin assignor, 200 records/s into one allowed topic:
- (a) feature off, deleted topic read: gap 60,398 ms, rebalance under 0.1 s after the delete.
- (b) feature on, deleted topic allowed: gap 60,528 ms, rebalance under 0.1 s.
- (c) feature on, deleted topic paused: gap 60,550 ms, rebalance 2 min 38 s after the delete (next metadata refresh).
- (d) feature off, deleted topic without records: gap 60,268 ms.
- (e) feature on, paused, `consumer.override.default.api.timeout.ms=10000`: gap 10,296 ms.
- In every variant: 276,000 records, 0 missing, 0 duplicates.

Mechanism (Kafka 4.2.0 source): the delete starts an eager rebalance. In the revoke callback the worker commits
synchronously. The commit map is built from `lastCommittedOffsets` of all revoked partitions
(`WorkerSinkTask.java:472`), so it always contains the deleted partition. The broker answers
`UNKNOWN_TOPIC_OR_PARTITION`, which the consumer retries until `default.api.timeout.ms` (60 s) runs out. The pause of
a non-listed topic only delays the detection; until the rebalance, the minute commits fail with ERROR lines but do
not block forwarding.

Mitigation: `consumer.override.default.api.timeout.ms` is accepted on Confluent Cloud and bounds the stop. The same
timeout also bounds `position()` in `onPartitionsAssigned` and the commit on task stop, so do not set it below
`request.timeout.ms` (20,000 on Confluent Cloud). Only 10,000 was tested. Refused on Confluent Cloud with
"Unsupported use of override connector config key": `partition.assignment.strategy` (cooperative assignor) and
`metadata.max.age.ms`.
