package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/** A cut is applied once: lookup retries, rebalances, restarts, compaction and recreated topics. */
class ControlCutStabilityTest {

    private static final String CTL = "ctl";
    private static final String A = "in.a";
    private static final TopicPartition A0 = new TopicPartition(A, 0);

    private static Map<String, String> props(String... extra) {
        Map<String, String> p = new HashMap<>();
        p.put("name", "fanin");
        p.put("topics.regex", "^in\\..+");
        p.put(FanInSinkConfig.TARGET_TOPIC, "fanin-out");
        p.put(FanInSinkConfig.TARGET_BOOTSTRAP_SERVERS, "localhost:9092");
        p.put(FanInSinkConfig.TARGET_API_KEY, "key");
        p.put(FanInSinkConfig.TARGET_API_SECRET, "secret");
        // These tests were written for LATEST, the default before EARLIEST. A test of EARLIEST overrides it.
        p.put(FanInSinkConfig.CONTROL_ADD_START, "LATEST");
        p.put(FanInSinkConfig.CONTROL_TOPIC, CTL);
        for (int i = 0; i < extra.length; i += 2) {
            p.put(extra[i], extra[i + 1]);
        }
        return p;
    }

    private static Object[] add(String t, long time) {
        return new Object[] {t, "1", time};
    }

    private static FanInSinkTaskControlTest.TestTask started(FanInSinkTaskControlTest.Ctx ctx,
                                                            FanInSinkTaskControlTest.TestTask task,
                                                            Object[][] control, String... extra) {
        task.initialControl(control);
        task.initialize(ctx.context());
        task.start(props(extra));
        return task;
    }

    private static void assign(FanInSinkTaskControlTest.TestTask task, FanInSinkTaskControlTest.Ctx ctx,
                               TopicPartition... tps) {
        ctx.assignment.addAll(List.of(tps));
        task.open(List.of(tps));
    }

    private static void rebalance(FanInSinkTaskControlTest.TestTask task, FanInSinkTaskControlTest.Ctx ctx,
                                  TopicPartition... tps) {
        task.close(List.of(tps));
        ctx.paused.removeAll(List.of(tps));
        task.open(List.of(tps));
    }

    private static SinkRecord rec(TopicPartition tp, long offset) {
        return new SinkRecord(tp.topic(), tp.partition(), Schema.OPTIONAL_BYTES_SCHEMA, null,
            Schema.OPTIONAL_BYTES_SCHEMA, ("v" + offset).getBytes(StandardCharsets.UTF_8), offset, 5L,
            TimestampType.CREATE_TIME);
    }

    private static List<Long> forwardedOffsets(FanInSinkTaskControlTest.TestTask task) {
        List<Long> out = new ArrayList<>();
        task.producer.history().forEach(r -> out.add(Long.parseLong(
            new String(r.headers().lastHeader(FanInSinkTask.HEADER_OFFSET).value(), StandardCharsets.UTF_8))));
        return out;
    }

    /**
     * Remove + re-add drained in one put(), the add-time lookup fails once. The records in the current batch
     * were written while the topic was removed. They must not be forwarded.
     */
    @Test
    void readdInOneInterval_lookupFails_mustNotForwardRecordsWrittenWhileRemoved() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.atTime.put(A0, 3L);
        assign(task, ctx, A0);

        task.controlState().apply(A, false, 200);
        task.controlState().apply(A, true, 300);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("admin slow");
        // offsets 6 and 7 were written between the remove (200) and the add (300)
        task.put(List.of(rec(A0, 6), rec(A0, 7)));

        assertEquals(List.of(), forwardedOffsets(task), "records written while removed were forwarded");
    }

    /**
     * A rebalance (or task start) where the add-time lookup fails once. The retry in put() uses
     * onlyIfBehind=false and seeks the partition BACK from its committed offset 1000 to the add-time offset 10.
     */
    @Test
    void openLookupFails_retryMustNotRewindBehindCommitted() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 1000L);
        task.lookup.atTime.put(A0, 10L);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("admin slow");

        assign(task, ctx, A0); // task start, LATEST: A has a cut at its add time 100

        task.lookup.failure = null;
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());

        Long seek = ctx.seekedTo().get(A0);
        assertTrue(seek == null || seek >= 1000L, "partition was rewound from committed 1000 to " + seek);
    }

    /**
     * Source records carry event-time timestamps that are older than the add time (or the source producer's
     * clock is behind the control producer's clock). After the add, records 40..99 are written while A is allowed.
     * The task forwarded and committed up to 60. A rebalance (or a restart) recomputes the cut: no record has a
     * timestamp at or after the add time, so the cut is the end offset 100 and offsets 60..99 are skipped.
     */
    @Test
    void rebalanceAfterAdd_recordsWithOlderTimestamps_mustNotSkipCommittedLag() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {});
        assign(task, ctx, A0);
        task.lookup.ends.put(A0, 40L);
        task.controlState().apply(A, true, 500);
        task.put(List.of());
        assertEquals(Map.of(A0, 40L), ctx.seekedTo());

        task.put(List.of(rec(A0, 40), rec(A0, 59)));
        // the framework commits what preCommit returns
        task.lookup.commit(task.preCommit(Map.of(A0, new OffsetAndMetadata(60L))));
        task.lookup.ends.put(A0, 100L);     // 40 more records written while allowed, timestamps < 500
        ctx.seeks.clear();

        rebalance(task, ctx, A0);

        assertTrue(ctx.seeks.isEmpty(), "allowed records 60..99 skipped by seek " + ctx.seekedTo());
    }

    /** Same as the test above, on a plain task restart (LATEST puts every allowed topic into cutAt at start). */
    @Test
    void restart_recordsWithOlderTimestamps_mustNotSkipCommittedLag() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 500)});
        task.lookup.committed.put(A0, 60L);
        // the earlier run applied the add at 500 and committed offset 60 with its ledger
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(500).encode());
        task.lookup.ends.put(A0, 100L); // no record with timestamp >= 500

        assign(task, ctx, A0);

        assertTrue(ctx.seeks.isEmpty(), "restart skipped allowed records 60..99: " + ctx.seekedTo());
    }

    /**
     * Two tasks of one connector. Task 1 started before compaction and read add@100 and the repeated add@900
     * (the first add wins). Task 2 started after compaction and sees only add@900. Same partition, committed at 5,
     * first record at/after 100 is offset 3, first at/after 900 is offset 50. Task 2 skips offsets 5..49 that were
     * written while A was allowed.
     */
    @Test
    void repeatedAddAfterCompaction_mustGiveTheSameCut() {
        FanInSinkTaskControlTest.Ctx ctx1 = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask t1 = started(ctx1, timeLookupTask(), new Object[][] {add(A, 100),
            add(A, 900)});
        t1.lookup.committed.put(A0, 5L);
        assign(t1, ctx1, A0);

        FanInSinkTaskControlTest.Ctx ctx2 = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask t2 = started(ctx2, timeLookupTask(), new Object[][] {add(A, 900)});
        t2.lookup.committed.put(A0, 5L);
        assign(t2, ctx2, A0);

        assertEquals(ctx1.seekedTo(), ctx2.seekedTo(), "same control list, different cut");
    }

    private static FanInSinkTaskControlTest.TestTask timeLookupTask() {
        return new FanInSinkTaskControlTest.TestTask() {
            final FanInSinkTaskControlTest.Lookup l = new FanInSinkTaskControlTest.Lookup() {
                @Override
                public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> p, long ts) {
                    Map<TopicPartition, Long> r = new HashMap<>();
                    p.forEach(tp -> r.put(tp, ts <= 100 ? 3L : 50L));
                    return r;
                }

                @Override
                public Map<TopicPartition, Long> committed(String g) {
                    return Map.of(A0, 5L);
                }

                @Override
                public Map<TopicPartition, OffsetAndMetadata> committedWithMetadata(String g) {
                    // offset 5 was committed by a task that had applied the add at 100
                    return Map.of(A0, new OffsetAndMetadata(5L, ControlLedger.allowedAt(100).encode()));
                }
            };

            @Override
            StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
                return l;
            }
        };
    }

    /**
     * Topic A is re-added (cut at 60, dropBelow A0=60), then deleted and recreated with the same name before
     * the partition is revoked. New records start at offset 0 and are dropped as "below the cut".
     */
    @Test
    void recreatedTopic_staleDropBelow_dropsAllowedRecords() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.atTime.put(A0, 3L);
        assign(task, ctx, A0);
        task.controlState().apply(A, false, 200);
        task.put(List.of());
        task.lookup.atTime.put(A0, 60L);
        task.controlState().apply(A, true, 300);
        task.put(List.of());

        // topic deleted and recreated; consumer resets to 0 without a revoke
        task.put(List.of(rec(A0, 0), rec(A0, 1)));
        assertEquals(List.of(0L, 1L), forwardedOffsets(task), "records of the new topic dropped");
    }

    /**
     * A record in the same batch, after the cut offset, is forwarded and then the seek moves the consumer
     * back to the cut: the same record is delivered and forwarded again (duplicate without any failure).
     */
    @Test
    void readdInOneInterval_recordAfterCutForwardedThenSeekBack() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.atTime.put(A0, 3L);
        assign(task, ctx, A0);
        task.controlState().apply(A, false, 200);
        task.controlState().apply(A, true, 300);
        task.lookup.atTime.put(A0, 60L);
        task.lookup.at(200, A0, 59L); // offset 59 was written after the remove
        task.put(List.of(rec(A0, 59), rec(A0, 60), rec(A0, 61)));
        // offsets 60 and 61 forwarded; seek to 60 queued -> 60, 61 delivered again
        assertEquals(List.of(60L, 61L), forwardedOffsets(task));
        assertTrue(!ctx.seekedTo().containsKey(A0) || ctx.seekedTo().get(A0) >= 62L,
            "seek back to " + ctx.seekedTo().get(A0) + " after forwarding 60 and 61 -> duplicates");
    }
}
