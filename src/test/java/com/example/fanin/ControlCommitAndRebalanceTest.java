package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

/** Commits on revoke and stop, commit hold of pending partitions, EARLIEST re-add, wrong consumer group. */
class ControlCommitAndRebalanceTest {

    private static final String CTL = "ctl";
    private static final String A = "in.a";
    private static final String B = "in.b";
    private static final TopicPartition A0 = new TopicPartition(A, 0);
    private static final TopicPartition B0 = new TopicPartition(B, 0);

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

    private static SinkRecord rec(TopicPartition tp, long offset) {
        return new SinkRecord(tp.topic(), tp.partition(), Schema.OPTIONAL_BYTES_SCHEMA, null,
            Schema.OPTIONAL_BYTES_SCHEMA, ("v" + offset).getBytes(StandardCharsets.UTF_8), offset, 5L,
            TimestampType.CREATE_TIME);
    }

    private static List<String> forwarded(FanInSinkTaskControlTest.TestTask task) {
        List<String> out = new ArrayList<>();
        task.producer.history().forEach(r -> out.add(
            new String(r.headers().lastHeader(FanInSinkTask.HEADER_TOPIC).value(), StandardCharsets.UTF_8) + "@"
                + new String(r.headers().lastHeader(FanInSinkTask.HEADER_OFFSET).value(), StandardCharsets.UTF_8)));
        return out;
    }

    private static OffsetAndMetadata om(long o) {
        return new OffsetAndMetadata(o);
    }

    /**
     * WorkerSinkTask.closeAllPartitions (line 687) commits currentOffsets.keySet(). A revoke that is not "lost"
     * does not remove the partition from currentOffsets (only line 702, lost path, does). So at stop, preCommit gets
     * a partition that this task closed earlier. Its ledger is unknown, preCommit returns {} and the final commit is
     * skipped for every partition.
     */
    @Test
    void stopAfterAnEarlierRevoke_finalCommitMustNotBeSkipped() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100), add(B, 100)});
        task.lookup.committed.putAll(Map.of(A0, 5L, B0, 5L));
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.lookup.metadata.put(B0, ControlLedger.allowedAt(100).encode());
        assign(task, ctx, A0, B0);
        task.put(List.of(rec(A0, 5), rec(B0, 5)));

        // B0 moves to another task (cooperative revoke, not lost)
        task.preCommit(Map.of(B0, om(6)));
        task.close(List.of(B0));
        ctx.assignment.remove(B0);

        task.put(List.of(rec(A0, 6), rec(A0, 7), rec(A0, 8)));
        // stop: closeAllPartitions -> commitOffsets(closing, currentOffsets.keySet() = {A0, B0})
        Map<TopicPartition, OffsetAndMetadata> fin = task.preCommit(Map.of(A0, om(9), B0, om(6)));
        assertEquals(9L, fin.containsKey(A0) ? fin.get(A0).offset() : -1L,
            "final commit at stop was skipped: preCommit returned " + fin);
    }

    /**
     * One partition whose add-time lookup keeps failing (here: ListOffsets for B0 fails, for example no
     * DESCRIBE for the target key on topic B, or B deleted) holds every commit of the task, also of A0, which keeps
     * forwarding. On restart all of A0 since the last commit is sent again. Only a DEBUG line says commits are held.
     */
    @Test
    void onePendingPartition_mustNotHoldCommitsOfOtherPartitionsForever() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = new FanInSinkTaskControlTest.TestTask() {
            @Override
            StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
                FanInSinkTaskControlTest.Lookup l = new FanInSinkTaskControlTest.Lookup() {
                    @Override
                    public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> partitions,
                                                                      long timestampMs) {
                        if (partitions.contains(B0)) {
                            throw new org.apache.kafka.common.errors.TopicAuthorizationException("no DESCRIBE on in.b");
                        }
                        return super.offsetsAtOrAfter(partitions, timestampMs);
                    }
                };
                l.committed.put(A0, 5L);
                l.metadata.put(A0, ControlLedger.allowedAt(100).encode());
                return l;
            }
        };
        started(ctx, task, new Object[][] {add(A, 100), add(B, 100)});
        assign(task, ctx, A0); // ok
        assign(task, ctx, B0); // cooperative: B0 alone, no ledger, LATEST -> cut -> lookup fails -> pending
        for (int i = 0; i < 10; i++) {
            task.now += ControlCuts.CONTROL_RETRY_MS;
            task.put(List.of(rec(A0, 5 + i)));
        }
        assertEquals(10, forwarded(task).size());
        Map<TopicPartition, OffsetAndMetadata> c = task.preCommit(Map.of(A0, om(15), B0, om(0)));
        assertTrue(c.containsKey(A0), "A0 forwarded 10 records but nothing is committed: " + c);
    }

    /**
     * EARLIEST. A was removed at 200; the task wrote removed:200. A restart after the tombstone was deleted by
     * compaction: the control topic has no record for A. Then A is added again at 300 while the task runs. The live
     * path only checks the in-memory state (not readd), ignores the committed ledger removed:200, and resumes at the
     * committed offset: records written while A was removed are forwarded. open() would cut (docs table row
     * "removed:<t> -> Yes, also with EARLIEST after the tombstone is gone").
     */
    @Test
    void earliest_liveReaddAfterTombstoneGone_mustCut() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {}, FanInSinkConfig.CONTROL_ADD_START, "EARLIEST");
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.removedAt(200).encode());
        task.lookup.atTime.put(A0, 60L);
        assign(task, ctx, A0);
        assertTrue(ctx.paused.contains(A0));

        task.controlState().apply(A, true, 300);
        task.put(List.of());
        assertEquals(60L, ctx.seekedTo().getOrDefault(A0, -1L), "no cut at the re-add time");
    }

    /**
     * Same as earliest_liveReaddAfterTombstoneGone_mustCut, but the ledger was allowed:100 and the task wrote
     * removed:100 itself.
     */
    @Test
    void earliest_liveReaddAfterTombstoneGone_allowedLedger_mustCut() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {}, FanInSinkConfig.CONTROL_ADD_START, "EARLIEST");
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.lookup.atTime.put(A0, 60L);
        assign(task, ctx, A0);
        assertEquals(ControlLedger.removedAt(100).encode(), task.preCommit(Map.of(A0, om(5))).get(A0).metadata());

        task.controlState().apply(A, true, 300);
        // Adapted: the record arrives in the put() that applies the add. The first version of this test delivered it
        // in the put() after the seek to 60, which the framework applies before that put(); a record below the seek
        // there means the topic was created again (ControlCutStabilityTest.recreatedTopic_staleDropBelow_...).
        task.put(List.of(rec(A0, 6)));
        assertEquals(60L, ctx.seekedTo().getOrDefault(A0, -1L), "no cut at the re-add time");
        assertEquals(List.of(), forwarded(task), "record written while A was removed was forwarded");
    }

    /**
     * fanin.start.consumer.group points to a group that does not exist (typo, group.id override). decide()
     * then cuts every LATEST partition at its add time without the committed offsets, on every open(). The ledger is
     * never read, and each rebalance moves the partition back to the add time: the same records again each time.
     */
    @Test
    void groupNotFound_rebalanceMustNotRewindAgain() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100)});
        task.lookup.committed = null; // group not found
        task.lookup.atTime.put(A0, 3L);
        assign(task, ctx, A0);
        assertEquals(Map.of(A0, 3L), ctx.seekedTo());
        task.put(List.of());
        task.put(List.of(rec(A0, 3), rec(A0, 4), rec(A0, 5), rec(A0, 6)));
        task.preCommit(Map.of(A0, om(7)));
        ctx.seeks.clear();
        task.close(List.of(A0));
        task.open(List.of(A0));
        assertTrue(ctx.seeks.isEmpty(), "rebalance moved A0 back again: " + ctx.seeks);
    }
}
