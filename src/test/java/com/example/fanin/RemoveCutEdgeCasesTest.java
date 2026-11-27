package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/** Edge cases of the time-based remove: lookup failures, redelivery, non-atomic lookups, foreign ledgers. */
class RemoveCutEdgeCasesTest {

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

    private static Object[] remove(String t, long time) {
        return new Object[] {t, null, time};
    }

    private static FanInSinkTaskControlTest.TestTask started(FanInSinkTaskControlTest.Ctx ctx, Object[]... control) {
        FanInSinkTaskControlTest.TestTask task = new FanInSinkTaskControlTest.TestTask();
        task.initialControl(control);
        task.initialize(ctx.context());
        task.start(props());
        return task;
    }

    private static SinkRecord rec(String topic, long offset, long timestamp) {
        return new SinkRecord(topic, 0, Schema.OPTIONAL_BYTES_SCHEMA, null, Schema.OPTIONAL_BYTES_SCHEMA,
            ("v" + offset).getBytes(StandardCharsets.UTF_8), offset, timestamp, TimestampType.CREATE_TIME);
    }

    private static SinkRecord rec(long offset, long timestamp) {
        return rec(A, offset, timestamp);
    }

    private static List<String> forwarded(FanInSinkTaskControlTest.TestTask task) {
        List<String> out = new ArrayList<>();
        task.producer.history().forEach(r -> out.add(
            new String(r.headers().lastHeader(FanInSinkTask.HEADER_TOPIC).value(), StandardCharsets.UTF_8) + "@"
                + new String(r.headers().lastHeader(FanInSinkTask.HEADER_OFFSET).value(), StandardCharsets.UTF_8)));
        return out;
    }

    /**
     * Remove at 200 (cut 8), re-add at 300 (add cut 12), the partition reaches the remove cut in a put() that
     * fails with a RetriableException after the gap was entered. The framework redelivers the same batch; a second
     * remove at 400 is applied at the start of the redelivery. Records 8 and 9 were written while the topic was
     * removed (timestamps 210 and 250, inside [200, 300)) and must never be forwarded.
     */
    @Test
    void redeliveryAfterGap_secondRemove_dropsRecordsWrittenWhileRemoved() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100), add(B, 100));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.lookup.committed.put(B0, 0L);
        task.lookup.metadata.put(B0, ControlLedger.allowedAt(100).encode());
        task.lookup.at(200, A0, 8L);
        task.lookup.at(300, A0, 12L);
        task.lookup.at(400, A0, 20L);
        ctx.assignment.add(A0);
        ctx.assignment.add(B0);
        task.open(List.of(A0, B0));

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150), rec(6, 160)));
        task.controlState().apply(A, true, 300);
        task.put(List.of(rec(7, 190)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7"), forwarded(task));

        // Batch reaches the remove cut; a send of B fails with a retriable error after A's records were filtered.
        List<SinkRecord> batch = List.of(rec(8, 210), rec(9, 250), rec(B, 0, 260));
        task.producer.sendException = new org.apache.kafka.common.errors.TimeoutException("metadata");
        assertThrows(RetriableException.class, () -> task.put(batch));
        task.producer.sendException = null;

        // Redelivery of the same batch; the reader drained a second remove in between.
        task.controlState().apply(A, false, 400);
        task.put(batch);
        List<String> out = forwarded(task);
        assertTrue(!out.contains("in.a@8") && !out.contains("in.a@9"),
            "records written while the topic was removed were forwarded: " + out);
    }

    /**
     * open() cannot read the committed offsets (broker hiccup), so A0 is pending (false). The committed ledger is
     * removed:200:8 at offset 5: records 5..7 were written before the remove and are still due. The re-add at 300
     * comes before the 10 s retry. The add branch overwrites pending false with true, and decide() skips
     * removeCutFromLedger for pending true, so the remove cut of the ledger is never read.
     */
    @Test
    void openLookupFailed_readdBeforeRetry_forwardsRecordsBelowTheRemoveCut() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100), remove(A, 200));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.removedAt(200, 8).encode());
        task.lookup.at(300, A0, 12L);
        task.lookup.ends.put(A0, 20L); // the log ends after the cut of the ledger
        ctx.assignment.add(A0);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("broker away");
        task.open(List.of(A0));
        task.lookup.failure = null;

        task.controlState().apply(A, true, 300);
        task.put(List.of());
        assertNull(ctx.seekedTo().get(A0), "moved to the add cut before forwarding 5..7 (below the remove cut 8): "
            + ctx.seeks);
        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7"), forwarded(task));
        assertEquals(12L, ctx.seekedTo().get(A0));
    }

    /**
     * The remove record of A is compacted away (no add, no remove of A on the control topic), the
     * committed ledger is removed:200:8, open() cannot read it, and the first add (LATEST) comes before the retry.
     */
    @Test
    void compactedRemove_openLookupFailed_firstAddBeforeRetry_keepsTheRemoveCut() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(B, 100));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.removedAt(200, 8).encode());
        task.lookup.at(300, A0, 12L);
        task.lookup.ends.put(A0, 20L); // the log ends after the cut of the ledger
        ctx.assignment.add(A0);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("broker away");
        task.open(List.of(A0));
        task.lookup.failure = null;

        task.controlState().apply(A, true, 300);
        task.put(List.of());
        assertNull(ctx.seekedTo().get(A0), ctx.seeks.toString());
        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7"), forwarded(task));
        assertEquals(12L, ctx.seekedTo().get(A0));
    }

    /**
     * Same setup as openLookupFailed_readdBeforeRetry_forwardsRecordsBelowTheRemoveCut, but the retry runs before
     * the re-add: the ledger cut is used. This case passed before the fix and after it.
     */
    @Test
    void openLookupFailed_retryBeforeReadd_keepsTheRemoveCut() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100), remove(A, 200));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.removedAt(200, 8).encode());
        task.lookup.at(300, A0, 12L);
        task.lookup.ends.put(A0, 20L); // the log ends after the cut of the ledger
        ctx.assignment.add(A0);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("broker away");
        task.open(List.of(A0));
        task.lookup.failure = null;
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());

        task.controlState().apply(A, true, 300);
        task.put(List.of());
        assertNull(ctx.seekedTo().get(A0), ctx.seeks.toString());
        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7"), forwarded(task));
        assertEquals(12L, ctx.seekedTo().get(A0));
    }

    /**
     * Remove cut. The end-offset fallback and the by-time lookup are two ListOffsets calls. Records 8 and 9 (ts 250,
     * at or after R) are appended between the two calls, whichever comes first: before the append the partition ends
     * at 8 and has no record at or after 200; after it, it ends at 10 and offset 8 is the first at or after 200. Asked
     * by time first, the fallback end offset 10 lets 8 and 9 through.
     */
    @Test
    void endOffsetFallback_removeCut_dropsRecordsAppendedBetweenTheLookups() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        task.lookup.ends.put(A0, 8L);
        task.lookup.afterNextOffsetCall = () -> {
            task.lookup.ends.put(A0, 10L); // offsets 8, 9 (ts 250) are appended between the two calls
            task.lookup.at(200, A0, 8L);
        };

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(7, 190), rec(8, 250), rec(9, 250)));
        assertEquals(List.of("in.a@7"), forwarded(task), "records with ts >= R below the end-offset cut");
    }

    /**
     * Add cut. A is added at 300 while the task runs. Records 8 and 9 (ts 350, at or after the add time) are
     * appended between the two lookups. Asked by time first, the fallback end offset 10 skips them.
     */
    @Test
    void endOffsetFallback_addCut_keepsRecordsAppendedBetweenTheLookups() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(B, 100));
        task.lookup.committed.put(A0, 5L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        task.lookup.ends.put(A0, 8L);
        task.lookup.afterNextOffsetCall = () -> {
            task.lookup.ends.put(A0, 10L);
            task.lookup.at(300, A0, 8L);
        };

        task.controlState().apply(A, true, 300);
        task.put(List.of());
        assertEquals(8L, ctx.seekedTo().get(A0), ctx.seeks.toString());
        task.put(List.of(rec(8, 350), rec(9, 350)));
        assertEquals(List.of("in.a@8", "in.a@9"), forwarded(task));
    }

    /**
     * A foreign or corrupt ledger has a huge cut. Without a cap, a removed topic would forward every record. The cut
     * is capped at the end offset.
     */
    @Test
    void ledgerWithHugeCut_cutIsCappedAtTheEndOffset() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100), remove(A, 200));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, "fanin.ctl:v1:removed:200:" + Long.MAX_VALUE);
        task.lookup.ends.put(A0, 6L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        task.put(List.of());
        task.put(List.of(rec(5, 150), rec(6, 5000), rec(7, 9000)));
        assertEquals(List.of("in.a@5"), forwarded(task), "the ledger cut is not checked against the end offset");
    }

    /**
     * The end offset lookup that checks the ledger cut fails in open(): the partition is not decided, nothing is
     * forwarded or committed, and the retry reads the ledger again and applies its cut.
     */
    @Test
    void ledgerCutEndLookupFails_partitionWaitsAndRetries() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100), remove(A, 200));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.removedAt(200, 8).encode());
        task.lookup.ends.put(A0, 20L);
        ctx.assignment.add(A0);
        task.lookup.endFailure = new org.apache.kafka.common.errors.TimeoutException("broker away");
        task.open(List.of(A0));
        assertTrue(ctx.paused.contains(A0));
        task.put(List.of(rec(5, 150)));
        assertEquals(List.of(), forwarded(task));
        assertTrue(task.preCommit(Map.of(A0, new OffsetAndMetadata(6))).isEmpty(), "the ledger is not known");

        task.lookup.endFailure = null;
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(5L, ctx.seekedTo().get(A0));
        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7"), forwarded(task));
        assertTrue(ctx.paused.contains(A0));
    }

    /** A ledger cut at or below the end offset is kept as it is. */
    @Test
    void ledgerCutBelowTheEnd_isNotCapped() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100), remove(A, 200));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.removedAt(200, 8).encode());
        task.lookup.ends.put(A0, 8L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7"), forwarded(task));
    }

    /**
     * Ledger through remove and re-add on a running task, as seen on Confluent Cloud with 5e38333: the commit made
     * between the re-add decision and the framework's seek (WorkerSinkTask.iteration commits before poll(), and
     * poll() applies the seek) is below the add cut. Its ledger must carry the remove time, not the first add time.
     * After the records from the add cut are forwarded, the ledger is allowed:<re-add time>.
     */
    @Test
    void readd_noCommitBetween_ledgerCarriesTheRemoveTimeThenTheReaddState() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.lookup.ends.put(A0, 20L);
        task.lookup.at(200, A0, 8L);
        task.lookup.at(300, A0, 12L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210), rec(9, 220)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7"), forwarded(task));
        // no commit between the remove and the re-add: the committed ledger is still allowed:100
        task.controlState().apply(A, true, 300);
        task.put(List.of());
        assertEquals(12L, ctx.seekedTo().get(A0));

        OffsetAndMetadata between = task.preCommit(Map.of(A0, new OffsetAndMetadata(10))).get(A0);
        assertEquals(10L, between.offset(), "below the add cut 12: the framework has not applied the seek yet");
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse(between.metadata()),
            "the remove time, not the first add time: " + between.metadata());

        task.put(List.of(rec(12, 310), rec(13, 320)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7", "in.a@12", "in.a@13"), forwarded(task));
        OffsetAndMetadata after = task.preCommit(Map.of(A0, new OffsetAndMetadata(14))).get(A0);
        assertEquals(14L, after.offset());
        assertEquals(ControlLedger.allowedAt(300).encode(), after.metadata());
    }

    /** As above, with a commit between the remove and the re-add: that commit carries the remove cut. */
    @Test
    void readd_commitBetween_ledgerCarriesTheRemoveCutThenTheReaddState() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.lookup.ends.put(A0, 20L);
        task.lookup.at(200, A0, 8L);
        task.lookup.at(300, A0, 12L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210), rec(9, 220)));
        OffsetAndMetadata removed = task.preCommit(Map.of(A0, new OffsetAndMetadata(10))).get(A0);
        assertEquals(8L, removed.offset());
        assertEquals(ControlLedger.removedAt(200, 8).encode(), removed.metadata());
        task.lookup.commit(Map.of(A0, removed));

        task.controlState().apply(A, true, 300);
        task.put(List.of());
        assertEquals(12L, ctx.seekedTo().get(A0));
        OffsetAndMetadata between = task.preCommit(Map.of(A0, new OffsetAndMetadata(10))).get(A0);
        assertEquals(10L, between.offset());
        ControlLedger b = ControlLedger.parse(between.metadata());
        assertTrue(!b.allowed() && b.timeMs() == 200, "the remove time: " + between.metadata());

        task.put(List.of(rec(12, 310), rec(13, 320)));
        OffsetAndMetadata after = task.preCommit(Map.of(A0, new OffsetAndMetadata(14))).get(A0);
        assertEquals(14L, after.offset());
        assertEquals(ControlLedger.allowedAt(300).encode(), after.metadata());
    }

    /**
     * The remove lookup fails, then the reader stalls, then the lookup works while stalled, then the reader
     * recovers: each record below the cut is forwarded exactly once and nothing at or after the cut.
     */
    @Test
    void removeLookupFails_thenReaderStalls_thenRecovers_forwardsEachRecordOnce() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        task.put(List.of(rec(5, 150)));
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("x");
        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(6, 150), rec(7, 190), rec(8, 210)));
        task.readerCaughtUpAt = task.now - 60_000; // reader behind
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        task.lookup.failure = null;
        task.lookup.at(200, A0, 8L);
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.readerCaughtUpAt = task.now - 60_000;
        task.put(List.of());
        assertEquals(6L, task.preCommit(Map.of(A0, new OffsetAndMetadata(9))).get(A0).offset());
        task.readerCaughtUpAt = null;
        task.put(List.of());
        assertEquals(6L, ctx.seekedTo().get(A0));
        task.put(List.of(rec(6, 150), rec(7, 190), rec(8, 210)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@7"), forwarded(task));
        assertTrue(ctx.paused.contains(A0));
        OffsetAndMetadata c = task.preCommit(Map.of(A0, new OffsetAndMetadata(9))).get(A0);
        assertEquals(8L, c.offset());
        assertEquals(ControlLedger.removedAt(200, 8).encode(), c.metadata());
    }

    /** Idle partition: the last forwarded record ends exactly at the end-offset cut. */
    @Test
    void idle_endCutReachedByForwardedRecords_staysUnpaused() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        task.lookup.ends.put(A0, 8L);
        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 150)));
        task.put(List.of());
        task.put(List.of());
        assertTrue(!ctx.paused.contains(A0), "observed: stays unpaused until a record at the cut arrives");
    }
}
