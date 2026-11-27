package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * A remove is time-based, like the add: after "remove A at R" the task still forwards the records of A that come
 * before the remove cut (the first offset whose record timestamp is at or after R), drops the records from the cut
 * on, and pauses the partition once its position reaches the cut.
 */
class FanInSinkTaskRemoveCutTest {

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

    /** A allowed since 100, committed at 5 by a task that applied that add; A0 assigned. */
    private static FanInSinkTaskControlTest.TestTask runningAt5(FanInSinkTaskControlTest.Ctx ctx) {
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        return task;
    }

    private static SinkRecord rec(long offset, long timestamp) {
        return new SinkRecord(A, 0, Schema.OPTIONAL_BYTES_SCHEMA, null, Schema.OPTIONAL_BYTES_SCHEMA,
            ("v" + offset).getBytes(StandardCharsets.UTF_8), offset, timestamp, TimestampType.CREATE_TIME);
    }

    private static List<Long> forwarded(FanInSinkTaskControlTest.TestTask task) {
        List<Long> out = new ArrayList<>();
        task.producer.history().forEach(r -> out.add(Long.parseLong(
            new String(r.headers().lastHeader(FanInSinkTask.HEADER_OFFSET).value(), StandardCharsets.UTF_8))));
        return out;
    }

    private static OffsetAndMetadata om(long o) {
        return new OffsetAndMetadata(o);
    }

    /** Records of A that are still in the consumer lag and were written before the remove time are forwarded. */
    @Test
    void removeCut_forwardsRecordsBeforeTheRemoveTime() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.at(200, A0, 8L); // offsets 8.. have a timestamp at or after 200

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150), rec(6, 160), rec(7, 190)));

        assertEquals(List.of(5L, 6L, 7L), forwarded(task));
        assertFalse(ctx.paused.contains(A0), "the position has not reached the remove cut yet");
        assertTrue(task.lookup.timeQueries.contains(200L), task.lookup.timeQueries.toString());
    }

    @Test
    void removeCut_dropsRecordsAtOrAfterTheRemoveTime() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.at(200, A0, 8L);

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(7, 190), rec(8, 210), rec(9, 220)));

        assertEquals(List.of(7L), forwarded(task));
        Map<TopicPartition, OffsetAndMetadata> c = task.preCommit(Map.of(A0, om(10)));
        assertEquals(8L, c.get(A0).offset(), "not committed past the first dropped record");
        assertEquals(ControlLedger.removedAt(200, 8).encode(), c.get(A0).metadata());
    }

    @Test
    void removeCut_pausesOnceThePositionReachesTheCut() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.at(200, A0, 8L);

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150)));
        assertFalse(ctx.paused.contains(A0));
        task.put(List.of(rec(6, 150), rec(7, 150)));
        assertFalse(ctx.paused.contains(A0));
        task.put(List.of(rec(8, 210)));
        assertTrue(ctx.paused.contains(A0), "paused once a record at the cut was read");
        assertEquals(List.of(5L, 6L, 7L), forwarded(task));
    }

    @Test
    void removeCut_positionAlreadyPastTheCut_pausesAtOnce() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.at(200, A0, 3L); // the committed position 5 is already past the cut

        task.controlState().apply(A, false, 200);
        task.put(List.of());

        assertTrue(ctx.paused.contains(A0));
    }

    @Test
    void removeCut_noRecordAtOrAfterTheRemoveTime_cutsAtTheEndOffset() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.ends.put(A0, 9L); // no record at or after 200 yet; records 9.. are written later

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(7, 190), rec(8, 190), rec(9, 150)));

        assertEquals(List.of(7L, 8L), forwarded(task), "a record written after the remove was applied is dropped");
        assertTrue(ctx.paused.contains(A0));
        assertEquals(ControlLedger.removedAt(200, 9).encode(), task.preCommit(Map.of(A0, om(10))).get(A0).metadata());
    }

    /** The cut lookup fails: the partition is held (paused, commit not moved), and no record before R is lost. */
    @Test
    void removeCut_lookupFails_holdsAndDoesNotDrop() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("broker away");

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150), rec(6, 150)));
        assertEquals(List.of(), forwarded(task));
        assertTrue(ctx.paused.contains(A0));
        Map<TopicPartition, OffsetAndMetadata> c = task.preCommit(Map.of(A0, om(7)));
        assertEquals(5L, c.get(A0).offset(), "commit held at the first held record");
        assertEquals(ControlLedger.allowedAt(100).encode(), c.get(A0).metadata(),
            "the ledger stays as committed, so a restart looks the cut up again");

        task.lookup.failure = null;
        task.lookup.at(200, A0, 8L);
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(5L, ctx.seekedTo().get(A0), "the held records are read again");
        assertFalse(ctx.paused.contains(A0));

        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210)));
        assertEquals(List.of(5L, 6L, 7L), forwarded(task));
        assertTrue(ctx.paused.contains(A0));
    }

    /** Remove at 200 and add at 300 in one interval: before R forwarded, [R, T) dropped, from T forwarded. */
    @Test
    void removeThenReaddSameInterval_forwardsBeforeR_dropsBetween_forwardsAfterT() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.at(200, A0, 8L);
        task.lookup.at(300, A0, 12L);

        task.controlState().apply(A, false, 200);
        task.controlState().apply(A, true, 300);
        task.put(List.of(rec(5, 150), rec(6, 160), rec(7, 190)));
        assertEquals(List.of(5L, 6L, 7L), forwarded(task));
        assertFalse(ctx.paused.contains(A0));
        assertNull(ctx.seekedTo().get(A0), "no move before the remove cut is reached");

        task.put(List.of(rec(8, 210), rec(9, 250)));
        assertEquals(12L, ctx.seekedTo().get(A0), "moves over the removed records to the add cut");
        assertEquals(List.of(5L, 6L, 7L), forwarded(task));

        task.put(List.of(rec(12, 310)));
        assertEquals(List.of(5L, 6L, 7L, 12L), forwarded(task));
        assertEquals(ControlLedger.allowedAt(300).encode(), task.preCommit(Map.of(A0, om(13))).get(A0).metadata());
    }

    /** The same rule when the add comes in a later put() while the partition is still before the remove cut. */
    @Test
    void removeThenReaddLaterInterval_forwardsBeforeR_dropsBetween_forwardsAfterT() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.at(200, A0, 8L);
        task.lookup.at(300, A0, 12L);

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150), rec(6, 160)));
        task.controlState().apply(A, true, 300);
        task.put(List.of(rec(7, 190), rec(8, 210), rec(9, 250)));
        assertEquals(List.of(5L, 6L, 7L), forwarded(task));
        assertEquals(12L, ctx.seekedTo().get(A0));

        task.put(List.of(rec(12, 310)));
        assertEquals(List.of(5L, 6L, 7L, 12L), forwarded(task));
    }

    /**
     * A restart while the partition is still before the remove cut: the new task takes the cut from the ledger (no
     * new lookup, whose answer can differ), forwards the rest of the records before it once, and drops the others.
     */
    @Test
    void removeCut_restartBeforeTheCutIsReached_doesNotLoseOrDuplicate() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask first = runningAt5(ctx);
        first.lookup.at(200, A0, 8L);
        first.controlState().apply(A, false, 200);
        first.put(List.of(rec(5, 150), rec(6, 160)));
        Map<TopicPartition, OffsetAndMetadata> c = first.preCommit(Map.of(A0, om(7)));
        assertEquals(7L, c.get(A0).offset());
        assertEquals(ControlLedger.removedAt(200, 8).encode(), c.get(A0).metadata());
        first.stop();

        FanInSinkTaskControlTest.Ctx ctx2 = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask second = started(ctx2, add(A, 100), remove(A, 200));
        second.lookup.commit(c);
        second.lookup.at(200, A0, 99L); // a lookup now would give another answer
        second.lookup.ends.put(A0, 99L);
        ctx2.assignment.add(A0);
        second.open(List.of(A0));
        assertTrue(ctx2.seeks.isEmpty(), ctx2.seeks.toString());
        second.put(List.of());
        assertFalse(ctx2.paused.contains(A0), "the partition runs until it reaches the cut of the ledger");

        second.put(List.of(rec(7, 190), rec(8, 210)));
        assertEquals(List.of(5L, 6L), forwarded(first));
        assertEquals(List.of(7L), forwarded(second), "offset 7 once, offset 8 not at all");
        assertTrue(ctx2.paused.contains(A0));
        assertFalse(second.lookup.timeQueries.contains(200L), "the cut comes from the ledger");
    }

    /** A rebalance before the cut is reached: the partition gets the same cut again, from the committed ledger. */
    @Test
    void removeCut_rebalanceBeforeTheCut_newOwnerAppliesTheSameCut() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = runningAt5(ctx);
        task.lookup.at(200, A0, 8L);
        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(5, 150)));
        // revoke: the framework calls preCommit and commits synchronously, then close()
        task.lookup.commit(task.preCommit(Map.of(A0, om(6))));
        task.close(List.of(A0));
        ctx.paused.remove(A0);

        task.lookup.at(200, A0, 99L); // a new lookup would give another answer
        task.lookup.ends.put(A0, 20L); // the log ends after the cut of the ledger
        task.open(List.of(A0));
        task.put(List.of());
        assertFalse(ctx.paused.contains(A0));
        task.put(List.of(rec(6, 160), rec(7, 190), rec(8, 210), rec(9, 220)));
        assertEquals(List.of(5L, 6L, 7L), forwarded(task));
        assertTrue(ctx.paused.contains(A0));
        Map<TopicPartition, OffsetAndMetadata> c = task.preCommit(Map.of(A0, om(10)));
        assertEquals(8L, c.get(A0).offset());
        assertEquals(ControlLedger.removedAt(200, 8).encode(), c.get(A0).metadata());
    }

    /**
     * A new owner whose committed ledger does not show the remove yet (the old owner had not committed since the
     * remove): it looks the cut up by the remove time, which gives the same offset.
     */
    @Test
    void removeCut_newOwnerWithoutTheRemoveInTheLedger_looksTheCutUpByTime() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100), remove(A, 200));
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.lookup.at(200, A0, 8L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        task.put(List.of());
        assertFalse(ctx.paused.contains(A0));

        task.put(List.of(rec(5, 150), rec(6, 150), rec(7, 190), rec(8, 210)));
        assertEquals(List.of(5L, 6L, 7L), forwarded(task));
        assertTrue(ctx.paused.contains(A0));
    }

    /**
     * Restart after a re-add while the partition was still before the remove cut: forward up to the remove cut,
     * then move to the add cut.
     */
    @Test
    void removeCut_restartAfterReadd_forwardsToTheCutThenSkipsToTheAddTime() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, add(A, 100), remove(A, 200), add(A, 300));
        task.lookup.committed.put(A0, 6L);
        task.lookup.metadata.put(A0, ControlLedger.removedAt(200, 8).encode());
        task.lookup.at(300, A0, 12L);
        task.lookup.ends.put(A0, 20L); // the log ends after the cut of the ledger
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        assertTrue(ctx.seeks.isEmpty(), "no move before the remove cut is reached: " + ctx.seeks);

        task.put(List.of(rec(6, 160), rec(7, 190), rec(8, 210)));
        assertEquals(List.of(6L, 7L), forwarded(task));
        assertEquals(12L, ctx.seekedTo().get(A0));
        task.put(List.of(rec(12, 310)));
        assertEquals(List.of(6L, 7L, 12L), forwarded(task));
    }

    @Test
    void ledger_removedWithCut_roundTrip() {
        assertEquals("fanin.ctl:v2:removed:200:8", ControlLedger.removedAt(200, 8).encode());
        assertEquals(ControlLedger.removedAt(200, 8), ControlLedger.parse("fanin.ctl:v2:removed:200:8"));
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse("fanin.ctl:v1:removed:200"));
        assertFalse(ControlLedger.removedAt(200).hasCut());
        // A removed ledger with a cut that is not valid is read as removed without a cut (forward nothing more).
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse("fanin.ctl:v1:removed:200:x"));
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse("fanin.ctl:v1:removed:200:-1"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:allowed:100:8"));
    }

    /** Forms written by earlier builds are still read; the form with a cut is written under v2. */
    @Test
    void ledger_v1FormsOfEarlierBuilds_roundTrip() {
        assertEquals("fanin.ctl:v1:allowed:100", ControlLedger.allowedAt(100).encode());
        assertEquals("fanin.ctl:v1:removed:200", ControlLedger.removedAt(200).encode());
        for (ControlLedger l : List.of(ControlLedger.allowedAt(100), ControlLedger.removedAt(200),
            ControlLedger.removedAt(200, 8))) {
            assertEquals(l, ControlLedger.parse(l.encode()));
        }
        assertEquals(ControlLedger.allowedAt(100), ControlLedger.parse("fanin.ctl:v1:allowed:100"));
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse("fanin.ctl:v1:removed:200"));
        // written under v1 by the first build of the time-based remove (8b46a87)
        assertEquals(ControlLedger.removedAt(200, 8), ControlLedger.parse("fanin.ctl:v1:removed:200:8"));
        assertEquals(ControlLedger.allowedAt(100), ControlLedger.parse("fanin.ctl:v2:allowed:100"));
    }

    @Test
    void ledger_parserIsStrict() {
        assertNull(ControlLedger.parse(null));
        assertNull(ControlLedger.parse(""));
        assertNull(ControlLedger.parse("fanin.ctl:v3:allowed:100"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:allowed:+100"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:allowed:-100"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:allowed:"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:allowed: 100"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:allowed:99999999999999999999"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:removed:-200"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:removed:+200:8"));
        assertNull(ControlLedger.parse("fanin.ctl:v2:removed::8"));
        assertNull(ControlLedger.parse("fanin.ctl:v1:paused:100"));
        // removed with a valid time: a cut that is not valid gives no cut
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse("fanin.ctl:v1:removed:200:"));
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse("fanin.ctl:v2:removed:200:+8"));
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse("fanin.ctl:v2:removed:200:8:9"));
        assertEquals(ControlLedger.removedAt(200), ControlLedger.parse("fanin.ctl:v2:removed:200:99999999999999999999"));
        assertEquals(ControlLedger.removedAt(0, 0), ControlLedger.parse("fanin.ctl:v2:removed:0:0"));
        assertEquals(ControlLedger.removedAt(200, Long.MAX_VALUE),
            ControlLedger.parse("fanin.ctl:v2:removed:200:" + Long.MAX_VALUE));
    }
}
