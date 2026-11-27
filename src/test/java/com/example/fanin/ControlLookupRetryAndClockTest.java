package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
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
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

/**
 * Per-partition lookups, open() retries and their wait, remove times from other clocks, quick re-add after a remove,
 * a failing move in open(), the add cut of a running partition.
 */
class ControlLookupRetryAndClockTest {

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

    private static FanInSinkTaskControlTest.TestTask started(FanInSinkTaskControlTest.Ctx ctx,
                                                            FanInSinkTaskControlTest.TestTask task,
                                                            Object[][] control, String... extra) {
        task.initialControl(control);
        task.initialize(ctx.context());
        task.start(props(extra));
        return task;
    }

    private static SinkRecord rec(TopicPartition tp, long offset) {
        return rec(tp, offset, 5L);
    }

    private static SinkRecord rec(TopicPartition tp, long offset, long timestamp) {
        return new SinkRecord(tp.topic(), tp.partition(), Schema.OPTIONAL_BYTES_SCHEMA, null,
            Schema.OPTIONAL_BYTES_SCHEMA, ("v" + offset).getBytes(StandardCharsets.UTF_8), offset, timestamp,
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

    /** A lookup whose offset lookups fail for every partition of topic in.b, as with a missing DESCRIBE ACL. */
    static class FailingB extends FanInSinkTaskControlTest.Lookup {
        boolean failB = true;

        @Override
        public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> partitions, long timestampMs) {
            if (failB && partitions.stream().anyMatch(tp -> tp.topic().equals(B))) {
                throw new org.apache.kafka.common.errors.TopicAuthorizationException("no DESCRIBE on in.b");
            }
            return super.offsetsAtOrAfter(partitions, timestampMs);
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            if (failB && partitions.stream().anyMatch(tp -> tp.topic().equals(B))) {
                throw new org.apache.kafka.common.errors.TopicAuthorizationException("no DESCRIBE on in.b");
            }
            return super.endOffsets(partitions);
        }
    }

    /**
     * Eager assignment: A0 and B0 arrive in one open(). The offset lookup of B0 fails. A0 must still be cut,
     * forward and commit; only B0 stays paused, and B0 is decided when its lookup works again.
     */
    @Test
    void eagerBatch_oneFailingTopicDoesNotStopTheOthers() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FailingB failing = new FailingB();
        FanInSinkTaskControlTest.TestTask task = new FanInSinkTaskControlTest.TestTask() {
            @Override
            StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
                return failing;
            }
        };
        started(ctx, task, new Object[][] {add(A, 100), add(B, 100)});
        failing.atTime.put(A0, 3L);
        failing.atTime.put(B0, 4L);
        ctx.assignment.addAll(List.of(A0, B0));
        task.open(List.of(A0, B0));

        assertEquals(Map.of(A0, 3L), ctx.seekedTo(), "A0 is cut although the lookup of B0 failed");
        assertTrue(ctx.paused.contains(B0));
        assertFalse(ctx.paused.contains(A0));

        task.put(List.of(rec(A0, 3), rec(A0, 4), rec(B0, 4)));
        assertEquals(List.of("in.a@3", "in.a@4"), forwarded(task));
        Map<TopicPartition, OffsetAndMetadata> c = task.preCommit(Map.of(A0, om(5), B0, om(5)));
        assertEquals(5L, c.get(A0).offset());
        assertEquals(ControlLedger.allowedAt(100).encode(), c.get(A0).metadata());
        assertEquals(4L, c.get(B0).offset(), "B0 is not committed past its first dropped record");

        failing.failB = false;
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(4L, ctx.seekedTo().get(B0));
        assertFalse(ctx.paused.contains(B0));
    }

    /**
     * A partition that open() could not decide is paused; this task commits its position meanwhile. The retry must
     * use the committed offset that open() saw (none), so that the start timestamp still applies.
     */
    @Test
    void openRetry_usesTheCommittedOffsetThatOpenSaw() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FailingB failing = new FailingB();
        FanInSinkTaskControlTest.TestTask task = new FanInSinkTaskControlTest.TestTask() {
            @Override
            StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
                return failing;
            }
        };
        started(ctx, task, new Object[][] {add(B, 100)}, FanInSinkConfig.START_TIMESTAMP, "50",
            FanInSinkConfig.CONTROL_ADD_START, "EARLIEST");
        failing.atTime.put(B0, 2L);
        ctx.assignment.add(B0);
        task.open(List.of(B0));
        assertTrue(ctx.paused.contains(B0));
        failing.commit(task.preCommit(Map.of(B0, om(9)))); // the position of a consumer with auto.offset.reset=latest

        failing.failB = false;
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(2L, ctx.seekedTo().get(B0), "the start timestamp no longer applied after the own commit");
    }

    /**
     * Trace minor: the remove record was written by a writer whose clock is behind (remove time 50, before the add
     * time 100 of the ledger). The control topic still shows that the remove came after the add of the ledger, and
     * the topic was added again at 300, so the partition must be cut: it forwards its records up to the remove cut
     * (offset 20, the first record at or after 50) and then moves to the add cut (offset 60).
     */
    @Test
    void restart_removeTimeFromASlowClock_stillCuts() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100), remove(A, 50), add(A, 300)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.lookup.atTime.put(A0, 60L);
        task.lookup.at(50, A0, 20L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        assertTrue(ctx.seeks.isEmpty(), "the committed offset 5 is before the remove cut 20: " + ctx.seeks);
        assertTrue(task.lookup.timeQueries.contains(50L), task.lookup.timeQueries.toString());
        task.put(List.of(rec(A0, 19), rec(A0, 20)));
        assertEquals(List.of("in.a@19"), forwarded(task));
        assertEquals(Map.of(A0, 60L), ctx.seekedTo());
    }

    /** The other direction: a remove before the add of the ledger, with a clock ahead, must not cut. */
    @Test
    void restart_removeBeforeTheLedgerAdd_clockAhead_doesNotCut() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100), remove(A, 500), add(A, 300)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(300).encode());
        task.lookup.atTime.put(A0, 60L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        assertTrue(ctx.seeks.isEmpty(), "the ledger records the add after the remove: " + ctx.seeks);
    }

    /**
     * Trace minor: remove at 200 and add at 300 come in two put() calls. The put() that applies the remove drops
     * records written after the add time (offset 8, time 350). A commit past them would make the re-add start at the
     * committed offset, after them. The commit must stay at the first dropped record written after the remove.
     * Offset 6 (time 150) was written before the remove and is forwarded (remove cut at offset 7).
     */
    @Test
    void reAddRightAfterRemove_recordsAfterTheAddTimeAreNotLost() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        task.put(List.of(rec(A0, 5, 150)));

        task.lookup.at(200, A0, 7L);
        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(A0, 6, 150), rec(A0, 7, 250), rec(A0, 8, 350)));
        Map<TopicPartition, OffsetAndMetadata> c = task.preCommit(Map.of(A0, om(9)));
        assertEquals(7L, c.get(A0).offset(), "committed past records written after the remove");
        assertEquals(ControlLedger.removedAt(200, 7).encode(), c.get(A0).metadata());
        task.lookup.commit(c);

        task.controlState().apply(A, true, 300);
        task.lookup.atTime.put(A0, 8L);
        task.put(List.of());
        assertEquals(8L, ctx.seekedTo().get(A0));
        task.put(List.of(rec(A0, 8, 350)));
        assertEquals(List.of("in.a@5", "in.a@6", "in.a@8"), forwarded(task));
        assertEquals(9L, task.preCommit(Map.of(A0, om(9))).get(A0).offset());
    }

    /**
     * context.offset() throws in open(), for example because the consumer rejects the move. open() must not throw:
     * the failure is handled like a failed committed offset lookup, so the partition stays paused as pending and the
     * move is tried again after the retry wait.
     */
    @Test
    void openWithFailingContextOffset_doesNotThrowAndPartitionStaysPaused() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        boolean[] offsetFails = {true};
        SinkTaskContext inner = ctx.context();
        SinkTaskContext context = (SinkTaskContext) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {SinkTaskContext.class}, (proxy, m, args) -> {
                if (m.getName().equals("offset") && offsetFails[0]) {
                    throw new IllegalStateException("offset failed");
                }
                try {
                    return m.invoke(inner, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        FanInSinkTaskControlTest.TestTask task = new FanInSinkTaskControlTest.TestTask();
        task.initialControl(add(A, 100));
        task.initialize(context);
        task.start(props());
        task.lookup.atTime.put(A0, 7L); // no committed offset: the partition starts at the add time, a move in open()
        ctx.assignment.add(A0);

        assertDoesNotThrow(() -> task.open(List.of(A0)));
        assertTrue(ctx.paused.contains(A0), "the partition stays paused as pending");
        assertTrue(ctx.seeks.isEmpty(), ctx.seeks.toString());

        offsetFails[0] = false;
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(Map.of(A0, 7L), ctx.seekedTo(), "the retry moves the partition to the add cut");
    }

    /**
     * A failed lookup is tried again only after {@link ControlCuts#CONTROL_RETRY_MS}. A put() before that must not
     * look up again, even though the lookup would now work.
     */
    @Test
    void pendingRetry_waitsForTheRetryInterval() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {add(A, 100)});
        task.lookup.atTime.put(A0, 7L);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("broker not reachable");
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        assertTrue(ctx.paused.contains(A0));

        task.lookup.failure = null;
        task.now += ControlCuts.CONTROL_RETRY_MS - 1;
        task.put(List.of());
        assertEquals(0, task.lookup.committedCalls, "looked up again before the retry interval");
        assertTrue(ctx.seeks.isEmpty(), ctx.seeks.toString());
        assertTrue(ctx.paused.contains(A0));

        task.now += 1;
        task.put(List.of());
        assertEquals(1, task.lookup.committedCalls);
        assertEquals(Map.of(A0, 7L), ctx.seekedTo());
        assertFalse(ctx.paused.contains(A0));
    }

    /**
     * A topic is added while the task runs. Its partition has a committed offset (50) after the add cut (10): it
     * must start at the committed offset, never move back to the cut and forward records again.
     */
    @Test
    void liveAdd_cutBelowTheCommittedOffset_startsAtTheCommittedOffset() {
        FanInSinkTaskControlTest.Ctx ctx = new FanInSinkTaskControlTest.Ctx();
        FanInSinkTaskControlTest.TestTask task = started(ctx, new FanInSinkTaskControlTest.TestTask(),
            new Object[][] {});
        task.lookup.committed.put(A0, 50L);
        ctx.assignment.add(A0);
        task.open(List.of(A0));
        assertTrue(ctx.paused.contains(A0), "not allowed yet");

        task.lookup.atTime.put(A0, 10L);
        task.controlState().apply(A, true, 500);
        task.put(List.of());
        assertEquals(Map.of(A0, 50L), ctx.seekedTo(), "moved below the committed offset");
    }
}
