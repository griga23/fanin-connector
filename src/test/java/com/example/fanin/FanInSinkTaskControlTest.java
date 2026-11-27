package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

/** The control topic, as seen by the task: pause, resume, seek and drop. */
class FanInSinkTaskControlTest {

    private static final String TARGET = "fanin-out";
    private static final String CTL = "ctl";
    private static final TopicPartition CTL0 = new TopicPartition(CTL, 0);

    private static final String A = "in.a";
    private static final String B = "in.b";
    private static final TopicPartition A0 = new TopicPartition(A, 0);
    private static final TopicPartition A1 = new TopicPartition(A, 1);
    private static final TopicPartition B0 = new TopicPartition(B, 0);

    /** Fake framework context: records pause, resume and seeks, and keeps an assignment. */
    static class Ctx {
        final Set<TopicPartition> assignment = new LinkedHashSet<>();
        final Set<TopicPartition> paused = new HashSet<>();
        final List<TopicPartition> resumes = new ArrayList<>();
        final List<Map<TopicPartition, Long>> seeks = new ArrayList<>();

        SinkTaskContext context() {
            return (SinkTaskContext) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {SinkTaskContext.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "assignment":
                            return Set.copyOf(assignment);
                        case "pause":
                            for (TopicPartition tp : (TopicPartition[]) args[0]) {
                                if (!assignment.contains(tp)) {
                                    throw new IllegalStateException("not assigned: " + tp);
                                }
                                paused.add(tp);
                            }
                            return null;
                        case "resume":
                            for (TopicPartition tp : (TopicPartition[]) args[0]) {
                                if (!assignment.contains(tp)) {
                                    throw new IllegalStateException("not assigned: " + tp);
                                }
                                paused.remove(tp);
                                resumes.add(tp);
                            }
                            return null;
                        case "offset":
                            if (args.length == 1) {
                                @SuppressWarnings("unchecked")
                                Map<TopicPartition, Long> m = (Map<TopicPartition, Long>) args[0];
                                seeks.add(new HashMap<>(m));
                            } else {
                                seeks.add(Map.of((TopicPartition) args[0], (Long) args[1]));
                            }
                            return null;
                        default:
                            return null;
                    }
                });
        }

        /** All seeks merged, last one wins. */
        Map<TopicPartition, Long> seekedTo() {
            Map<TopicPartition, Long> all = new HashMap<>();
            seeks.forEach(all::putAll);
            return all;
        }
    }

    /** Fake broker lookups. */
    static class Lookup implements StartOffsets.Lookup {
        Map<TopicPartition, Long> committed = new HashMap<>();
        /** Metadata of the committed offsets, for example a ledger. */
        Map<TopicPartition, String> metadata = new HashMap<>();
        int committedCalls;
        Map<TopicPartition, Long> atTime = new HashMap<>();
        /** Answers for one timestamp; a partition not in it falls back to {@link #atTime}. */
        Map<Long, Map<TopicPartition, Long>> atTimeFor = new HashMap<>();
        Map<TopicPartition, Long> ends = new HashMap<>();
        Set<String> topics = new HashSet<>();
        final List<Long> timeQueries = new ArrayList<>();
        int topicListings;
        RuntimeException failure;
        /** Runs once, after the next offset lookup (by time or end) has taken its answer: records appended then. */
        Runnable afterNextOffsetCall;
        /** Thrown by end offset lookups only. */
        RuntimeException endFailure;

        @Override
        public Map<TopicPartition, Long> committed(String groupId) {
            fail();
            assertEquals("connect-fanin", groupId);
            committedCalls++;
            return committed;
        }

        @Override
        public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committedWithMetadata(
            String groupId) throws Exception {
            Map<TopicPartition, Long> offsets = committed(groupId);
            if (offsets == null) {
                return null;
            }
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> r = new HashMap<>();
            offsets.forEach((tp, o) -> r.put(tp,
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(o, metadata.getOrDefault(tp, ""))));
            return r;
        }

        /** Stores what preCommit returns, as the framework commits it. */
        void commit(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
            offsets.forEach((tp, om) -> {
                committed.put(tp, om.offset());
                metadata.put(tp, om.metadata());
            });
        }

        @Override
        public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> partitions, long timestampMs) {
            fail();
            timeQueries.add(timestampMs);
            Map<TopicPartition, Long> r = new HashMap<>();
            Map<TopicPartition, Long> forTime = atTimeFor.getOrDefault(timestampMs, Map.of());
            partitions.forEach(tp -> r.put(tp, forTime.containsKey(tp) ? forTime.get(tp) : atTime.getOrDefault(tp, -1L)));
            afterOffsetCall();
            return r;
        }

        /** Sets the first offset at or after a timestamp for one partition. */
        void at(long timestampMs, TopicPartition tp, long offset) {
            atTimeFor.computeIfAbsent(timestampMs, k -> new HashMap<>()).put(tp, offset);
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            fail();
            if (endFailure != null) {
                throw endFailure;
            }
            Map<TopicPartition, Long> r = new HashMap<>();
            partitions.forEach(tp -> r.put(tp, ends.getOrDefault(tp, 0L)));
            afterOffsetCall();
            return r;
        }

        private void afterOffsetCall() {
            Runnable r = afterNextOffsetCall;
            afterNextOffsetCall = null;
            if (r != null) {
                r.run();
            }
        }

        @Override
        public Set<String> topicNames() {
            fail();
            topicListings++;
            return Set.copyOf(topics);
        }

        private void fail() {
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public void close() {
        }
    }

    static class TestTask extends FanInSinkTask {
        final Lookup lookup = new Lookup();
        final MockProducer<byte[], byte[]> producer;
        final MockConsumer<byte[], byte[]> controlConsumer = new MockConsumer<>("earliest");
        final List<String> warnings = new ArrayList<>();
        long now = 1_000_000L;
        /** What the reader reports as caught-up time; null means "now" (never behind). */
        Long readerCaughtUpAt;
        boolean readerCreated;
        boolean background;
        ControlReader reader;

        TestTask() {
            Node node = new Node(0, "localhost", 9092);
            List<PartitionInfo> infos = List.of(new PartitionInfo(TARGET, 0, node, new Node[] {node},
                new Node[] {node}));
            producer = new MockProducer<>(new Cluster("t", List.of(node), infos, Set.of(), Set.of()), true, null,
                new ByteArraySerializer(), new ByteArraySerializer());
            controlConsumer.updatePartitions(CTL, List.of(new PartitionInfo(CTL, 0, null, null, null)));
            controlConsumer.updateBeginningOffsets(Map.of(CTL0, 0L));
            controlConsumer.updateEndOffsets(Map.of(CTL0, 0L));
            clock = () -> now;
        }

        /** Control records that are on the topic before the task starts: {name, value or null, time}. */
        void initialControl(Object[]... records) {
            controlConsumer.updateEndOffsets(Map.of(CTL0, (long) records.length));
            controlConsumer.schedulePollTask(() -> {
                long offset = 0;
                for (Object[] r : records) {
                    byte[] k = ((String) r[0]).getBytes(StandardCharsets.UTF_8);
                    byte[] v = r[1] == null ? null : ((String) r[1]).getBytes(StandardCharsets.UTF_8);
                    controlConsumer.addRecord(new ConsumerRecord<>(CTL, 0, offset++, (Long) r[2],
                        TimestampType.CREATE_TIME, k.length, v == null ? -1 : v.length, k, v, new RecordHeaders(),
                        java.util.Optional.empty()));
                }
            });
        }

        @Override
        Producer<byte[], byte[]> createProducer(Map<String, Object> producerConfigs) {
            return producer;
        }

        @Override
        StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
            return lookup;
        }

        @Override
        ControlReader createControlReader(String topic, ControlState state) {
            readerCreated = true;
            reader = new ControlReader(controlConsumer, topic, state, clock) {
                @Override
                void startBackground() {
                    if (background) {
                        super.startBackground();
                    }
                    // otherwise the tests change the state directly
                }

                @Override
                long caughtUpAt() {
                    if (background) {
                        return super.caughtUpAt();
                    }
                    return readerCaughtUpAt != null ? readerCaughtUpAt : now;
                }
            };
            return reader;
        }

        @Override
        void warn(String message) {
            warnings.add(message);
        }
    }

    private static Map<String, String> props(String... extra) {
        Map<String, String> p = new HashMap<>();
        p.put("name", "fanin");
        p.put("topics.regex", "^in\\..+");
        p.put(FanInSinkConfig.TARGET_TOPIC, TARGET);
        p.put(FanInSinkConfig.TARGET_BOOTSTRAP_SERVERS, "localhost:9092");
        p.put(FanInSinkConfig.TARGET_API_KEY, "key");
        p.put(FanInSinkConfig.TARGET_API_SECRET, "secret");
        // These tests were written for LATEST, the default before EARLIEST. A test of EARLIEST overrides it.
        p.put(FanInSinkConfig.CONTROL_ADD_START, "LATEST");
        for (int i = 0; i < extra.length; i += 2) {
            p.put(extra[i], extra[i + 1]);
        }
        return p;
    }

    private static Object[] add(String topic, long time) {
        return new Object[] {topic, "1", time};
    }

    private static Object[] remove(String topic, long time) {
        return new Object[] {topic, null, time};
    }

    private static TestTask started(Ctx ctx, Object[][] control, String... extra) {
        TestTask task = new TestTask();
        task.initialControl(control);
        task.initialize(ctx.context());
        String[] all = new String[extra.length + 2];
        all[0] = FanInSinkConfig.CONTROL_TOPIC;
        all[1] = CTL;
        System.arraycopy(extra, 0, all, 2, extra.length);
        task.start(props(all));
        return task;
    }

    /** Assigns partitions the way the framework does: assignment first, then open(). */
    private static void assign(TestTask task, Ctx ctx, TopicPartition... tps) {
        ctx.assignment.addAll(List.of(tps));
        task.open(List.of(tps));
    }

    /** A rebalance that revokes the partitions and assigns them again; the framework keeps no pause of its own. */
    private static void rebalance(TestTask task, Ctx ctx, TopicPartition... tps) {
        task.close(List.of(tps));
        ctx.paused.removeAll(List.of(tps));
        task.open(List.of(tps));
    }

    private static SinkRecord rec(TopicPartition tp, long offset) {
        return new SinkRecord(tp.topic(), tp.partition(), Schema.OPTIONAL_BYTES_SCHEMA, null,
            Schema.OPTIONAL_BYTES_SCHEMA, ("v" + offset).getBytes(StandardCharsets.UTF_8), offset, 5L,
            TimestampType.CREATE_TIME);
    }

    private static List<String> forwardedTopics(TestTask task) {
        List<String> topics = new ArrayList<>();
        task.producer.history().forEach(r -> topics.add(
            new String(r.headers().lastHeader(FanInSinkTask.HEADER_TOPIC).value(), StandardCharsets.UTF_8)));
        return topics;
    }

    // ---- feature off ----

    @Test
    void featureOff_noPauseNoFilter() {
        Ctx ctx = new Ctx();
        TestTask task = new TestTask();
        task.initialize(ctx.context());
        task.start(props());

        assign(task, ctx, A0, B0);
        task.put(List.of(rec(A0, 1), rec(B0, 1)));

        assertTrue(ctx.paused.isEmpty());
        assertTrue(ctx.seeks.isEmpty());
        assertEquals(List.of(A, B), forwardedTopics(task));
        assertTrue(!task.readerCreated, "no control reader without fanin.control.topic");
    }

    // ---- open ----

    @Test
    void open_pausesNotAllowed() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 10L);
        task.lookup.atTime.put(A0, 3L);

        assign(task, ctx, A0, B0);

        assertEquals(Set.of(B0), ctx.paused);
        assertTrue(ctx.seeks.isEmpty(), "A0 committed at 10 is already past its add time offset 3");
    }

    @Test
    void open_firstAssignmentStartsAnAllowedTopicAtItsAddTime() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.atTime.put(A0, 7L); // no committed offset yet

        assign(task, ctx, A0);

        assertEquals(Map.of(A0, 7L), ctx.seekedTo());
        assertEquals(List.of(100L), task.lookup.timeQueries);
    }

    // ---- put ----

    @Test
    void put_dropsRecordsOfRemovedTopic() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100), add(B, 100)});
        task.lookup.committed.putAll(Map.of(A0, 0L, B0, 0L));
        assign(task, ctx, A0, B0);
        task.lookup.at(200, A0, 1L); // the remove cut: offsets 1.. were written at or after the remove time

        task.controlState().apply(A, false, 200);
        task.put(List.of(rec(A0, 1), rec(B0, 1), rec(A0, 2)));

        assertEquals(List.of(B), forwardedTopics(task));
        assertEquals(Set.of(A0), ctx.paused);
    }

    @Test
    void add_resumesAndSeeksToEnd() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {});
        assign(task, ctx, A0, A1);
        assertEquals(Set.of(A0, A1), ctx.paused);
        task.lookup.ends.putAll(Map.of(A0, 40L, A1, 41L)); // no record at or after the add time

        task.controlState().apply(A, true, 500);
        task.put(List.of());

        assertEquals(Map.of(A0, 40L, A1, 41L), ctx.seekedTo());
        assertTrue(ctx.paused.isEmpty());
        assertEquals(List.of(500L), task.lookup.timeQueries);

        task.put(List.of(rec(A0, 40)));
        assertEquals(List.of(A), forwardedTopics(task));
    }

    @Test
    void add_seeksToTheFirstRecordAtOrAfterTheAddTime() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {});
        assign(task, ctx, A0);
        task.lookup.atTime.put(A0, 12L);
        task.lookup.ends.put(A0, 20L);

        task.controlState().apply(A, true, 500);
        task.put(List.of());

        assertEquals(Map.of(A0, 12L), ctx.seekedTo());
    }

    @Test
    void addEarliest_resumesWithoutSeek() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {}, FanInSinkConfig.CONTROL_ADD_START, "EARLIEST");
        assign(task, ctx, A0);

        task.controlState().apply(A, true, 500);
        task.put(List.of());

        assertTrue(ctx.paused.isEmpty());
        assertEquals(List.of(A0), ctx.resumes);
        assertTrue(ctx.seeks.isEmpty());
    }

    @Test
    void addStartNotSet_firstAddKeepsTheCommittedPosition_readdCuts() {
        Ctx ctx = new Ctx();
        TestTask task = new TestTask();
        task.initialControl(new Object[][] {});
        task.initialize(ctx.context());
        Map<String, String> p = props(FanInSinkConfig.CONTROL_TOPIC, CTL);
        p.remove(FanInSinkConfig.CONTROL_ADD_START); // the default, EARLIEST
        task.start(p);
        task.lookup.committed.put(A0, 5L);
        assign(task, ctx, A0);

        task.controlState().apply(A, true, 500);
        task.put(List.of());
        assertTrue(ctx.seeks.isEmpty(), "the first add keeps the committed position 5");
        assertEquals(List.of(A0), ctx.resumes);

        task.controlState().apply(A, false, 600);
        task.put(List.of());
        task.lookup.ends.put(A0, 90L);
        task.controlState().apply(A, true, 700);
        task.put(List.of());
        assertEquals(Map.of(A0, 90L), ctx.seekedTo(), "a re-add starts at its add time, also by default");
    }

    @Test
    void readd_startsAtEnd() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)}, FanInSinkConfig.CONTROL_ADD_START, "EARLIEST");
        task.lookup.committed.put(A0, 5L);
        assign(task, ctx, A0);
        assertTrue(ctx.seeks.isEmpty(), "EARLIEST: a first add keeps the committed position");

        task.controlState().apply(A, false, 200);
        task.put(List.of());
        assertEquals(Set.of(A0), ctx.paused);

        task.lookup.ends.put(A0, 90L);
        task.controlState().apply(A, true, 300);
        task.put(List.of());

        assertEquals(Map.of(A0, 90L), ctx.seekedTo(), "a re-added topic starts at its end, not at offset 5");
        assertTrue(ctx.paused.isEmpty());
    }

    @Test
    void readdInOneInterval_cutsATopicThatWasNeverPaused() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)}, FanInSinkConfig.CONTROL_ADD_START, "EARLIEST");
        task.lookup.committed.put(A0, 5L);
        assign(task, ctx, A0);

        task.controlState().apply(A, false, 200);
        task.controlState().apply(A, true, 300);
        task.lookup.atTime.put(A0, 60L);
        task.lookup.at(200, A0, 6L); // offset 6 was written after the remove
        task.put(List.of(rec(A0, 6)));

        assertEquals(Map.of(A0, 60L), ctx.seekedTo());
        assertEquals(List.of(200L, 300L), task.lookup.timeQueries, "the remove cut, then the add cut are looked up");
        assertTrue(ctx.paused.isEmpty());
        assertEquals(List.of(), forwardedTopics(task), "offset 6 was read before the cut at 60 and is dropped");
        task.put(List.of(rec(A0, 60)));
        assertEquals(List.of(A), forwardedTopics(task));
    }

    // ---- rebalance ----

    @Test
    void rebalanceAfterRemove_staysPaused() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 10L);
        assign(task, ctx, A0);
        task.controlState().apply(A, false, 200);
        task.put(List.of());
        assertEquals(Set.of(A0), ctx.paused);

        rebalance(task, ctx, A0);

        assertEquals(Set.of(A0), ctx.paused);
        task.put(List.of(rec(A0, 11)));
        assertEquals(List.of(), forwardedTopics(task));
    }

    @Test
    void removeSeenFirstInOpen_isPausedAndDropped() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 10L);
        assign(task, ctx, A0);

        task.controlState().apply(A, false, 200); // not yet drained by put
        rebalance(task, ctx, A0);
        task.put(List.of(rec(A0, 11)));

        assertEquals(Set.of(A0), ctx.paused);
        assertEquals(List.of(), forwardedTopics(task));
    }

    @Test
    void addThenRebalance_notPaused() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {});
        assign(task, ctx, A0);
        task.lookup.atTime.put(A0, 30L);
        task.controlState().apply(A, true, 500);
        task.put(List.of());
        assertTrue(ctx.paused.isEmpty());

        task.lookup.committed.put(A0, 35L); // the framework committed after the seek
        ctx.seeks.clear();
        rebalance(task, ctx, A0);

        assertTrue(ctx.paused.isEmpty());
        assertTrue(ctx.seeks.isEmpty(), "a committed offset past the cut is kept");
    }

    @Test
    void rebalanceBeforeCommit_keepsTheCut() {
        // The framework does not commit a seek on its own. A rebalance before the next commit would resume the
        // partition at its old committed offset, which is before the add time.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {});
        task.lookup.committed.put(A0, 3L);
        assign(task, ctx, A0);
        task.lookup.atTime.put(A0, 30L);
        task.controlState().apply(A, true, 500);
        task.put(List.of());
        ctx.seeks.clear();

        rebalance(task, ctx, A0);

        assertEquals(Map.of(A0, 30L), ctx.seekedTo());
    }

    // ---- start timestamp together with the control topic ----

    @Test
    void startTimestamp_andControl_bothApplied() {
        Ctx ctx = new Ctx();
        TestTask task = new TestTask();
        task.initialControl(add(A, 100));
        task.initialize(ctx.context());
        task.start(props(FanInSinkConfig.CONTROL_TOPIC, CTL, FanInSinkConfig.START_TIMESTAMP, "50"));
        // offsetsAtOrAfter is asked for 50 (start timestamp) and 100 (add time); the fake gives the same answer, so
        // use the end offset path to tell them apart: A0 has a record after 50 only.
        task.lookup.atTime.put(A0, 20L);

        assign(task, ctx, A0, A1, B0);

        Map<TopicPartition, Long> seeks = ctx.seekedTo();
        assertEquals(20L, seeks.get(A0));
        assertEquals(0L, seeks.get(A1), "no record after the times: end offset");
        assertEquals(Set.of(B0), ctx.paused);
        assertTrue(task.lookup.timeQueries.containsAll(List.of(50L, 100L)), task.lookup.timeQueries.toString());
    }

    @Test
    void startTimestampAfterTheAddTime_wins() {
        Ctx ctx = new Ctx();
        TestTask task = new TestTask() {
            @Override
            StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
                return new Lookup() {
                    @Override
                    public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> p, long ts) {
                        Map<TopicPartition, Long> r = new HashMap<>();
                        p.forEach(tp -> r.put(tp, ts == 900L ? 80L : 10L));
                        return r;
                    }
                };
            }
        };
        task.initialControl(add(A, 100));
        task.initialize(ctx.context());
        task.start(props(FanInSinkConfig.CONTROL_TOPIC, CTL, FanInSinkConfig.START_TIMESTAMP, "900"));

        assign(task, ctx, A0);

        assertEquals(Map.of(A0, 80L), ctx.seekedTo(), "the later of the two positions wins");
    }

    // ---- failures ----

    @Test
    void cutLookupFails_staysPausedAndRetries() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {});
        assign(task, ctx, A0);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("broker away");

        task.controlState().apply(A, true, 500);
        task.put(List.of());
        assertEquals(Set.of(A0), ctx.paused, "no resume without a cut");

        task.lookup.failure = null;
        task.lookup.ends.put(A0, 9L);
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(Map.of(A0, 9L), ctx.seekedTo());
        assertTrue(ctx.paused.isEmpty());
    }

    @Test
    void cutLookupFailsInOpen_pausesAndRetriesInPut() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("broker away");

        assign(task, ctx, A0);
        assertEquals(Set.of(A0), ctx.paused);

        task.lookup.failure = null;
        task.lookup.atTime.put(A0, 4L);
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(Map.of(A0, 4L), ctx.seekedTo());
        assertTrue(ctx.paused.isEmpty());
    }

    @Test
    void readerFailure_failsTheTask() throws Exception {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {});
        task.controlConsumer.setPollException(new org.apache.kafka.common.errors.AuthorizationException("no READ"));
        task.background = true;
        task.reader.startBackground();
        long deadline = System.currentTimeMillis() + 5_000;
        while (task.reader.failure() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        assertThrows(ConnectException.class, () -> task.put(List.of()));
    }

    @Test
    void missingControlTopic_failsStart() {
        Ctx ctx = new Ctx();
        TestTask task = new TestTask();
        task.controlConsumer.updatePartitions(CTL, List.of());
        task.initialize(ctx.context());
        assertThrows(ConnectException.class, () -> task.start(props(FanInSinkConfig.CONTROL_TOPIC, CTL,
            FanInSinkConfig.CONTROL_LOAD_TIMEOUT_MS, "100")));
        assertTrue(task.controlConsumer.closed(), "the control consumer is closed when start fails");
    }

    @Test
    void stop_closesTheReader() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {});
        task.stop();
        assertTrue(task.controlConsumer.closed());
    }

    // ---- warnings ----

    @Test
    void missingTopic_warnsOnceAfterThreshold() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {}, FanInSinkConfig.CONTROL_MISSING_WARN_MS, "120000");
        long t0 = task.now;
        task.lookup.topics.add(A);
        task.controlState().apply("in.typo", true, t0);

        task.put(List.of());                    // t0: too young
        task.now = t0 + 60_000;
        task.put(List.of());                    // 1 min: too young
        assertEquals(List.of(), task.warnings);
        task.now = t0 + 120_000;
        task.put(List.of());                    // 2 min: warn
        task.now = t0 + 180_000;
        task.put(List.of());                    // no second warning
        task.now = t0 + 200_000;
        task.put(List.of());                    // inside the check interval: no topic listing

        assertEquals(List.of("Control topic lists in.typo but no such topic exists for 2 min"), task.warnings);
        assertEquals(1, task.lookup.topicListings, "topics are listed only when a name is old enough to warn");

        // removed and added again: a new warning after the threshold
        task.controlState().apply("in.typo", false, t0 + 200_000);
        task.put(List.of());
        task.controlState().apply("in.typo", true, t0 + 240_000);
        task.now = t0 + 400_000;
        task.put(List.of());
        assertEquals(2, task.warnings.size(), task.warnings.toString());
    }

    @Test
    void existingTopic_noWarning() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 0)}, FanInSinkConfig.CONTROL_MISSING_WARN_MS, "0");
        task.lookup.topics.add(A);
        task.put(List.of());
        task.now += 120_000;
        task.put(List.of());
        assertEquals(List.of(), task.warnings);
    }

    @Test
    void onlyTaskZeroWarns() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add("in.typo", 0)}, FanInSinkConfig.CONTROL_MISSING_WARN_MS,
            "0", FanInSinkConnector.TASK_INDEX, "1");
        task.put(List.of());
        task.now += 120_000;
        task.put(List.of());
        assertEquals(List.of(), task.warnings);
        assertEquals(0, task.lookup.topicListings);
    }

    @Test
    void nameOutsideTheSubscription_warnsOnAdd() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add("other.x", 0)});
        task.controlState().apply("other.y", true, 10);
        task.put(List.of());

        assertEquals(List.of(
            "Control topic lists other.x but it does not match the subscription of this connector",
            "Control topic lists other.y but it does not match the subscription of this connector"),
            task.warnings);
    }

    @Test
    void loadTimeoutIsUsed() {
        Ctx ctx = new Ctx();
        TestTask task = new TestTask();
        task.controlConsumer.updateEndOffsets(Map.of(CTL0, 3L)); // records never arrive
        task.initialize(ctx.context());
        long t0 = System.nanoTime();
        assertThrows(org.apache.kafka.connect.errors.RetriableException.class, () -> task.start(props(
            FanInSinkConfig.CONTROL_TOPIC, CTL, FanInSinkConfig.CONTROL_LOAD_TIMEOUT_MS, "150")));
        assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() < 5_000);
    }

    // ---- lookup retry, compacted adds, remove and add in one interval ----

    /** A lookup that answers offsetsAtOrAfter by timestamp. */
    static class TimeLookup extends Lookup {
        final Map<Long, Long> offsetForTime = new HashMap<>();
        @Override
        public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> partitions, long ts) {
            if (failure != null) { throw failure; }
            timeQueries.add(ts);
            Map<TopicPartition, Long> r = new HashMap<>();
            partitions.forEach(tp -> r.put(tp, offsetForTime.getOrDefault(ts, -1L)));
            return r;
        }
    }

    @Test
    void openLookupFails_retryMustNotRewindBelowCommitted() {
        // Restart with LATEST: A was added long ago (time 100) and forwarded up to committed offset 500.
        // The Admin lookup fails once in open(). The retry in put() uses onlyIfBehind=false.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 500L);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("broker away");
        assign(task, ctx, A0);

        task.lookup.failure = null;
        task.lookup.atTime.put(A0, 4L); // first record at/after the add time
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());

        Long seek = ctx.seekedTo().get(A0);
        assertTrue(seek == null || seek >= 500L,
            "partition was seeked back from committed 500 to " + seek + ": offsets " + seek + "..499 are forwarded again");
    }

    @Test
    void compactedRedundantAdd_mustNotMoveTheCutForward() {
        // History: add A at 100, later a redundant add A at 900 (ignored in memory). Compaction keeps only the
        // newest record per key, so a task that starts after compaction sees add A at 900 only.
        // The group is behind: committed 10. Records 10..79 were written while A was allowed (time < 900).
        Ctx ctx = new Ctx();
        TestTask task = new TestTask() {
            final TimeLookup tl = new TimeLookup();
            @Override
            StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
                tl.committed.put(A0, 10L);
                // the task that committed offset 10 applied the add at 100 and wrote the ledger
                tl.metadata.put(A0, ControlLedger.allowedAt(100).encode());
                tl.offsetForTime.put(100L, 5L);
                tl.offsetForTime.put(900L, 80L);
                return tl;
            }
        };
        task.initialControl(add(A, 900)); // compacted: only the last add survives
        task.initialize(ctx.context());
        task.start(props(FanInSinkConfig.CONTROL_TOPIC, CTL));

        assign(task, ctx, A0);

        assertTrue(ctx.seekedTo().isEmpty(),
            "the uncompacted log gives no seek (cut 5 < committed 10); the compacted log gives " + ctx.seekedTo()
                + ", so offsets 10..79 of an allowed topic are skipped");
    }

    @Test
    void removeThenAddInOneInterval_seeksBackOverForwardedRecords() {
        // LATEST. A allowed, forwarded up to offset 40. The reader saw remove@200 and add@300 before the task drained.
        // Records 30..40 (time >= 300) were already forwarded in earlier batches.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 41L);
        assign(task, ctx, A0);
        task.put(List.of(rec(A0, 40)));

        task.controlState().apply(A, false, 200);
        task.controlState().apply(A, true, 300);
        task.lookup.atTime.put(A0, 30L);
        task.put(List.of());

        Long seek = ctx.seekedTo().get(A0);
        assertTrue(seek == null || seek >= 41L, "seek back to " + seek + " re-forwards offsets " + seek + "..40");
    }

    // ---- ledger in the committed offset metadata, reader lag, deferred resume, lookup budget ----

    private static org.apache.kafka.clients.consumer.OffsetAndMetadata om(long offset) {
        return new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset);
    }

    private static String meta(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> m,
                               TopicPartition tp) {
        return m.get(tp).metadata();
    }

    @Test
    void preCommit_writesAllowedAndRemovedLedger() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.committed.putAll(Map.of(A0, 5L, B0, 5L));
        assign(task, ctx, A0, B0);

        var first = task.preCommit(Map.of(A0, om(7), B0, om(5)));
        assertEquals(ControlLedger.allowedAt(100).encode(), meta(first, A0));
        assertEquals("", meta(first, B0), "a topic that was never on the list gets no ledger");

        task.lookup.at(200, A0, 7L);
        task.controlState().apply(A, false, 200);
        task.put(List.of());
        var second = task.preCommit(Map.of(A0, om(7), B0, om(5)));
        assertEquals(ControlLedger.removedAt(200, 7).encode(), meta(second, A0), "the ledger carries the remove cut");
        assertEquals(7L, second.get(A0).offset());
    }

    @Test
    void preCommit_beforeTheFrameworkAppliedTheSeek_keepsTheOldLedgerAndOffset() {
        // The framework commits at the start of the next iteration, before it applies the seek of put().
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.atTime.put(A0, 3L);
        assign(task, ctx, A0);
        task.controlState().apply(A, false, 200);
        task.put(List.of());
        task.lookup.atTime.put(A0, 60L);
        task.controlState().apply(A, true, 300);
        task.put(List.of());
        assertEquals(Map.of(A0, 60L), ctx.seekedTo());

        var before = task.preCommit(Map.of(A0, om(8)));
        assertEquals(ControlLedger.removedAt(200).encode(), meta(before, A0), "the cut is not applied yet");

        task.put(List.of()); // the framework applied the seek before this put
        var after = task.preCommit(Map.of(A0, om(60)));
        assertEquals(ControlLedger.allowedAt(300).encode(), meta(after, A0));
    }

    @Test
    void preCommit_issuedSeekBackward_commitsNotPastIt() {
        // A pending re-add dropped records 6..9; the seek goes back to 6. A commit before the framework applies the
        // seek must not pass offset 6.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.atTime.put(A0, 3L);
        assign(task, ctx, A0);
        task.controlState().apply(A, false, 200);
        task.controlState().apply(A, true, 300);
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("admin slow");
        task.put(List.of(rec(A0, 6), rec(A0, 7), rec(A0, 8), rec(A0, 9)));
        assertEquals(List.of(), forwardedTopics(task));
        assertEquals(6L, task.preCommit(Map.of(A0, om(10))).get(A0).offset(), "held at the first dropped record");

        task.lookup.failure = null;
        task.lookup.atTime.put(A0, 6L); // records 6.. were written after the add
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(Map.of(A0, 6L), ctx.seekedTo());
        assertEquals(6L, task.preCommit(Map.of(A0, om(10))).get(A0).offset());
    }

    @Test
    void preCommit_ledgerNotKnown_commitsNothing() {
        // A commit would write empty metadata and remove the committed ledger.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.lookup.failure = new org.apache.kafka.common.errors.TimeoutException("admin slow");
        assign(task, ctx, A0);
        assertEquals(Map.of(), task.preCommit(Map.of(A0, om(5))));

        task.lookup.failure = null;
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.now += ControlCuts.CONTROL_RETRY_MS;
        task.put(List.of());
        assertEquals(ControlLedger.allowedAt(100).encode(), meta(task.preCommit(Map.of(A0, om(5))), A0));
        assertTrue(ctx.seeks.isEmpty());
    }

    @Test
    void restart_removedLedger_cutsAlsoWithEarliestAfterTheTombstoneIsGone() {
        // The remove tombstone was deleted by compaction and the topic was added again. The ledger still shows the
        // remove, so the re-add rule applies.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 300)}, FanInSinkConfig.CONTROL_ADD_START, "EARLIEST");
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.removedAt(200).encode());
        task.lookup.atTime.put(A0, 60L);

        assign(task, ctx, A0);

        assertEquals(Map.of(A0, 60L), ctx.seekedTo());
    }

    @Test
    void restart_allowedLedger_butARemoveAfterIt_cuts() {
        // The control topic still shows a remove at 200 after the add at 100 that the ledger records. Records before
        // the remove cut (offset 30) are forwarded, then the partition moves to the add cut (offset 60).
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100), remove(A, 200), add(A, 300)});
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        task.lookup.atTime.put(A0, 60L);
        task.lookup.at(200, A0, 30L);

        assign(task, ctx, A0);
        assertTrue(ctx.seeks.isEmpty(), "the committed offset 5 is before the remove cut 30: " + ctx.seeks);

        task.put(List.of(rec(A0, 29), rec(A0, 30)));
        assertEquals(List.of(A), forwardedTopics(task), "offset 29 forwarded, offset 30 dropped");
        assertEquals(Map.of(A0, 60L), ctx.seekedTo());
    }

    @Test
    void notAllowed_withAllowedLedger_writesRemoved() {
        // The tombstone was compacted away: the topic is not on the list, but the ledger says allowed.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {});
        task.lookup.committed.put(A0, 5L);
        task.lookup.metadata.put(A0, ControlLedger.allowedAt(100).encode());
        assign(task, ctx, A0);

        assertEquals(ControlLedger.removedAt(100).encode(), meta(task.preCommit(Map.of(A0, om(5))), A0));
    }

    @Test
    void readerBehind_dropsAllAndPausesAll_thenResumesAndReadsAgain() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)}, FanInSinkConfig.CONTROL_MAX_LAG_MS, "60000");
        task.lookup.committed.put(A0, 5L);
        assign(task, ctx, A0);
        task.put(List.of(rec(A0, 5)));
        assertEquals(List.of(A), forwardedTopics(task));

        task.readerCaughtUpAt = task.now;
        task.now += 60_001;
        task.put(List.of(rec(A0, 6), rec(A0, 7)));
        assertEquals(List.of(A), forwardedTopics(task), "nothing forwarded while the reader is behind");
        assertEquals(Set.of(A0), ctx.paused);
        assertEquals(6L, task.preCommit(Map.of(A0, om(8))).get(A0).offset(), "dropped records are not committed");

        task.readerCaughtUpAt = null; // caught up
        task.put(List.of());
        assertTrue(ctx.paused.isEmpty());
        assertEquals(Map.of(A0, 6L), ctx.seekedTo(), "records dropped while behind are read again");
        task.put(List.of(rec(A0, 6), rec(A0, 7)));
        assertEquals(List.of(A, A, A), forwardedTopics(task));
    }

    @Test
    void readerBehind_newPartitionsArePausedInOpen() {
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100)});
        task.readerCaughtUpAt = task.now;
        task.now += 60_001;
        task.put(List.of());
        assign(task, ctx, A0);
        assertEquals(Set.of(A0), ctx.paused);
    }

    @Test
    void resume_waitsUntilPutCompletes() {
        // A resume during a redelivery lets the consumer fetch new records into the batch.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {}, FanInSinkConfig.CONTROL_ADD_START, "EARLIEST");
        task.lookup.committed.put(B0, 0L);
        assign(task, ctx, A0, B0);
        task.controlState().apply(B, true, 100);
        task.put(List.of());
        task.controlState().apply(A, true, 500);
        task.producer.sendException = new org.apache.kafka.common.errors.TimeoutException("metadata");
        assertThrows(org.apache.kafka.connect.errors.RetriableException.class, () -> task.put(List.of(rec(B0, 1))));
        assertTrue(ctx.paused.contains(A0), "no resume in a put() that failed");

        task.producer.sendException = null;
        task.put(List.of(rec(B0, 1)));
        assertTrue(ctx.paused.isEmpty());
    }

    @Test
    void open_oneBatchOfLookups() {
        // One committed lookup and one offsets-for-times call for all partitions.
        Ctx ctx = new Ctx();
        TestTask task = started(ctx, new Object[][] {add(A, 100), add(B, 200)});
        task.lookup.atTime.putAll(Map.of(A0, 1L, A1, 2L, B0, 3L));
        assign(task, ctx, A0, A1, B0);
        assertEquals(1, task.lookup.committedCalls);
        assertEquals(Map.of(A0, 1L, A1, 2L, B0, 3L), ctx.seekedTo());
    }

    @Test
    void open_lookupsOverTheTimeBudget_pauseAndRetry() {
        Ctx ctx = new Ctx();
        TestTask task = new TestTask() {
            @Override
            StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
                return new Lookup() {
                    @Override
                    public Map<TopicPartition, Long> committed(String groupId) {
                        now += ControlCuts.LOOKUP_BUDGET_MS + 1; // a slow broker
                        return Map.of();
                    }
                };
            }
        };
        task.initialControl(add(A, 100));
        task.initialize(ctx.context());
        task.start(props(FanInSinkConfig.CONTROL_TOPIC, CTL));

        assign(task, ctx, A0);

        assertEquals(Set.of(A0), ctx.paused);
        assertTrue(ctx.seeks.isEmpty());
    }
}
