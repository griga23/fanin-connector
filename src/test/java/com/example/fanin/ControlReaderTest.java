package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;

/** Reading the control topic with a MockConsumer. */
class ControlReaderTest {

    private static final String TOPIC = "ctl";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);

    private static MockConsumer<byte[], byte[]> consumer(long end) {
        MockConsumer<byte[], byte[]> c = new MockConsumer<>("earliest");
        c.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null)));
        c.updateBeginningOffsets(Map.of(P0, 0L));
        c.updateEndOffsets(Map.of(P0, end));
        return c;
    }

    private static ConsumerRecord<byte[], byte[]> rec(long offset, String key, String value, long ts) {
        byte[] k = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        byte[] v = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>(TOPIC, 0, offset, ts, TimestampType.CREATE_TIME,
            k == null ? -1 : k.length, v == null ? -1 : v.length, k, v, new RecordHeaders(),
            java.util.Optional.empty());
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    @Test
    void reader_loadInitial_readsCompactedState() {
        MockConsumer<byte[], byte[]> c = consumer(3);
        c.schedulePollTask(() -> {
            c.addRecord(rec(0, "a", "1", 100));
            c.addRecord(rec(1, "b", "1", 200));
            c.addRecord(rec(2, "a", null, 300));
        });
        ControlState state = new ControlState();
        try (ControlReader reader = new ControlReader(c, TOPIC, state, () -> 999L)) {
            reader.loadInitial(Duration.ofSeconds(5));
            assertEquals(999L, reader.caughtUpAt(), "caught up as of the time the end offset was fetched");
        }
        assertEquals(Set.of("b"), state.allowed());
        assertEquals(Map.of("b", 200L), state.addedAt());
        assertEquals(Map.of("a", 300L), state.removedAt());
    }

    @Test
    void reader_loadInitial_emptyTopicIsDoneAtOnce() {
        MockConsumer<byte[], byte[]> c = consumer(0);
        ControlState state = new ControlState();
        try (ControlReader reader = new ControlReader(c, TOPIC, state, () -> 0L)) {
            reader.loadInitial(Duration.ofSeconds(5));
        }
        assertEquals(Set.of(), state.allowed());
    }

    @Test
    void reader_loadInitial_timeoutThrowsRetriable() {
        MockConsumer<byte[], byte[]> c = consumer(5); // 5 records announced, none arrive
        try (ControlReader reader = new ControlReader(c, TOPIC, new ControlState(), () -> 0L)) {
            assertThrows(RetriableException.class, () -> reader.loadInitial(Duration.ofMillis(200)));
        }
    }

    @Test
    void reader_loadInitial_missingTopicFails() {
        MockConsumer<byte[], byte[]> c = new MockConsumer<>("earliest");
        try (ControlReader reader = new ControlReader(c, TOPIC, new ControlState(), () -> 0L)) {
            ConnectException e = assertThrows(ConnectException.class,
                () -> reader.loadInitial(Duration.ofMillis(200)));
            assertTrue(e.getMessage().contains(TOPIC), e.getMessage());
        }
    }

    /** With two partitions the order of changes across partitions is poll order. */
    @Test
    void reader_loadInitial_twoPartitionsFail() {
        MockConsumer<byte[], byte[]> c = consumer(0);
        c.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null),
            new PartitionInfo(TOPIC, 1, null, null, null)));
        try (ControlReader reader = new ControlReader(c, TOPIC, new ControlState(), () -> 0L)) {
            ConnectException e = assertThrows(ConnectException.class,
                () -> reader.loadInitial(Duration.ofSeconds(1)));
            assertTrue(e.getMessage().contains("exactly 1 partition"), e.getMessage());
        }
    }

    @Test
    void reader_nullKey_skipped() {
        MockConsumer<byte[], byte[]> c = consumer(2);
        c.schedulePollTask(() -> {
            c.addRecord(rec(0, null, "1", 100));
            c.addRecord(rec(1, "a", "1", 100));
        });
        ControlState state = new ControlState();
        try (ControlReader reader = new ControlReader(c, TOPIC, state, () -> 0L)) {
            reader.loadInitial(Duration.ofSeconds(5));
        }
        assertEquals(Set.of("a"), state.allowed());
    }

    /**
     * This replaces reader_recordWithoutTimestamp_usesClock: the reader's clock differs per
     * task and per restart, so the add time would differ too. Such a record is skipped.
     */
    @Test
    void reader_recordWithoutTimestamp_skipped() {
        MockConsumer<byte[], byte[]> c = consumer(2);
        c.schedulePollTask(() -> {
            c.addRecord(rec(0, "a", "1", -1));
            c.addRecord(rec(1, "b", "1", 100));
        });
        ControlState state = new ControlState();
        try (ControlReader reader = new ControlReader(c, TOPIC, state, () -> 777L)) {
            reader.loadInitial(Duration.ofSeconds(5));
        }
        assertEquals(Map.of("b", 100L), state.addedAt());
    }

    /** Compaction treats "a " and "a" as different keys, so the key is not trimmed. */
    @Test
    void reader_keyWithWhiteSpace_usedAsIs() {
        MockConsumer<byte[], byte[]> c = consumer(2);
        c.schedulePollTask(() -> {
            c.addRecord(rec(0, "a ", "1", 100));
            c.addRecord(rec(1, "a", null, 200));
        });
        ControlState state = new ControlState();
        try (ControlReader reader = new ControlReader(c, TOPIC, state, () -> 0L)) {
            reader.loadInitial(Duration.ofSeconds(5));
        }
        assertEquals(Set.of("a "), state.allowed(), "the tombstone of 'a' does not remove 'a '");
    }

    @Test
    void reader_background_appliesLaterRecordsAndStopsOnClose() throws Exception {
        MockConsumer<byte[], byte[]> c = consumer(0);
        ControlState state = new ControlState();
        ControlReader reader = new ControlReader(c, TOPIC, state, () -> 0L);
        reader.loadInitial(Duration.ofSeconds(5));
        c.schedulePollTask(() -> c.addRecord(rec(0, "z", "1", 500)));
        reader.startBackground();
        waitFor(() -> state.isAllowed("z"));
        assertTrue(state.isAllowed("z"));
        reader.close();
        assertTrue(c.closed(), "the background thread closes the consumer");
    }

    @Test
    void reader_background_authorizationFailureIsReported() throws Exception {
        MockConsumer<byte[], byte[]> c = consumer(0);
        ControlReader reader = new ControlReader(c, TOPIC, new ControlState(), () -> 0L);
        reader.loadInitial(Duration.ofSeconds(5));
        c.setPollException(new AuthorizationException("no READ on ctl"));
        reader.startBackground();
        waitFor(() -> reader.failure() != null);
        assertNotNull(reader.failure());
        reader.close();
    }

    /** A transient error must not stop the reader, because a failed task stays failed on CC. */
    @Test
    void reader_background_transientErrorIsRetried() throws Exception {
        MockConsumer<byte[], byte[]> c = consumer(0);
        ControlState state = new ControlState();
        ControlReader reader = new ControlReader(c, TOPIC, state, () -> 0L);
        reader.loadInitial(Duration.ofSeconds(5));
        c.setPollException(new KafkaException("broker not available"));
        c.schedulePollTask(() -> c.addRecord(rec(0, "z", "1", 500)));
        reader.startBackground();
        waitFor(() -> state.isAllowed("z"));
        assertTrue(state.isAllowed("z"), "the reader read on after the error");
        assertNull(reader.failure());
        reader.close();
    }

    /** The task needs to know how current the reader is. */
    @Test
    void reader_background_reportsCaughtUpTime() throws Exception {
        MockConsumer<byte[], byte[]> c = consumer(0);
        AtomicLong clock = new AtomicLong(1_000);
        ControlReader reader = new ControlReader(c, TOPIC, new ControlState(), clock::get);
        reader.loadInitial(Duration.ofSeconds(5));
        assertEquals(1_000L, reader.caughtUpAt());
        c.updateEndOffsets(Map.of(P0, 1L)); // a record the reader has not read yet
        clock.set(2_000);
        reader.startBackground();
        Thread.sleep(300);
        assertEquals(1_000L, reader.caughtUpAt(), "not caught up while a record is missing");

        c.schedulePollTask(() -> c.addRecord(rec(0, "z", "1", 1_500)));
        clock.set(2_000 + ControlReader.CHECK_MS);
        waitFor(() -> reader.caughtUpAt() > 1_000L);
        assertTrue(reader.caughtUpAt() >= 2_000L, "caught up again: " + reader.caughtUpAt());
        reader.close();
    }

    /** A partition added at run time would never be read. */
    @Test
    void reader_background_partitionAddedLater_fails() throws Exception {
        MockConsumer<byte[], byte[]> c = consumer(0);
        AtomicLong clock = new AtomicLong(0);
        ControlReader reader = new ControlReader(c, TOPIC, new ControlState(), clock::get);
        reader.loadInitial(Duration.ofSeconds(5));
        reader.startBackground();
        c.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null),
            new PartitionInfo(TOPIC, 1, null, null, null)));
        clock.set(ControlReader.PARTITION_CHECK_MS + 1);
        waitFor(() -> reader.failure() != null);
        assertInstanceOf(ControlReader.ControlTopicException.class, reader.failure());
        reader.close();
    }
}
