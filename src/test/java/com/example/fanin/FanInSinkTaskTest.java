package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class FanInSinkTaskTest {

    private static final String TARGET = "fanin-out";
    private static final int PARTITIONS = 6;

    /** Used only when a record carries no explicit partition (KEY_HASH mode). */
    static class FirstPartitioner implements org.apache.kafka.clients.producer.Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                             Cluster cluster) {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    /** MockProducer that counts flush() and close() calls. */
    static class CountingProducer extends MockProducer<byte[], byte[]> {
        int flushes;
        int closes;

        CountingProducer(Cluster cluster, boolean autoComplete) {
            super(cluster, autoComplete, new FirstPartitioner(), new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public void flush() {
            flushes++;
            super.flush();
        }

        @Override
        public void close(java.time.Duration timeout) {
            closes++;
            super.close(timeout);
        }
    }

    static class TestTask extends FanInSinkTask {
        final CountingProducer mock;

        TestTask(CountingProducer mock) {
            this.mock = mock;
        }

        @Override
        Producer<byte[], byte[]> createProducer(Map<String, Object> producerConfigs) {
            return mock;
        }
    }

    private static Cluster cluster() {
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> infos = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) {
            infos.add(new PartitionInfo(TARGET, i, node, new Node[] {node}, new Node[] {node}));
        }
        return new Cluster("test", List.of(node), infos, Set.of(), Set.of());
    }

    private static Map<String, String> props(String... extra) {
        Map<String, String> props = new HashMap<>();
        props.put("fanin.target.topic", TARGET);
        props.put("fanin.target.bootstrap.servers", "localhost:9092");
        props.put("fanin.target.api.key", "key");
        props.put("fanin.target.api.secret", "secret");
        props.put("topics.regex", "^pfx\\..+");
        for (int i = 0; i < extra.length; i += 2) {
            props.put(extra[i], extra[i + 1]);
        }
        return props;
    }

    private static TestTask started(boolean autoComplete, String... extra) {
        TestTask task = new TestTask(new CountingProducer(cluster(), autoComplete));
        task.start(props(extra));
        return task;
    }

    private static SinkRecord record(String topic, int partition, long offset, byte[] key, byte[] value) {
        return new SinkRecord(topic, partition, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA,
            value, offset, 1_700_000_000_000L, org.apache.kafka.common.record.TimestampType.CREATE_TIME);
    }

    private static String header(ProducerRecord<byte[], byte[]> r, String key) {
        Header h = r.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    // ---- config ----

    @Test
    void startFailsWhenRegexMatchesTarget() {
        TestTask task = new TestTask(new CountingProducer(cluster(), true));
        Map<String, String> props = props("topics.regex", "^fanin-.+");
        assertThrows(ConfigException.class, () -> task.start(props));
    }

    @Test
    void startFailsWithoutRequiredConfig() {
        TestTask task = new TestTask(new CountingProducer(cluster(), true));
        Map<String, String> props = props();
        props.remove("fanin.target.topic");
        assertThrows(ConfigException.class, () -> task.start(props));
    }

    // ---- headers ----

    @Test
    void headersModePassesKeyValueThroughAndAddsSourceHeaders() {
        TestTask task = started(true);
        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        byte[] value = {0, 1, 2, (byte) 0xff};
        task.put(List.of(record("pfx.a", 3, 42L, key, value)));

        ProducerRecord<byte[], byte[]> out = task.mock.history().get(0);
        assertEquals(TARGET, out.topic());
        assertArrayEquals(key, out.key());
        assertArrayEquals(value, out.value());
        assertEquals("pfx.a", header(out, "fanin.source.topic"));
        assertEquals("3", header(out, "fanin.source.partition"));
        assertEquals("42", header(out, "fanin.source.offset"));
        assertEquals("1700000000000", header(out, "fanin.source.timestamp"));
    }

    // ---- timestamp ----

    @Test
    void sourceTimestampIsKeptOnOutputRecord() {
        TestTask task = started(true);
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        assertEquals(1_700_000_000_000L, task.mock.history().get(0).timestamp(),
            "Flink reads the Kafka record timestamp as $rowtime, so the source time must be kept");
    }

    @Test
    void sourceTimestampIsKeptInJsonEnvelopeMode() {
        TestTask task = started(true, "fanin.output.mode", "JSON_ENVELOPE");
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        assertEquals(1_700_000_000_000L, task.mock.history().get(0).timestamp());
    }

    @Test
    void recordWithoutTimestampLetsProducerSetTheTime() {
        TestTask task = started(true);
        SinkRecord in = new SinkRecord("pfx.a", 0, Schema.OPTIONAL_BYTES_SCHEMA, null,
            Schema.OPTIONAL_BYTES_SCHEMA, new byte[] {1}, 7L, null,
            org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE);
        task.put(List.of(in));
        assertNull(task.mock.history().get(0).timestamp());
    }

    @Test
    void negativeTimestampIsNotPassedToProducer() {
        TestTask task = started(true);
        SinkRecord in = new SinkRecord("pfx.a", 0, Schema.OPTIONAL_BYTES_SCHEMA, null,
            Schema.OPTIONAL_BYTES_SCHEMA, new byte[] {1}, 7L, -5L,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME);
        task.put(List.of(in));
        assertNull(task.mock.history().get(0).timestamp(), "KafkaProducer rejects a negative timestamp");
    }

    // ---- timestamp mode ----

    @Test
    void timestampModeDefaultsToSource() {
        assertEquals(FanInSinkConfig.TimestampMode.SOURCE, new FanInSinkConfig(props()).timestampMode());
        TestTask task = started(true);
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        assertEquals(1_700_000_000_000L, task.mock.history().get(0).timestamp());
    }

    @Test
    void copyModeLetsTheProducerSetTheTime() {
        TestTask task = started(true, "fanin.timestamp.mode", "COPY");
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        ProducerRecord<byte[], byte[]> out = task.mock.history().get(0);
        assertNull(out.timestamp(), "COPY: no explicit timestamp, so the producer uses the time of the copy");
        assertEquals("1700000000000", header(out, "fanin.source.timestamp"),
            "the original time stays in the header in both modes");
    }

    @Test
    void copyModeAlsoAppliesInJsonEnvelopeMode() {
        TestTask task = started(true, "fanin.timestamp.mode", "COPY", "fanin.output.mode", "JSON_ENVELOPE");
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        assertNull(task.mock.history().get(0).timestamp());
    }

    @Test
    void timestampModeIsCaseInsensitive() {
        assertEquals(FanInSinkConfig.TimestampMode.COPY,
            new FanInSinkConfig(props("fanin.timestamp.mode", "copy")).timestampMode());
        assertEquals(FanInSinkConfig.TimestampMode.SOURCE,
            new FanInSinkConfig(props("fanin.timestamp.mode", "Source")).timestampMode());
    }

    @Test
    void anUnknownTimestampModeIsRejected() {
        assertThrows(ConfigException.class, () -> started(true, "fanin.timestamp.mode", "LATEST"));
    }

    @Test
    void outputTimestampFollowsTheModeAndIgnoresMissingOrNegativeSourceTimes() {
        FanInSinkConfig.TimestampMode source = FanInSinkConfig.TimestampMode.SOURCE;
        FanInSinkConfig.TimestampMode copy = FanInSinkConfig.TimestampMode.COPY;
        assertEquals(5L, FanInSinkTask.outputTimestamp(source, 5L));
        assertEquals(0L, FanInSinkTask.outputTimestamp(source, 0L));
        assertNull(FanInSinkTask.outputTimestamp(source, null));
        assertNull(FanInSinkTask.outputTimestamp(source, -1L));
        assertNull(FanInSinkTask.outputTimestamp(copy, 5L));
        assertNull(FanInSinkTask.outputTimestamp(copy, null));
    }

    @Test
    void existingHeadersArePreserved() {
        TestTask task = started(true);
        ConnectHeaders headers = new ConnectHeaders();
        headers.addBytes("trace-id", new byte[] {9, 8, 7});
        headers.addBytes("trace-id", new byte[] {6});
        headers.addString("kind", "event");
        SinkRecord in = new SinkRecord("pfx.a", 0, Schema.OPTIONAL_BYTES_SCHEMA, null,
            Schema.OPTIONAL_BYTES_SCHEMA, new byte[] {1}, 7L, null,
            org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE, headers);
        task.put(List.of(in));

        ProducerRecord<byte[], byte[]> out = task.mock.history().get(0);
        List<byte[]> traces = new ArrayList<>();
        out.headers().headers("trace-id").forEach(h -> traces.add(h.value()));
        assertEquals(2, traces.size());
        assertArrayEquals(new byte[] {9, 8, 7}, traces.get(0));
        assertArrayEquals(new byte[] {6}, traces.get(1));
        assertEquals("event", header(out, "kind"));
        assertEquals("pfx.a", header(out, "fanin.source.topic"));
        assertNull(out.headers().lastHeader("fanin.source.timestamp"), "no timestamp header without timestamp");
    }

    @Test
    void nullKeyAndTombstoneArePreservedInHeadersMode() {
        TestTask task = started(true);
        task.put(List.of(record("pfx.a", 0, 1L, null, null)));
        ProducerRecord<byte[], byte[]> out = task.mock.history().get(0);
        assertNull(out.key());
        assertNull(out.value());
    }

    @Test
    void nonByteArrayValueIsRejectedWithHint() {
        TestTask task = started(true);
        SinkRecord in = new SinkRecord("pfx.a", 0, null, null, Schema.STRING_SCHEMA, "text", 1L);
        DataException e = assertThrows(DataException.class, () -> task.put(List.of(in)));
        assertTrue(e.getMessage().contains("ByteArrayConverter"));
    }

    // ---- JSON envelope ----

    @Test
    void jsonEnvelopeContainsSourceMetadataAndBase64() {
        TestTask task = started(true, "fanin.output.mode", "JSON_ENVELOPE");
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        task.put(List.of(record("pfx.a", 2, 10L, key, value)));

        ProducerRecord<byte[], byte[]> out = task.mock.history().get(0);
        String json = new String(out.value(), StandardCharsets.UTF_8);
        assertEquals("{\"source_topic\":\"pfx.a\",\"source_partition\":2,\"source_offset\":10,"
            + "\"source_timestamp\":1700000000000,\"key\":\""
            + Base64.getEncoder().encodeToString(key) + "\",\"value\":\""
            + Base64.getEncoder().encodeToString(value) + "\"}", json);
        assertArrayEquals(key, out.key(), "key itself is kept as the record key");
        assertEquals("pfx.a", header(out, "fanin.source.topic"), "headers are kept too");
    }

    @Test
    void jsonEnvelopeWithNullKeyAndTombstone() {
        TestTask task = started(true, "fanin.output.mode", "JSON_ENVELOPE");
        task.put(List.of(record("pfx.a", 0, 5L, null, null)));
        String json = new String(task.mock.history().get(0).value(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"key\":null"), json);
        assertTrue(json.contains("\"value\":null"), json);
        assertNull(task.mock.history().get(0).key());
    }

    @Test
    void jsonEnvelopeEscapesTopicName() {
        byte[] json = FanInSinkTask.envelope("we\"ird\\t\nopic", 0, 0, null, null, null);
        String s = new String(json, StandardCharsets.UTF_8);
        assertTrue(s.contains("\"source_topic\":\"we\\\"ird\\\\t\\nopic\""), s);
        assertTrue(s.contains("\"source_timestamp\":null"), s);
    }

    // ---- partitioning ----

    @Test
    void sourcePartitionHashIsStableAndInRange() {
        TestTask task = started(true);
        for (int p = 0; p < 50; p++) {
            int expected = FanInSinkTask.selectPartition("pfx.a", p, PARTITIONS);
            assertTrue(expected >= 0 && expected < PARTITIONS);
            assertEquals(expected, FanInSinkTask.selectPartition("pfx.a", p, PARTITIONS));
        }
        task.put(List.of(record("pfx.a", 4, 1L, null, new byte[] {1}), record("pfx.a", 4, 2L, null, new byte[] {2})));
        Integer first = task.mock.history().get(0).partition();
        assertNotNull(first);
        assertEquals(first, task.mock.history().get(1).partition(), "same source partition, same target partition");
        assertEquals(FanInSinkTask.selectPartition("pfx.a", 4, PARTITIONS), first);
    }

    @Test
    void sourcePartitionHashSpreadsAcrossTargetPartitions() {
        Set<Integer> used = new HashSet<>();
        for (int t = 0; t < 100; t++) {
            used.add(FanInSinkTask.selectPartition("pfx." + t, 0, PARTITIONS));
        }
        assertEquals(PARTITIONS, used.size());
    }

    @Test
    void keyHashModeLeavesPartitionToProducer() {
        TestTask task = started(true, "fanin.partitioning.mode", "KEY_HASH");
        task.put(List.of(record("pfx.a", 1, 1L, "k".getBytes(StandardCharsets.UTF_8), new byte[] {1})));
        assertNull(task.mock.history().get(0).partition());
    }

    @Test
    void partitionCountIsCachedAndRefreshed() {
        long[] now = {0};
        int[] lookups = {0};
        CountingProducer mock = new CountingProducer(cluster(), true) {
            @Override
            public List<PartitionInfo> partitionsFor(String topic) {
                lookups[0]++;
                return super.partitionsFor(topic);
            }
        };
        TestTask task = new TestTask(mock);
        task.clock = () -> now[0];
        task.start(props());

        task.put(List.of(record("pfx.a", 0, 1L, null, null), record("pfx.a", 0, 2L, null, null)));
        assertEquals(1, lookups[0]);
        now[0] = FanInSinkTask.PARTITION_REFRESH_MS + 1;
        task.put(List.of(record("pfx.a", 0, 3L, null, null)));
        assertEquals(2, lookups[0]);
    }

    @Test
    void partitionLookupTimeoutIsRetriableWhenNothingCached() {
        CountingProducer mock = new CountingProducer(cluster(), true) {
            @Override
            public List<PartitionInfo> partitionsFor(String topic) {
                throw new TimeoutException("metadata timeout");
            }
        };
        TestTask task = new TestTask(mock);
        task.start(props());
        assertThrows(RetriableException.class, () -> task.put(List.of(record("pfx.a", 0, 1L, null, null))));
    }

    // ---- delivery guarantees ----

    @Test
    void preCommitFlushesAndReturnsOffsetsWhenSendsSucceed() {
        TestTask task = started(false);
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(new TopicPartition("pfx.a", 0),
            new OffsetAndMetadata(2L));
        assertEquals(offsets, task.preCommit(offsets));
        assertEquals(1, task.mock.flushes);
        assertTrue(task.mock.flushed(), "no sends left pending after preCommit");
    }

    @Test
    void failedSendMakesPreCommitThrowSoOffsetsAreNotCommitted() {
        TestTask task = started(false);
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        task.mock.errorNext(new RuntimeException("broker said no"));

        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(new TopicPartition("pfx.a", 0),
            new OffsetAndMetadata(2L));
        ConnectException e = assertThrows(ConnectException.class, () -> task.preCommit(offsets));
        assertTrue(e.getMessage().contains(TARGET));
    }

    @Test
    void failureIsStickyAndFailsNextPut() {
        TestTask task = started(false);
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        task.mock.errorNext(new RuntimeException("boom"));
        assertThrows(ConnectException.class, () -> task.put(List.of(record("pfx.a", 0, 2L, null, null))));
        assertThrows(ConnectException.class, () -> task.flush(Map.of()));
    }

    @Test
    void synchronousRetriableSendErrorBecomesRetriableException() {
        CountingProducer mock = new CountingProducer(cluster(), false) {
            @Override
            public java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                ProducerRecord<byte[], byte[]> record, org.apache.kafka.clients.producer.Callback callback) {
                throw new TimeoutException("buffer full");
            }
        };
        TestTask task = new TestTask(mock);
        task.start(props());
        assertThrows(RetriableException.class, () -> task.put(List.of(record("pfx.a", 0, 1L, null, null))));
    }

    @Test
    void synchronousNonRetriableSendErrorFailsTask() {
        CountingProducer mock = new CountingProducer(cluster(), false) {
            @Override
            public java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                ProducerRecord<byte[], byte[]> record, org.apache.kafka.clients.producer.Callback callback) {
                throw new org.apache.kafka.common.errors.RecordTooLargeException("too big");
            }
        };
        TestTask task = new TestTask(mock);
        task.start(props());
        assertThrows(ConnectException.class, () -> task.put(List.of(record("pfx.a", 0, 1L, null, null))));
    }

    // ---- lifecycle ----

    @Test
    void stopFlushesAndClosesProducer() {
        TestTask task = started(false);
        task.put(List.of(record("pfx.a", 0, 1L, null, new byte[] {1})));
        task.stop();
        assertEquals(1, task.mock.flushes);
        assertEquals(1, task.mock.closes);
        assertTrue(task.mock.closed());
        assertEquals(1, task.mock.history().size());
        assertTrue(task.mock.flushed(), "no sends left pending after stop");
    }

    @Test
    void stopTwiceAndStopWithoutStartAreSafe() {
        TestTask task = started(true);
        task.stop();
        task.stop();
        new FanInSinkTask().stop();
    }
}
