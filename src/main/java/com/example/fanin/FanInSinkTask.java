package com.example.fanin;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-produces every consumed record to a single target topic.
 *
 * <p>Delivery is at-least-once: offsets are only committed (see {@link #preCommit}) after the producer has
 * acknowledged all records sent so far.
 */
public class FanInSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(FanInSinkTask.class);

    static final String HEADER_TOPIC = "fanin.source.topic";
    static final String HEADER_PARTITION = "fanin.source.partition";
    static final String HEADER_OFFSET = "fanin.source.offset";
    static final String HEADER_TIMESTAMP = "fanin.source.timestamp";

    static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);
    static final long PARTITION_REFRESH_MS = Duration.ofMinutes(5).toMillis();

    private FanInSinkConfig config;
    private String targetTopic;
    private FanInSinkConfig.OutputMode outputMode;
    private FanInSinkConfig.PartitioningMode partitioningMode;
    private FanInSinkConfig.TimestampMode timestampMode;
    private Producer<byte[], byte[]> producer;

    /** First asynchronous send failure; sticky, so the task fails and offsets are never committed past it. */
    private final AtomicReference<Throwable> sendFailure = new AtomicReference<>();

    private long startTimestamp = -1L;
    private String consumerGroup;
    private String clientId;
    private StartOffsets.Lookup startLookup;
    /** The control topic feature; null when fanin.control.topic is not set. */
    private ControlCoordinator control;

    private int cachedPartitionCount;
    private long partitionCountFetchedAt;
    LongSupplier clock = System::currentTimeMillis;

    @Override
    public void start(Map<String, String> props) {
        config = new FanInSinkConfig(props);
        targetTopic = config.targetTopic();
        outputMode = config.outputMode();
        partitioningMode = config.partitioningMode();
        timestampMode = config.timestampMode();

        Optional<String> error = FanInSinkConfig.targetSubscriptionError(props);
        if (error.isPresent()) {
            throw new ConfigException(error.get());
        }

        Map<String, Object> producerConfigs = config.producerConfigs();
        clientId = "fanin-" + props.getOrDefault("name", "connector") + "-"
            + props.getOrDefault(FanInSinkConnector.TASK_INDEX, "0");
        startTimestamp = config.startTimestamp();
        consumerGroup = config.startConsumerGroup(props.getOrDefault("name", "connector"));
        producerConfigs.putIfAbsent("client.id", clientId);
        producer = createProducer(producerConfigs);
        log.info("Started fan-in task: target topic {}, output mode {}, partitioning {}",
            targetTopic, outputMode, partitioningMode);
        log.info("Output record timestamp: {}", timestampMode);
        Optional<String> controlTopic = config.controlTopic();
        if (controlTopic.isPresent()) {
            control = ControlCoordinator.start(config, props, controlTopic.get(), context, this::lookup,
                this::createControlReader, this::warn, () -> clock.getAsLong(), startTimestamp, consumerGroup);
        }
    }

    /** Overridable so tests can inject a reader on a MockConsumer. */
    ControlReader createControlReader(String topic, ControlState state) {
        Map<String, Object> configs = config.controlConsumerConfigs();
        configs.putIfAbsent("client.id", clientId + "-control");
        return new ControlReader(new KafkaConsumer<>(configs), topic, state, clock);
    }

    /** The control state, or null if the feature is off. For tests. */
    ControlState controlState() {
        return control == null ? null : control.state();
    }

    /** Logs a warning about the control list. Overridable so tests can see the message. */
    void warn(String message) {
        log.warn("{}", message);
    }

    /**
     * Moves partitions that have no committed offset to the configured start timestamp. Runs when partitions are
     * assigned, which includes every new topic and every rebalance. Partitions with a committed offset are never
     * touched. A failure is logged and the task carries on with the consumer's own reset policy: stopping would not
     * help, and the worst case is the behaviour without this setting. With the control topic, the
     * {@link ControlCoordinator} decides the start position instead, including the start timestamp.
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        if (control != null && !partitions.isEmpty()) {
            control.onOpen(partitions);
            return;
        }
        if (startTimestamp < 0 || partitions.isEmpty()) {
            return;
        }
        try {
            Map<TopicPartition, Long> moves =
                StartOffsets.compute(lookup(), consumerGroup, partitions, startTimestamp);
            if (!moves.isEmpty()) {
                context.offset(moves);
                log.info("Start timestamp {}: moved {} of {} assigned partitions without a committed offset",
                    startTimestamp, moves.size(), partitions.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while applying the start timestamp; partitions keep their default start");
        } catch (Exception | LinkageError e) {
            // LinkageError (for example NoSuchMethodError) covers a client library that differs from the one this
            // was built against. An optional feature must never be able to kill the task.
            log.warn("Could not apply the start timestamp {} for group {}; partitions keep their default start: {}",
                startTimestamp, consumerGroup, e.toString());
        }
    }

    /** Overridable so tests can inject a fake lookup. */
    StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
        Map<String, Object> configs = new java.util.HashMap<>(adminConfigs);
        configs.putIfAbsent("client.id", clientId + "-admin");
        return new AdminLookup(Admin.create(configs));
    }

    private StartOffsets.Lookup lookup() {
        if (startLookup == null) {
            startLookup = createStartLookup(config.adminConfigs());
        }
        return startLookup;
    }

    /** Overridable so tests can inject a MockProducer. */
    Producer<byte[], byte[]> createProducer(Map<String, Object> producerConfigs) {
        return new KafkaProducer<>(producerConfigs);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        checkSendFailure();
        if (control != null) {
            control.beforePut();
        }
        for (SinkRecord record : records) {
            if (control != null && !control.accept(record)) {
                continue;
            }
            ProducerRecord<byte[], byte[]> out = toProducerRecord(record);
            if (log.isDebugEnabled()) {
                log.debug("Forwarding {}-{}@{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            }
            try {
                producer.send(out, this::onSendCompleted);
            } catch (org.apache.kafka.common.errors.RetriableException e) {
                // Raised synchronously (for example a metadata timeout). Connect redelivers the batch.
                throw new RetriableException("Transient producer error: " + e.getMessage(), e);
            } catch (KafkaException e) {
                throw new ConnectException("Producer error: " + e.getMessage(), e);
            }
            if (control != null) {
                control.forwarded(record);
            }
        }
        if (control != null) {
            control.afterPut(records);
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        if (control != null) {
            control.onClose(partitions);
        }
    }

    /**
     * Flushes the producer, then returns the offsets to commit. With the control topic, the
     * {@link ControlCoordinator} holds some offsets back and adds the ledger of each partition to its metadata.
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        flushAndCheck();
        return control == null ? currentOffsets : control.preCommit(currentOffsets);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        flushAndCheck();
    }

    @Override
    public void stop() {
        if (control != null) {
            control.stop();
        }
        StartOffsets.Lookup lookup = startLookup;
        startLookup = null;
        if (lookup != null) {
            try {
                lookup.close();
            } catch (RuntimeException e) {
                log.warn("Closing the admin client failed: {}", e.toString());
            }
        }
        Producer<byte[], byte[]> p = producer;
        producer = null;
        if (p == null) {
            return;
        }
        try {
            p.flush();
        } catch (RuntimeException e) {
            log.warn("Flush on stop failed: {}", e.toString());
        }
        try {
            p.close(CLOSE_TIMEOUT);
        } catch (RuntimeException e) {
            log.warn("Closing the producer failed: {}", e.toString());
        }
    }

    @Override
    public String version() {
        return Version.get();
    }

    // ---- internals ----

    private void onSendCompleted(RecordMetadata metadata, Exception exception) {
        if (exception != null && sendFailure.compareAndSet(null, exception)) {
            log.error("Producing to {} failed, the task will fail: {}", targetTopic, exception.toString());
        }
    }

    private void flushAndCheck() {
        try {
            producer.flush();
        } catch (KafkaException e) {
            throw new ConnectException("Flushing the producer failed: " + e.getMessage(), e);
        }
        checkSendFailure();
    }

    private void checkSendFailure() {
        Throwable failure = sendFailure.get();
        if (failure != null) {
            throw new ConnectException("Failed to produce to " + targetTopic + ": " + failure.getMessage(), failure);
        }
    }

    ProducerRecord<byte[], byte[]> toProducerRecord(SinkRecord record) {
        byte[] key = toBytes(record.key(), "key");
        byte[] value = toBytes(record.value(), "value");

        RecordHeaders headers = new RecordHeaders();
        for (Header header : record.headers()) {
            headers.add(header.key(), headerBytes(header.value()));
        }
        headers.add(HEADER_TOPIC, utf8(record.topic()));
        headers.add(HEADER_PARTITION, utf8(String.valueOf(record.kafkaPartition())));
        headers.add(HEADER_OFFSET, utf8(String.valueOf(record.kafkaOffset())));
        if (record.timestamp() != null) {
            headers.add(HEADER_TIMESTAMP, utf8(String.valueOf(record.timestamp())));
        }

        byte[] outValue = outputMode == FanInSinkConfig.OutputMode.JSON_ENVELOPE
            ? envelope(record.topic(), record.kafkaPartition(), record.kafkaOffset(), record.timestamp(), key, value)
            : value;

        Integer partition = null;
        if (partitioningMode == FanInSinkConfig.PartitioningMode.SOURCE_PARTITION_HASH) {
            partition = selectPartition(record.topic(), record.kafkaPartition(), partitionCount());
        }
        return new ProducerRecord<>(targetTopic, partition, outputTimestamp(timestampMode, record.timestamp()), key, outValue,
            headers);
    }

    /**
     * In SOURCE mode keeps the source record's timestamp, so readers that use the Kafka timestamp as event time (for
     * example Flink's {@code $rowtime}) see the original time and not the time of the copy. Returns null, so the
     * producer sets the current time, in COPY mode and when the source record has no timestamp. KafkaProducer rejects
     * negative values.
     */
    static Long outputTimestamp(FanInSinkConfig.TimestampMode mode, Long sourceTimestamp) {
        if (mode == FanInSinkConfig.TimestampMode.COPY) {
            return null;
        }
        return sourceTimestamp != null && sourceTimestamp >= 0 ? sourceTimestamp : null;
    }

    /** Stable partition for a source topic-partition, always within [0, numPartitions). */
    static int selectPartition(String sourceTopic, int sourcePartition, int numPartitions) {
        byte[] bytes = (sourceTopic + "-" + sourcePartition).getBytes(StandardCharsets.UTF_8);
        return Utils.toPositive(Utils.murmur2(bytes)) % numPartitions;
    }

    /** Partition count of the target topic, cached and refreshed periodically. */
    private int partitionCount() {
        long now = clock.getAsLong();
        if (cachedPartitionCount <= 0 || now - partitionCountFetchedAt >= PARTITION_REFRESH_MS) {
            try {
                List<PartitionInfo> partitions = producer.partitionsFor(targetTopic);
                if (partitions != null && !partitions.isEmpty()) {
                    cachedPartitionCount = partitions.size();
                    partitionCountFetchedAt = now;
                } else if (cachedPartitionCount <= 0) {
                    throw new RetriableException("No partition metadata for target topic " + targetTopic);
                }
            } catch (org.apache.kafka.common.errors.RetriableException e) {
                if (cachedPartitionCount <= 0) {
                    throw new RetriableException("Cannot fetch partitions of " + targetTopic + ": "
                        + e.getMessage(), e);
                }
                log.warn("Refreshing partitions of {} failed, using cached count: {}", targetTopic, e.toString());
            } catch (KafkaException e) {
                throw new ConnectException("Cannot fetch partitions of " + targetTopic + ": " + e.getMessage(), e);
            }
        }
        return cachedPartitionCount;
    }

    static byte[] envelope(String topic, int partition, long offset, Long timestamp, byte[] key, byte[] value) {
        StringBuilder sb = new StringBuilder(128 + (key == null ? 0 : key.length * 2)
            + (value == null ? 0 : value.length * 2));
        sb.append("{\"source_topic\":");
        appendJsonString(sb, topic);
        sb.append(",\"source_partition\":").append(partition);
        sb.append(",\"source_offset\":").append(offset);
        sb.append(",\"source_timestamp\":").append(timestamp == null ? "null" : timestamp.toString());
        sb.append(",\"key\":");
        appendBase64(sb, key);
        sb.append(",\"value\":");
        appendBase64(sb, value);
        sb.append('}');
        return utf8(sb.toString());
    }

    private static void appendBase64(StringBuilder sb, byte[] data) {
        if (data == null) {
            sb.append("null");
        } else {
            sb.append('"').append(Base64.getEncoder().encodeToString(data)).append('"');
        }
    }

    private static void appendJsonString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static byte[] toBytes(Object o, String what) {
        if (o == null) {
            return null;
        }
        if (o instanceof byte[] bytes) {
            return bytes;
        }
        if (o instanceof ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
        throw new DataException("Record " + what + " is a " + o.getClass().getName()
            + ". Set key.converter and value.converter to "
            + "org.apache.kafka.connect.converters.ByteArrayConverter.");
    }

    /** Header values arrive as bytes with header.converter=ByteArrayConverter, otherwise as Connect types. */
    private static byte[] headerBytes(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof byte[] || o instanceof ByteBuffer) {
            return toBytes(o, "header");
        }
        return utf8(o.toString());
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
