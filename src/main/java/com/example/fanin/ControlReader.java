package com.example.fanin;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the control topic into a {@link ControlState}.
 *
 * <p>Record format: key = topic name (UTF-8, used as is). A value that is not null adds the topic, a null value
 * removes it. The time of the change is the timestamp of the control record. A record without a timestamp is skipped.
 *
 * <p>The control topic must have exactly one partition, so that all changes have one order. The reader assigns that
 * partition itself and uses no consumer group, so it needs only READ and DESCRIBE on the control topic.
 * {@link #loadInitial} runs on the task thread. After {@link #startBackground} only the reader's own thread uses the
 * consumer.
 *
 * <p>The background thread retries errors with a backoff. Only an authorization error or a wrong partition count
 * stops it ({@link #failure}). Every {@link #CHECK_MS} it fetches the end offset of the control partition; when its
 * position reaches that end offset, {@link #caughtUpAt} becomes the time of the fetch. All control records written
 * before that time are then in the state. The task uses this time to stop forwarding when the reader falls behind.
 */
class ControlReader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ControlReader.class);
    static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    /** Interval of the end offset fetch that measures whether the reader is caught up. */
    static final long CHECK_MS = 5_000L;
    /** Interval of the partition count check. */
    static final long PARTITION_CHECK_MS = 60_000L;
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    static final long BACKOFF_MIN_MS = 500L;
    static final long BACKOFF_MAX_MS = 30_000L;

    private final Consumer<byte[], byte[]> consumer;
    private final String topic;
    private final ControlState state;
    private final LongSupplier clock;
    private TopicPartition partition;

    private volatile boolean closed;
    private volatile Throwable failure;
    private volatile long caughtUpAt = Long.MIN_VALUE;
    private Thread thread;

    ControlReader(Consumer<byte[], byte[]> consumer, String topic, ControlState state, LongSupplier clock) {
        this.consumer = consumer;
        this.topic = topic;
        this.state = state;
        this.clock = clock;
    }

    /**
     * Reads the control topic from the beginning up to the end offset taken at the start of the call.
     *
     * @throws RetriableException if the end is not reached within the timeout
     * @throws ConnectException if the control topic does not exist or has more than one partition
     */
    void loadInitial(Duration timeout) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic, timeout);
        if (infos == null || infos.isEmpty()) {
            throw new ConnectException("Control topic " + topic + " does not exist or cannot be described. "
                + "Create it (cleanup.policy=compact, 1 partition) and give the connector's API key READ and "
                + "DESCRIBE on it.");
        }
        checkPartitionCount(infos);
        partition = new TopicPartition(topic, infos.get(0).partition());
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
        long checkTime = clock.getAsLong();
        long end = endOffset(timeout);
        long deadline = System.nanoTime() + timeout.toNanos();
        int records = 0;
        while (consumer.position(partition) < end) {
            if (System.nanoTime() >= deadline) {
                throw new RetriableException("Could not read control topic " + topic + " to its end offset " + end
                    + " within " + timeout.toMillis() + " ms");
            }
            records += applyAll(consumer.poll(Duration.ofMillis(200)));
        }
        caughtUpAt = checkTime;
        log.info("Read {} records of control topic {}: {} allowed topics {}", records, topic,
            state.allowed().size(), state.allowed());
    }

    /** A wrong control topic that a retry cannot fix. */
    static final class ControlTopicException extends ConnectException {
        ControlTopicException(String message) {
            super(message);
        }
    }

    private void checkPartitionCount(List<PartitionInfo> infos) {
        if (infos == null || infos.isEmpty()) {
            throw new IllegalStateException("Control topic " + topic + " does not exist or cannot be described");
        }
        if (infos.size() != 1) {
            throw new ControlTopicException("Control topic " + topic + " has " + infos.size() + " partitions. It must have "
                + "exactly 1 partition, so that all changes have one order. Create it again with 1 partition.");
        }
    }

    private long endOffset(Duration timeout) {
        Map<TopicPartition, Long> end = new HashMap<>(consumer.endOffsets(List.of(partition), timeout));
        Long offset = end.get(partition);
        if (offset == null) {
            throw new RetriableException("No end offset for " + partition);
        }
        return offset;
    }

    /** Starts a daemon thread that applies later control records until {@link #close}. */
    void startBackground() {
        long firstPartitionCheck = clock.getAsLong() + PARTITION_CHECK_MS;
        thread = new Thread(() -> runBackground(firstPartitionCheck), "fanin-control-" + topic);
        thread.setDaemon(true);
        thread.start();
    }

    private void runBackground(long firstPartitionCheck) {
        long backoff = BACKOFF_MIN_MS;
        long nextCheck = Long.MIN_VALUE;
        long nextPartitionCheck = firstPartitionCheck;
        long checkTime = 0;
        long checkEnd = -1;
        try {
            while (!closed) {
                try {
                    applyAll(consumer.poll(POLL_TIMEOUT));
                    long now = clock.getAsLong();
                    if (now >= nextPartitionCheck) {
                        checkPartitionCount(consumer.partitionsFor(topic, REQUEST_TIMEOUT));
                        nextPartitionCheck = now + PARTITION_CHECK_MS;
                    }
                    if (checkEnd < 0 && now >= nextCheck) {
                        checkTime = now;
                        checkEnd = endOffset(REQUEST_TIMEOUT);
                        nextCheck = now + CHECK_MS;
                    }
                    if (checkEnd >= 0 && consumer.position(partition) >= checkEnd) {
                        caughtUpAt = checkTime;
                        checkEnd = -1;
                    }
                    backoff = BACKOFF_MIN_MS;
                } catch (WakeupException | AuthorizationException | ControlTopicException e) {
                    throw e;
                } catch (RuntimeException e) {
                    checkEnd = -1;
                    log.warn("Reading control topic {} failed, retrying in {} ms: {}", topic, backoff, e.toString());
                    sleep(backoff);
                    backoff = Math.min(backoff * 2, BACKOFF_MAX_MS);
                }
            }
        } catch (WakeupException e) {
            if (!closed) {
                failure = e;
            }
        } catch (Throwable t) {
            failure = t;
            log.error("Reading control topic {} failed and cannot recover; the task will fail: {}", topic,
                t.toString());
        } finally {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("Closing the control topic consumer failed: {}", e.toString());
            }
        }
    }

    private void sleep(long ms) {
        long until = System.nanoTime() + ms * 1_000_000L;
        while (!closed && System.nanoTime() < until) {
            try {
                Thread.sleep(Math.min(100L, ms));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** The error that stopped the background thread, or null. Only non-recoverable errors stop it. */
    Throwable failure() {
        return failure;
    }

    /**
     * The latest time at which the reader had read all control records written before that time, or
     * {@link Long#MIN_VALUE} before the initial load. Uses the clock given to the constructor.
     */
    long caughtUpAt() {
        return caughtUpAt;
    }

    private int applyAll(ConsumerRecords<byte[], byte[]> records) {
        int n = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            n++;
            if (record.key() == null) {
                log.warn("Skipping control record without key at {}-{}@{}", record.topic(), record.partition(),
                    record.offset());
                continue;
            }
            if (record.timestamp() < 0) {
                log.warn("Skipping control record without timestamp at {}-{}@{}. The time of a change is the "
                    + "timestamp of its control record.", record.topic(), record.partition(), record.offset());
                continue;
            }
            String name = new String(record.key(), StandardCharsets.UTF_8);
            if (!name.equals(name.strip())) {
                log.warn("Control record key '{}' at {}-{}@{} has leading or trailing white space. It is used as is, "
                    + "so it does not match a topic name without the white space.", name, record.topic(),
                    record.partition(), record.offset());
            }
            boolean allow = record.value() != null;
            long time = record.timestamp();
            state.apply(name, allow, time);
            log.info("Control topic: {} {} (record time {}, {}-{}@{})", allow ? "add" : "remove", name, time,
                record.topic(), record.partition(), record.offset());
        }
        return n;
    }

    @Override
    public void close() {
        closed = true;
        Thread t = thread;
        if (t == null) {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("Closing the control topic consumer failed: {}", e.toString());
            }
            return;
        }
        consumer.wakeup();
        try {
            t.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
