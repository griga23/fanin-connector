package com.example.fanin;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/**
 * Configuration of the fan-in sink.
 *
 * <p>{@code topics} and {@code topics.regex} are framework properties and are deliberately not defined here.
 */
public class FanInSinkConfig extends AbstractConfig {

    public static final String TARGET_TOPIC = "fanin.target.topic";
    public static final String TARGET_BOOTSTRAP_SERVERS = "fanin.target.bootstrap.servers";
    public static final String TARGET_API_KEY = "fanin.target.api.key";
    public static final String TARGET_API_SECRET = "fanin.target.api.secret";
    public static final String OUTPUT_MODE = "fanin.output.mode";
    public static final String PARTITIONING_MODE = "fanin.partitioning.mode";
    public static final String TIMESTAMP_MODE = "fanin.timestamp.mode";
    public static final String START_TIMESTAMP = "fanin.start.timestamp";
    public static final String START_CONSUMER_GROUP = "fanin.start.consumer.group";
    public static final String CONTROL_TOPIC = "fanin.control.topic";
    public static final String CONTROL_ADD_START = "fanin.control.add.start";
    public static final String CONTROL_MISSING_WARN_MS = "fanin.control.missing.warn.ms";
    public static final String CONTROL_LOAD_TIMEOUT_MS = "fanin.control.load.timeout.ms";
    public static final String CONTROL_MAX_LAG_MS = "fanin.control.max.lag.ms";
    /**
     * Smallest fanin.control.max.lag.ms. A healthy reader can look this far behind: it measures its position every
     * {@link ControlReader#CHECK_MS}; the measurement starts after a poll (up to {@link ControlReader#POLL_TIMEOUT}),
     * the end offset request can take up to {@link ControlReader#REQUEST_TIMEOUT}, and the reader reaches that end
     * offset after one more poll. A smaller value would stall the task although the reader is fine.
     */
    static final long MIN_CONTROL_MAX_LAG_MS = ControlReader.CHECK_MS + 2 * ControlReader.POLL_TIMEOUT.toMillis()
        + ControlReader.REQUEST_TIMEOUT.toMillis();

    /** Prefix for pass-through producer settings, e.g. fanin.producer.compression.type. */
    public static final String PRODUCER_PREFIX = "fanin.producer.";

    /** Framework properties (not part of our ConfigDef). */
    public static final String TOPICS = "topics";
    public static final String TOPICS_REGEX = "topics.regex";

    public enum OutputMode { HEADERS, JSON_ENVELOPE }

    public enum PartitioningMode { SOURCE_PARTITION_HASH, KEY_HASH }

    public enum TimestampMode { SOURCE, COPY }

    /** Where a topic starts when the control topic adds it. */
    public enum AddStart { LATEST, EARLIEST }

    private static final String GROUP_TARGET = "Target cluster";
    private static final String GROUP_BEHAVIOUR = "Behaviour";
    private static final String GROUP_CONTROL = "Control topic";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TARGET_TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
            Importance.HIGH,
            "The single topic all records are written to. It must not match topics.regex (or be listed in "
                + "topics), otherwise the connector would consume its own output.",
            GROUP_TARGET, 1, Width.MEDIUM, "Target topic")
        .define(TARGET_BOOTSTRAP_SERVERS, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
            Importance.HIGH,
            "Bootstrap endpoint of the cluster that holds the target topic, as host:port. On Confluent Cloud "
                + "this endpoint may also need to be listed in confluent.custom.connection.endpoints.",
            GROUP_TARGET, 2, Width.LONG, "Target bootstrap servers")
        .define(TARGET_API_KEY, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
            Importance.HIGH,
            "Kafka API key used by the producer (SASL_SSL / PLAIN). The offset lookups and the control topic "
                + "reader use it too. It needs WRITE and DESCRIBE on the target topic, READ and DESCRIBE on the "
                + "source topics and on the connect- consumer group, and READ and DESCRIBE on the control topic.",
            GROUP_TARGET, 3, Width.MEDIUM, "Target API key")
        .define(TARGET_API_SECRET, Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            "Kafka API secret that belongs to the API key.",
            GROUP_TARGET, 4, Width.MEDIUM, "Target API secret")
        .define(OUTPUT_MODE, Type.STRING, OutputMode.HEADERS.name(),
            ConfigDef.CaseInsensitiveValidString.in(names(OutputMode.values())),
            Importance.MEDIUM,
            "HEADERS: key and value are passed through unchanged and the source topic, partition, offset and "
                + "timestamp are added as fanin.source.* headers. JSON_ENVELOPE: the value becomes a JSON object "
                + "with source_topic, source_partition, source_offset, source_timestamp and base64 key and value. "
                + "Use JSON_ENVELOPE if Flink cannot read Kafka headers.",
            GROUP_BEHAVIOUR, 1, Width.MEDIUM, "Output mode")
        .define(PARTITIONING_MODE, Type.STRING, PartitioningMode.SOURCE_PARTITION_HASH.name(),
            ConfigDef.CaseInsensitiveValidString.in(names(PartitioningMode.values())),
            Importance.MEDIUM,
            "SOURCE_PARTITION_HASH: the target partition is murmur2(sourceTopic-sourcePartition) modulo the "
                + "partition count of the target topic, which keeps per-source-partition ordering even for null "
                + "keys. KEY_HASH: the default producer partitioner decides from the record key.",
            GROUP_BEHAVIOUR, 2, Width.MEDIUM, "Partitioning mode")
        .define(TIMESTAMP_MODE, Type.STRING, TimestampMode.SOURCE.name(),
            ConfigDef.CaseInsensitiveValidString.in(names(TimestampMode.values())),
            Importance.MEDIUM,
            "SOURCE: the output record keeps the Kafka timestamp of the source record, so a reader that uses the "
                + "record timestamp as event time (Flink's $rowtime) sees the original time. A source record "
                + "without a timestamp gets the time of the copy. COPY: the output record gets the time of the "
                + "copy. Records older than the retention of the target topic are deleted soon after they are "
                + "written in SOURCE mode, which matters when old data is read again. The header "
                + "fanin.source.timestamp always has the original value.",
            GROUP_BEHAVIOUR, 3, Width.MEDIUM, "Timestamp mode")
        .define(START_TIMESTAMP, Type.LONG, -1L, ConfigDef.Range.atLeast(-1L),
            Importance.MEDIUM,
            "Epoch milliseconds. Partitions that have no committed offset in the connector's consumer group start "
                + "at the first record with a timestamp at or after this value, instead of at the beginning of "
                + "the topic. This covers the first deployment, newly discovered topics and a recreated "
                + "connector. Partitions with a committed offset always resume from it. A partition with no "
                + "record at or after the value starts at its end. -1 turns this off and leaves the choice to "
                + "auto.offset.reset. The source topics must be on the cluster of the target topic, because "
                + "the lookup uses the target cluster settings.",
            GROUP_BEHAVIOUR, 4, Width.MEDIUM, "Start timestamp")
        .define(START_CONSUMER_GROUP, Type.STRING, "", Importance.LOW,
            "Consumer group of the connector, used to find which partitions have a committed offset. Empty means "
                + "connect-<connector name>, the Kafka Connect default. Set it if the connector's group differs, "
                + "for example when group.id is overridden. If the group cannot be found, the start timestamp is "
                + "ignored and a warning is logged.",
            GROUP_BEHAVIOUR, 5, Width.MEDIUM, "Start consumer group")
        .define(CONTROL_TOPIC, Type.STRING, "", Importance.MEDIUM,
            "Compacted topic that lists the allowed source topics. Key: the topic name. A value that is not null "
                + "adds the topic, a null value (tombstone) removes it. Records of topics that are not allowed are "
                + "paused and dropped. Empty turns the feature off, and then every subscribed topic is forwarded.",
            GROUP_CONTROL, 1, Width.LONG, "Control topic")
        .define(CONTROL_ADD_START, Type.STRING, AddStart.EARLIEST.name(),
            ConfigDef.CaseInsensitiveValidString.in(names(AddStart.values())),
            Importance.MEDIUM,
            "EARLIEST (default): an added topic continues from its committed offset or the consumer's reset "
                + "policy, so the first add of an existing topic forwards its full history. "
                + "LATEST: an added topic starts at the time of its add record, so older records are not forwarded. "
                + "A topic that was removed and added again always starts at the time of the new add record.",
            GROUP_CONTROL, 2, Width.MEDIUM, "Start of an added topic")
        .define(CONTROL_MISSING_WARN_MS, Type.LONG, 3_600_000L, ConfigDef.Range.atLeast(0L),
            Importance.LOW,
            "A WARN line is logged once for every allowed name that has had no matching topic for this many "
                + "milliseconds since it was added.",
            GROUP_CONTROL, 3, Width.SHORT, "Missing topic warning (ms)")
        .define(CONTROL_LOAD_TIMEOUT_MS, Type.LONG, 60_000L, ConfigDef.Range.atLeast(1L),
            Importance.LOW,
            "Maximum time in milliseconds for a task to read the control topic to its end when it starts. The task "
                + "fails if it cannot read the control topic in this time.",
            GROUP_CONTROL, 4, Width.SHORT, "Control topic load timeout (ms)")
        .define(CONTROL_MAX_LAG_MS, Type.LONG, 60_000L, FanInSinkConfig::validateMaxLag,
            Importance.LOW,
            "If the task has not read the control topic to its end for this many milliseconds (for example because "
                + "the control topic cannot be reached), it stops forwarding: it pauses all its partitions and drops "
                + "the records it gets, until the reader has caught up again. A remove takes effect within this time "
                + "even when the control topic cannot be read. Minimum " + MIN_CONTROL_MAX_LAG_MS + ": a healthy "
                + "reader measures its position only every few seconds.",
            GROUP_CONTROL, 5, Width.SHORT, "Control topic maximum lag (ms)");

    private static void validateMaxLag(String name, Object value) {
        if (value == null || (Long) value < MIN_CONTROL_MAX_LAG_MS) {
            throw new ConfigException(name, value, "Must be at least " + MIN_CONTROL_MAX_LAG_MS + " ms. The control "
                + "reader measures its position every " + ControlReader.CHECK_MS + " ms, and one measurement can take "
                + "up to " + (MIN_CONTROL_MAX_LAG_MS - ControlReader.CHECK_MS) + " ms (two polls of "
                + ControlReader.POLL_TIMEOUT.toMillis() + " ms and an end offset request of up to "
                + ControlReader.REQUEST_TIMEOUT.toMillis() + " ms). A smaller value stalls the task although the "
                + "reader is healthy.");
        }
    }

    public FanInSinkConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }

    public static ConfigDef configDef() {
        return CONFIG_DEF;
    }

    public String targetTopic() {
        return getString(TARGET_TOPIC);
    }

    public OutputMode outputMode() {
        return OutputMode.valueOf(getString(OUTPUT_MODE).toUpperCase(Locale.ROOT));
    }

    /** Start timestamp in epoch milliseconds, or -1 if off. */
    public long startTimestamp() {
        return getLong(START_TIMESTAMP);
    }

    /** Consumer group of this connector, {@code connect-<name>} unless configured. */
    public String startConsumerGroup(String connectorName) {
        String configured = getString(START_CONSUMER_GROUP);
        return configured == null || configured.isBlank() ? "connect-" + connectorName : configured;
    }

    /** Admin client settings: the connection part of the producer settings, for the same cluster. */
    public Map<String, Object> adminConfigs() {
        Map<String, Object> admin = new HashMap<>();
        for (Map.Entry<String, Object> e : producerConfigs().entrySet()) {
            String key = e.getKey();
            if (key.startsWith("bootstrap.") || key.startsWith("security.") || key.startsWith("sasl.")
                || key.startsWith("ssl.") || key.equals("client.dns.lookup")) {
                admin.put(key, e.getValue());
            }
        }
        return admin;
    }

    /** The control topic, or empty if the feature is off. */
    public Optional<String> controlTopic() {
        String topic = getString(CONTROL_TOPIC);
        return topic == null || topic.isBlank() ? Optional.empty() : Optional.of(topic.trim());
    }

    public AddStart controlAddStart() {
        return AddStart.valueOf(getString(CONTROL_ADD_START).toUpperCase(Locale.ROOT));
    }

    public long controlMissingWarnMs() {
        return getLong(CONTROL_MISSING_WARN_MS);
    }

    public long controlLoadTimeoutMs() {
        return getLong(CONTROL_LOAD_TIMEOUT_MS);
    }

    public long controlMaxLagMs() {
        return getLong(CONTROL_MAX_LAG_MS);
    }

    /**
     * Settings of the control topic reader: the connection settings of the target cluster, byte array
     * deserializers and no consumer group. The reader uses assign(), so it needs only READ on the control topic.
     */
    public Map<String, Object> controlConsumerConfigs() {
        Map<String, Object> consumer = adminConfigs();
        consumer.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer.put("enable.auto.commit", "false");
        consumer.put("auto.offset.reset", "earliest");
        consumer.put("isolation.level", "read_committed");
        return consumer;
    }

    public TimestampMode timestampMode() {
        return TimestampMode.valueOf(getString(TIMESTAMP_MODE).toUpperCase(Locale.ROOT));
    }

    public PartitioningMode partitioningMode() {
        return PartitioningMode.valueOf(getString(PARTITIONING_MODE).toUpperCase(Locale.ROOT));
    }

    /**
     * Producer settings: secure defaults first, then user overrides from {@code fanin.producer.*}
     * (prefix stripped), so a user may deliberately override anything, for example security.protocol.
     */
    public Map<String, Object> producerConfigs() {
        Password secret = getPassword(TARGET_API_SECRET);
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", getString(TARGET_BOOTSTRAP_SERVERS));
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + escapeJaas(getString(TARGET_API_KEY)) + "\" password=\"" + escapeJaas(secret.value()) + "\";");
        props.putAll(originalsWithPrefix(PRODUCER_PREFIX));
        return props;
    }

    /**
     * Returns an error message if the subscription ({@code topics.regex} or {@code topics}) would include the
     * target topic, otherwise empty. The framework subscribes with a full-match of the regex.
     */
    public static Optional<String> targetSubscriptionError(Map<String, String> props) {
        String target = props.get(TARGET_TOPIC);
        if (target == null || target.isBlank()) {
            return Optional.empty();
        }
        String regex = props.get(TOPICS_REGEX);
        if (regex != null && !regex.isBlank()) {
            try {
                if (Pattern.compile(regex).matcher(target).matches()) {
                    return Optional.of("topics.regex '" + regex + "' matches the target topic '" + target
                        + "'. The connector would consume its own output. Choose a regex or target topic name "
                        + "that does not overlap.");
                }
            } catch (PatternSyntaxException e) {
                return Optional.of("topics.regex is not a valid regular expression: " + e.getDescription());
            }
        }
        String control = props.get(CONTROL_TOPIC);
        if (control != null && !control.isBlank()) {
            if (target.trim().equals(control.trim())) {
                return Optional.of("The control topic '" + control.trim() + "' is the target topic. Every forwarded "
                    + "record would be read as a control record. Use a separate control topic.");
            }
            Optional<String> controlError = subscriptionContains(props, control.trim(), "control topic");
            if (controlError.isPresent()) {
                return controlError;
            }
        }
        String topics = props.get(TOPICS);
        if (topics != null) {
            for (String topic : topics.split(",")) {
                if (topic.trim().equals(target)) {
                    return Optional.of("topics contains the target topic '" + target
                        + "'. The connector would consume its own output.");
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<String> subscriptionContains(Map<String, String> props, String topic, String what) {
        String regex = props.get(TOPICS_REGEX);
        if (regex != null && !regex.isBlank()) {
            try {
                if (Pattern.compile(regex).matcher(topic).matches()) {
                    return Optional.of("topics.regex '" + regex + "' matches the " + what + " '" + topic
                        + "'. Its records would be forwarded. Choose names that do not overlap.");
                }
            } catch (PatternSyntaxException e) {
                return Optional.empty(); // reported for the target topic
            }
        }
        String topics = props.get(TOPICS);
        if (topics != null) {
            for (String t : topics.split(",")) {
                if (t.trim().equals(topic)) {
                    return Optional.of("topics contains the " + what + " '" + topic + "'.");
                }
            }
        }
        return Optional.empty();
    }

    private static String[] names(Enum<?>[] values) {
        String[] names = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            names[i] = values[i].name();
        }
        return names;
    }

    private static String escapeJaas(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
