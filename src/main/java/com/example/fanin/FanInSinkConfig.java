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
    /** Prefix for pass-through producer settings, e.g. fanin.producer.compression.type. */
    public static final String PRODUCER_PREFIX = "fanin.producer.";

    /** Framework properties (not part of our ConfigDef). */
    public static final String TOPICS = "topics";
    public static final String TOPICS_REGEX = "topics.regex";

    public enum OutputMode { HEADERS, JSON_ENVELOPE }

    public enum PartitioningMode { SOURCE_PARTITION_HASH, KEY_HASH }

    public enum TimestampMode { SOURCE, COPY }

    private static final String GROUP_TARGET = "Target cluster";
    private static final String GROUP_BEHAVIOUR = "Behaviour";

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
            "Kafka API key used by the producer (SASL_SSL / PLAIN). Use a service account key that can only "
                + "write to the target topic.",
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
            GROUP_BEHAVIOUR, 5, Width.MEDIUM, "Start consumer group");

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
