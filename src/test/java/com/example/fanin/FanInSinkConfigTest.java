package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

/** The control-topic settings. */
class FanInSinkConfigTest {

    private static Map<String, String> props(String... extra) {
        Map<String, String> p = new HashMap<>();
        p.put("topics.regex", "^in\\..+");
        p.put(FanInSinkConfig.TARGET_TOPIC, "fanin-out");
        p.put(FanInSinkConfig.TARGET_BOOTSTRAP_SERVERS, "broker:9092");
        p.put(FanInSinkConfig.TARGET_API_KEY, "key");
        p.put(FanInSinkConfig.TARGET_API_SECRET, "secret");
        for (int i = 0; i < extra.length; i += 2) {
            p.put(extra[i], extra[i + 1]);
        }
        return p;
    }

    @Test
    void controlTopic_emptyMeansOff() {
        assertEquals(Optional.empty(), new FanInSinkConfig(props()).controlTopic());
        assertEquals(Optional.empty(), new FanInSinkConfig(props(FanInSinkConfig.CONTROL_TOPIC, "  ")).controlTopic());
        assertEquals(Optional.of("ctl"), new FanInSinkConfig(props(FanInSinkConfig.CONTROL_TOPIC, "ctl")).controlTopic());
    }

    @Test
    void addStart_defaultEarliest() {
        assertEquals(FanInSinkConfig.AddStart.EARLIEST, new FanInSinkConfig(props()).controlAddStart());
        assertEquals(FanInSinkConfig.AddStart.LATEST,
            new FanInSinkConfig(props(FanInSinkConfig.CONTROL_ADD_START, "latest")).controlAddStart());
    }

    @Test
    void addStart_rejectsUnknown() {
        assertThrows(ConfigException.class,
            () -> new FanInSinkConfig(props(FanInSinkConfig.CONTROL_ADD_START, "MIDDLE")));
    }

    @Test
    void timeouts_haveDefaultsAndRejectNegative() {
        FanInSinkConfig config = new FanInSinkConfig(props());
        assertEquals(3_600_000L, config.controlMissingWarnMs());
        assertEquals(60_000L, config.controlLoadTimeoutMs());
        assertThrows(ConfigException.class,
            () -> new FanInSinkConfig(props(FanInSinkConfig.CONTROL_MISSING_WARN_MS, "-1")));
        assertThrows(ConfigException.class,
            () -> new FanInSinkConfig(props(FanInSinkConfig.CONTROL_LOAD_TIMEOUT_MS, "0")));
    }

    @Test
    void controlConsumerConfigs_usesTargetCredentials() {
        Map<String, Object> c = new FanInSinkConfig(props(FanInSinkConfig.CONTROL_TOPIC, "ctl")).controlConsumerConfigs();
        assertEquals("broker:9092", c.get("bootstrap.servers"));
        assertEquals("SASL_SSL", c.get("security.protocol"));
        assertEquals("PLAIN", c.get("sasl.mechanism"));
        assertTrue(String.valueOf(c.get("sasl.jaas.config")).contains("username=\"key\""));
        assertFalse(c.containsKey("group.id"), "the control consumer uses assign() and no consumer group");
        assertEquals("false", c.get("enable.auto.commit"));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", c.get("key.deserializer"));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", c.get("value.deserializer"));
        assertFalse(c.containsKey("acks"), "producer-only settings must not reach the consumer");
        assertFalse(c.containsKey("key.serializer"));
    }

    @Test
    void controlTopic_mustNotMatchTheSubscription() {
        Optional<String> error = FanInSinkConfig.targetSubscriptionError(
            props(FanInSinkConfig.CONTROL_TOPIC, "in.control"));
        assertTrue(error.isPresent());
        assertTrue(error.get().contains("in.control"), error.get());
        assertEquals(Optional.empty(), FanInSinkConfig.targetSubscriptionError(
            props(FanInSinkConfig.CONTROL_TOPIC, "control")));
    }

    /** A control topic equal to the target topic is rejected: every forwarded record would become an add of its key. */
    @Test
    void controlTopicEqualToTargetTopic_isRejected() {
        Map<String, String> p = props(FanInSinkConfig.CONTROL_TOPIC, "fanin-out");
        assertTrue(FanInSinkConfig.targetSubscriptionError(p).isPresent(),
            "control topic == target topic is accepted; every forwarded record becomes an 'add' of its key");
    }

    @Test
    void maxLag_defaultAndReadCommitted() {
        FanInSinkConfig config = new FanInSinkConfig(props());
        assertEquals(60_000L, config.controlMaxLagMs());
        assertEquals("read_committed", config.controlConsumerConfigs().get("isolation.level"));
        assertThrows(ConfigException.class,
            () -> new FanInSinkConfig(props(FanInSinkConfig.CONTROL_MAX_LAG_MS, "0")));
    }
}
