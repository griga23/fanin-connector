package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

/** The start timestamp, as seen by the task: when it moves partitions, when it must not, and how it fails. */
class FanInSinkTaskStartTest {

    private static final TopicPartition A0 = new TopicPartition("a", 0);
    private static final TopicPartition A1 = new TopicPartition("a", 1);

    /** Task with the producer and the broker lookup replaced. */
    static class TestTask extends FanInSinkTask {
        final StartOffsetsTest.FakeLookup lookup;
        final List<Map<String, Object>> lookupConfigs = new ArrayList<>();
        int lookupsCreated;
        int lookupCloses;
        RuntimeException createFailure;

        TestTask(StartOffsetsTest.FakeLookup lookup) {
            this.lookup = lookup;
        }

        @Override
        Producer<byte[], byte[]> createProducer(Map<String, Object> producerConfigs) {
            return new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        StartOffsets.Lookup createStartLookup(Map<String, Object> adminConfigs) {
            lookupsCreated++;
            lookupConfigs.add(adminConfigs);
            if (createFailure != null) {
                throw createFailure;
            }
            return new StartOffsets.Lookup() {
                @Override
                public Map<TopicPartition, Long> committed(String groupId) throws Exception {
                    return lookup.committed(groupId);
                }

                @Override
                public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> p, long ts) {
                    return lookup.offsetsAtOrAfter(p, ts);
                }

                @Override
                public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> p) {
                    return lookup.endOffsets(p);
                }

                @Override
                public void close() {
                    lookupCloses++;
                }
            };
        }
    }

    /** Records what the task asks the framework to seek. */
    static class Seeks {
        final List<Map<TopicPartition, Long>> calls = new ArrayList<>();

        SinkTaskContext context() {
            return (SinkTaskContext) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {SinkTaskContext.class}, (proxy, method, args) -> {
                    if (method.getName().equals("offset") && args.length == 1) {
                        @SuppressWarnings("unchecked")
                        Map<TopicPartition, Long> offsets = (Map<TopicPartition, Long>) args[0];
                        calls.add(new HashMap<>(offsets));
                    }
                    return null;
                });
        }
    }

    private static Map<String, String> props(String... extra) {
        Map<String, String> p = new HashMap<>();
        p.put("name", "fanin");
        p.put("topics.regex", "^in\\..+");
        p.put(FanInSinkConfig.TARGET_TOPIC, "fanin-out");
        p.put(FanInSinkConfig.TARGET_BOOTSTRAP_SERVERS, "localhost:9092");
        p.put(FanInSinkConfig.TARGET_API_KEY, "key");
        p.put(FanInSinkConfig.TARGET_API_SECRET, "secret");
        for (int i = 0; i < extra.length; i += 2) {
            p.put(extra[i], extra[i + 1]);
        }
        return p;
    }

    private static TestTask started(StartOffsetsTest.FakeLookup lookup, Seeks seeks, String... extra) {
        TestTask task = new TestTask(lookup);
        task.initialize(seeks.context());
        task.start(props(extra));
        return task;
    }

    @Test
    void offByDefaultTheTaskNeverContactsTheBroker() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup();
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks);

        task.open(List.of(A0, A1));

        assertEquals(0, task.lookupsCreated);
        assertTrue(seeks.calls.isEmpty());
    }

    @Test
    void movesUncommittedPartitionsToTheTimestampPosition() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup();
        lookup.atTime.put(A0, 120L);
        lookup.atTime.put(A1, 7L);
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of(A0, A1));

        assertEquals(List.of(Map.of(A0, 120L, A1, 7L)), seeks.calls);
        assertEquals(1000L, lookup.lastTimestamp);
    }

    @Test
    void aRestartResumesFromCommittedOffsetsAndSeeksNothing() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup();
        lookup.committed.put(A0, 500L);
        lookup.committed.put(A1, 600L);
        lookup.atTime.put(A0, 1L);
        lookup.atTime.put(A1, 1L);
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of(A0, A1));

        assertTrue(seeks.calls.isEmpty(), "committed partitions must never be rewound");
    }

    @Test
    void aNewTopicNextToCommittedOnesIsTheOnlyOneMoved() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup();
        lookup.committed.put(A0, 500L);
        lookup.atTime.put(A1, 0L);
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of(A0, A1));

        assertEquals(List.of(Map.of(A1, 0L)), seeks.calls);
    }

    @Test
    void usesTheConfiguredGroupInsteadOfTheConnectDefault() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup() {
            @Override
            public Map<TopicPartition, Long> committed(String groupId) {
                return "my-group".equals(groupId) ? committed : null;
            }
        };
        lookup.atTime.put(A0, 5L);
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000",
            FanInSinkConfig.START_CONSUMER_GROUP, "my-group");

        task.open(List.of(A0));

        assertEquals(List.of(Map.of(A0, 5L)), seeks.calls);
    }

    @Test
    void aWrongGroupNameSeeksNothing() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup() {
            @Override
            public Map<TopicPartition, Long> committed(String groupId) {
                return null; // no such group
            }
        };
        lookup.atTime.put(A0, 5L);
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of(A0));

        assertTrue(seeks.calls.isEmpty());
    }

    @Test
    void aBrokerFailureDoesNotFailTheTask() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup() {
            @Override
            public Map<TopicPartition, Long> committed(String groupId) {
                throw new org.apache.kafka.common.errors.TimeoutException("broker not reachable");
            }
        };
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of(A0)); // must not throw

        assertTrue(seeks.calls.isEmpty());
    }

    @Test
    void aClientLibraryThatLacksAMethodDoesNotKillTheTask() {
        // seen on Confluent Cloud: java.lang.NoSuchMethodError for ConsumerGroupDescription.groupState()
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup() {
            @Override
            public Map<TopicPartition, Long> committed(String groupId) {
                throw new NoSuchMethodError("ConsumerGroupDescription.groupState()");
            }
        };
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of(A0)); // must not throw

        assertTrue(seeks.calls.isEmpty());
    }

    @Test
    void aFailureToCreateTheAdminClientDoesNotFailTheTaskAndIsRetriedOnTheNextOpen() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup();
        lookup.atTime.put(A0, 5L);
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");
        task.createFailure = new org.apache.kafka.common.KafkaException("bad config");

        task.open(List.of(A0));
        assertTrue(seeks.calls.isEmpty());

        task.createFailure = null;
        task.open(List.of(A0));
        assertEquals(2, task.lookupsCreated);
        assertEquals(List.of(Map.of(A0, 5L)), seeks.calls);
    }

    @Test
    void oneAdminClientServesEveryRebalanceAndIsClosedOnStop() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup();
        lookup.committed.put(A0, 1L);
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of(A0));
        task.open(List.of(A0, A1));
        task.open(List.of(A1));
        assertEquals(1, task.lookupsCreated);
        assertEquals(0, task.lookupCloses);

        task.stop();
        assertEquals(1, task.lookupCloses);
    }

    @Test
    void anEmptyAssignmentDoesNothing() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup();
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of());

        assertEquals(0, task.lookupsCreated);
    }

    @Test
    void theAdminClientUsesTheTargetClusterConnectionAndNothingElse() {
        StartOffsetsTest.FakeLookup lookup = new StartOffsetsTest.FakeLookup();
        Seeks seeks = new Seeks();
        TestTask task = started(lookup, seeks, FanInSinkConfig.START_TIMESTAMP, "1000");

        task.open(List.of(A0));

        Map<String, Object> admin = task.lookupConfigs.get(0);
        assertEquals("localhost:9092", admin.get("bootstrap.servers"));
        assertEquals("SASL_SSL", admin.get("security.protocol"));
        assertEquals("PLAIN", admin.get("sasl.mechanism"));
        assertTrue(admin.containsKey("sasl.jaas.config"));
        assertFalse(admin.containsKey("acks"), "producer-only settings must not reach the admin client");
        assertFalse(admin.containsKey("enable.idempotence"));
        assertFalse(admin.containsKey("key.serializer"));
    }

    @Test
    void configRejectsValuesBelowMinusOneAndDefaultsToOff() {
        assertEquals(-1L, new FanInSinkConfig(props()).startTimestamp());
        assertEquals(1234L,
            new FanInSinkConfig(props(FanInSinkConfig.START_TIMESTAMP, "1234")).startTimestamp());
        assertThrows(ConfigException.class,
            () -> new FanInSinkConfig(props(FanInSinkConfig.START_TIMESTAMP, "-2")));
    }

    @Test
    void theDefaultGroupIsTheConnectDefault() {
        FanInSinkConfig config = new FanInSinkConfig(props());
        assertEquals("connect-fanin", config.startConsumerGroup("fanin"));
        assertEquals("custom",
            new FanInSinkConfig(props(FanInSinkConfig.START_CONSUMER_GROUP, "custom")).startConsumerGroup("fanin"));
    }
}
