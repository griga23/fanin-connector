package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Local integration tests L1-L9 against the docker stack in docker/docker-compose.yml.
 * Run with scripts/local-it.sh. Each test uses its own topics, target topic and connector.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.MethodName.class)
class LocalIT {

    static final String BOOTSTRAP = "localhost:29092";
    static final String CONNECT = "http://localhost:8083";
    static final String RUN = Long.toString(System.currentTimeMillis() % 100_000_000L, 36);
    static final ObjectMapper JSON = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();

    static Admin admin;
    static KafkaProducer<byte[], byte[]> producer;
    final List<String> connectors = new ArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        Properties admin = new Properties();
        admin.put("bootstrap.servers", BOOTSTRAP);
        LocalIT.admin = Admin.create(admin);

        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("linger.ms", "5");
        p.put("batch.size", "262144");
        p.put("max.request.size", "5242880");
        p.put("enable.idempotence", "true");
        producer = new KafkaProducer<>(p);

        await("connect REST with plugin", 120_000, () -> {
            try {
                return http("GET", "/connector-plugins", null).body().contains("FanInSinkConnector");
            } catch (Exception e) {
                return false;
            }
        });
        log("setup", "run id " + RUN);
    }

    @AfterAll
    static void tearDown() {
        producer.close();
        admin.close();
    }

    @AfterEach
    void deleteConnectors() throws Exception {
        for (String name : connectors) {
            http("DELETE", "/connectors/" + name, null);
        }
        connectors.clear();
    }

    // ------------------------------------------------------------------ L1, L2, L3, L4 (both modes: L9)

    @ParameterizedTest
    @EnumSource(FanInSinkConfig.OutputMode.class)
    void l1_prefixMatchOnly(FanInSinkConfig.OutputMode mode) throws Exception {
        String p = prefix("l1", mode);
        String target = p + "-out";
        createTopic(p + ".a", 3);
        createTopic(p + ".b", 3);
        createTopic("other" + p + ".c", 3);
        createTopic(target, 3);
        startConnector(p, regex(p), target, mode, 1, Map.of());

        for (String t : List.of(p + ".a", p + ".b", "other" + p + ".c")) {
            for (int i = 0; i < 50; i++) {
                send(t, null, ("k" + i).getBytes(StandardCharsets.UTF_8), ("v" + i).getBytes(StandardCharsets.UTF_8),
                    null);
            }
        }
        producer.flush();

        List<ConsumerRecord<byte[], byte[]>> out = consume(target, 100, 60_000);
        settle(5_000);
        out = consume(target, 100, 5_000);
        Set<String> topics = new HashSet<>();
        out.forEach(r -> topics.add(header(r, "fanin.source.topic")));
        assertEquals(Set.of(p + ".a", p + ".b"), topics);
        assertEquals(100, out.size(), "exactly the records of the two matching topics");
        log("L1/" + mode, "PASS: " + out.size() + " records from " + topics + ", none from the non-matching topic");
    }

    @ParameterizedTest
    @EnumSource(FanInSinkConfig.OutputMode.class)
    void l2_newTopicPickedUpWithoutRestart(FanInSinkConfig.OutputMode mode) throws Exception {
        String p = prefix("l2", mode);
        String target = p + "-out";
        createTopic(p + ".a", 1);
        createTopic(target, 3);
        startConnector(p, regex(p), target, mode, 1, Map.of());
        send(p + ".a", null, null, "warmup".getBytes(StandardCharsets.UTF_8), null);
        producer.flush();
        consume(target, 1, 60_000);

        long t0 = System.nanoTime();
        createTopic(p + ".d", 2);
        for (int i = 0; i < 20; i++) {
            send(p + ".d", i % 2, null, ("late" + i).getBytes(StandardCharsets.UTF_8), null);
        }
        producer.flush();
        List<ConsumerRecord<byte[], byte[]>> out = consume(target, 21, 90_000);
        double secs = (System.nanoTime() - t0) / 1e9;
        long fromD = out.stream().filter(r -> (p + ".d").equals(header(r, "fanin.source.topic"))).count();
        assertEquals(20, fromD);
        log("L2/" + mode, String.format("PASS: records of the new topic arrived %.1fs after its creation "
            + "(consumer.override.metadata.max.age.ms=5000), no restart", secs));
    }

    @ParameterizedTest
    @EnumSource(FanInSinkConfig.OutputMode.class)
    void l3_bytesPreserved(FanInSinkConfig.OutputMode mode) throws Exception {
        String p = prefix("l3", mode);
        String target = p + "-out";
        createTopic(p + ".a", 1);
        createTopic(target, 3);
        startConnector(p, regex(p), target, mode, 1, Map.of());

        Random rnd = new Random(42);
        byte[] big = new byte[1_000_000];
        rnd.nextBytes(big);
        byte[] binKey = new byte[64];
        rnd.nextBytes(binKey);
        byte[] hdr = new byte[33];
        rnd.nextBytes(hdr);

        RecordHeaders custom = new RecordHeaders();
        custom.add(new RecordHeader("h-binary", hdr));
        custom.add(new RecordHeader("h-dup", new byte[] {1}));
        custom.add(new RecordHeader("h-dup", new byte[] {2}));
        custom.add(new RecordHeader("h-empty", new byte[0]));
        custom.add(new RecordHeader("h-null", null));

        // offset -> expected (key, value, headers)
        Map<Long, byte[][]> expected = new LinkedHashMap<>();
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        byte[][][] cases = {
            {null, "null key".getBytes(StandardCharsets.UTF_8)},
            {"tombstone".getBytes(StandardCharsets.UTF_8), null},
            {binKey, big},
            {null, null},
            {new byte[0], new byte[0]},
        };
        List<byte[][]> caseList = new ArrayList<>(List.of(cases));
        for (byte[][] c : caseList) {
            futures.add(producer.send(new ProducerRecord<>(p + ".a", 0, null, c[0], c[1], custom)));
        }
        for (int i = 0; i < caseList.size(); i++) {
            expected.put(futures.get(i).get().offset(), caseList.get(i));
        }

        List<ConsumerRecord<byte[], byte[]>> out = consume(target, expected.size(), 60_000);
        assertEquals(expected.size(), out.size());
        for (ConsumerRecord<byte[], byte[]> r : out) {
            long off = Long.parseLong(header(r, "fanin.source.offset"));
            byte[][] want = expected.get(off);
            assertTrue(want != null, "unexpected source offset " + off);
            assertBytes(want[0], r.key(), "key @" + off);
            if (mode == FanInSinkConfig.OutputMode.HEADERS) {
                assertBytes(want[1], r.value(), "value @" + off);
            } else {
                JsonNode env = JSON.readTree(r.value());
                assertEquals(p + ".a", env.get("source_topic").asText());
                assertEquals(off, env.get("source_offset").asLong());
                assertBytes(want[0], env.get("key").isNull() ? null : Base64.getDecoder().decode(
                    env.get("key").asText()), "envelope key @" + off);
                assertBytes(want[1], env.get("value").isNull() ? null : Base64.getDecoder().decode(
                    env.get("value").asText()), "envelope value @" + off);
            }
            // original headers: same order, same bytes (fanin.source.* are appended after them)
            List<Header> got = new ArrayList<>();
            r.headers().forEach(h -> {
                if (!h.key().startsWith("fanin.source.")) {
                    got.add(h);
                }
            });
            assertEquals(5, got.size(), "original headers @" + off);
            assertEquals("h-binary", got.get(0).key());
            assertBytes(hdr, got.get(0).value(), "binary header");
            assertBytes(new byte[] {1}, got.get(1).value(), "dup 1");
            assertBytes(new byte[] {2}, got.get(2).value(), "dup 2");
            assertBytes(new byte[0], got.get(3).value(), "empty header");
            assertBytes(null, got.get(4).value(), "null header");
        }
        log("L3/" + mode, "PASS: null key, tombstone, null/null, empty, 1,000,000-byte value and 5 headers "
            + "(binary, duplicate, empty, null) are byte-identical");
    }

    @ParameterizedTest
    @EnumSource(FanInSinkConfig.OutputMode.class)
    void l4_orderingPerSourcePartition(FanInSinkConfig.OutputMode mode) throws Exception {
        String p = prefix("l4", mode);
        String target = p + "-out";
        int perPartition = 10_000;
        createTopic(p + ".a", 3);
        createTopic(target, 6);
        startConnector(p, regex(p), target, mode, 2, Map.of());

        for (int i = 0; i < perPartition; i++) {
            for (int part = 0; part < 3; part++) {
                send(p + ".a", part, null, ByteBuffer.allocate(8).putLong(i).array(), null);
            }
        }
        producer.flush();

        List<ConsumerRecord<byte[], byte[]>> out = consume(target, 3 * perPartition, 120_000);
        settle(3_000);
        out = consume(target, 3 * perPartition, 5_000);
        assertEquals(3 * perPartition, out.size());

        // The fan-in topic is read partition by partition; the order inside one fan-in partition is the order
        // a downstream reader sees. Check that, per source partition, source offsets are strictly increasing.
        Map<String, Long> last = new HashMap<>();
        Map<String, Integer> targetPartition = new HashMap<>();
        Map<String, Integer> counts = new TreeMap<>();
        for (ConsumerRecord<byte[], byte[]> r : sortedByTargetOffset(out)) {
            String src = header(r, "fanin.source.topic") + "-" + header(r, "fanin.source.partition");
            long off = Long.parseLong(header(r, "fanin.source.offset"));
            Long prev = last.put(src, off);
            assertTrue(prev == null || off > prev, "out of order for " + src + ": " + prev + " then " + off);
            Integer tp = targetPartition.putIfAbsent(src, r.partition());
            assertTrue(tp == null || tp == r.partition(), "source partition " + src + " split across target partitions");
            counts.merge(src, 1, Integer::sum);
        }
        counts.values().forEach(c -> assertEquals(perPartition, c));
        log("L4/" + mode, "PASS: " + out.size() + " records, " + counts.size() + " source partitions, order "
            + "preserved, each source partition on one target partition " + targetPartition.values());
    }

    // ------------------------------------------------------------------ L5

    @Test
    void l5_workerKilledMidStream() throws Exception {
        String p = prefix("l5", FanInSinkConfig.OutputMode.HEADERS);
        String target = p + "-out";
        int total = 20_000;
        createTopic(p + ".a", 3);
        createTopic(target, 3);
        startConnector(p, regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 2, Map.of());

        Thread feeder = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    send(p + ".a", i % 3, null, ByteBuffer.allocate(8).putLong(i).array(), null);
                    if (i % 500 == 499) {
                        producer.flush();
                        Thread.sleep(500);
                    }
                }
                producer.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        feeder.start();

        await("some records delivered", 120_000, () -> consume(target, 3_000, 2_000).size() >= 3_000);
        run("docker", "kill", "fanin-it-connect");
        log("L5", "worker killed (docker kill) mid-stream");
        feeder.join();
        Thread.sleep(3_000);
        run("docker", "start", "fanin-it-connect");
        await("connect back", 120_000, () -> {
            try {
                return http("GET", "/connectors/" + p + "/status", null).body().contains("RUNNING");
            } catch (Exception e) {
                return false;
            }
        });
        await("all tasks running", 60_000, () -> allTasksRunning(p, 2));

        // Duplicates are expected after the kill, so wait for distinct source records, not for a record count.
        Set<String> distinct = new HashSet<>();
        List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 180_000;
        while (System.currentTimeMillis() < deadline) {
            out = consume(target, total, 5_000);
            distinct.clear();
            out.forEach(r -> distinct.add(header(r, "fanin.source.partition") + ":" + header(r, "fanin.source.offset")));
            if (distinct.size() >= total) {
                break;
            }
            Thread.sleep(2_000);
        }
        settle(10_000);
        out = consume(target, total, 5_000);
        distinct.clear();
        out.forEach(r -> distinct.add(header(r, "fanin.source.partition") + ":" + header(r, "fanin.source.offset")));
        if (distinct.size() != total) {
            Map<String, Long> maxOffset = new TreeMap<>();
            distinct.forEach(d -> maxOffset.merge(d.split(":")[0], Long.parseLong(d.split(":")[1]), Math::max));
            log("L5", "MISSING " + (total - distinct.size()) + " records; highest source offset seen per partition "
                + maxOffset + "; target records " + out.size());
        }
        assertEquals(total, distinct.size(), "no record lost");
        log("L5", "PASS: no loss. " + out.size() + " records in target for " + total + " produced, duplicates = "
            + (out.size() - distinct.size()));
    }

    // ------------------------------------------------------------------ L6

    @Test
    void l6_targetTopicUnavailable() throws Exception {
        String p = prefix("l6", FanInSinkConfig.OutputMode.HEADERS);
        String target = p + "-out";
        createTopic(p + ".a", 1);
        createTopic(target, 3);
        startConnector(p, regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 1, Map.of(
            "fanin.producer.delivery.timeout.ms", "10000",
            "fanin.producer.request.timeout.ms", "3000",
            "fanin.producer.max.block.ms", "5000"));

        for (int i = 0; i < 1000; i++) {
            send(p + ".a", null, null, ("a" + i).getBytes(StandardCharsets.UTF_8), null);
        }
        producer.flush();
        consume(target, 1000, 60_000);
        settle(7_000); // let offsets be committed

        admin.deleteTopics(List.of(target)).all().get();
        log("L6", "target topic deleted");
        for (int i = 0; i < 1000; i++) {
            send(p + ".a", null, null, ("b" + i).getBytes(StandardCharsets.UTF_8), null);
        }
        producer.flush();

        long t0 = System.nanoTime();
        await("task FAILED", 120_000, () -> taskFailed(p));
        double failAfter = (System.nanoTime() - t0) / 1e9;
        String trace = taskTrace(p);
        log("L6", String.format("task FAILED %.0fs after the first undeliverable record. Trace starts: %s",
            failAfter, trace.substring(0, Math.min(trace.length(), 200)).replace('\n', ' ')));

        createTopic(target, 3);
        http("POST", "/connectors/" + p + "/tasks/0/restart", null);
        await("task running again", 60_000, () -> allTasksRunning(p, 1));
        List<ConsumerRecord<byte[], byte[]>> out = consume(target, 1000, 120_000);
        Set<String> values = new HashSet<>();
        out.forEach(r -> values.add(new String(r.value(), StandardCharsets.UTF_8)));
        for (int i = 0; i < 1000; i++) {
            assertTrue(values.contains("b" + i), "record b" + i + " lost after recovery");
        }
        log("L6", "PASS: task failed loudly, restart after the topic came back delivered all 1000 records "
            + "produced during the outage (" + out.size() + " in target)");
    }

    // ------------------------------------------------------------------ L7

    @Test
    void l7_regexMatchingTargetIsRejected() throws Exception {
        String name = "l7" + RUN;
        ObjectNode cfg = connectorConfig(name, "^fanin-.+", "fanin-out", FanInSinkConfig.OutputMode.HEADERS, 1,
            Map.of());
        HttpResponse<String> validate = http("PUT", "/connector-plugins/FanInSinkConnector/config/validate",
            JSON.writeValueAsString(cfg));
        JsonNode body = JSON.readTree(validate.body());
        assertTrue(body.get("error_count").asInt() > 0);
        String message = null;
        for (JsonNode c : body.get("configs")) {
            JsonNode errors = c.get("value").get("errors");
            if ("topics.regex".equals(c.get("value").get("name").asText()) && errors.size() > 0) {
                message = errors.get(0).asText();
            }
        }
        assertTrue(message != null && message.contains("matches the target topic"), String.valueOf(message));

        HttpResponse<String> create = http("PUT", "/connectors/" + name + "/config", JSON.writeValueAsString(cfg));
        connectors.add(name);
        assertEquals(400, create.statusCode(), create.body());
        assertTrue(create.body().contains("matches the target topic"), create.body());
        log("L7", "PASS: creation rejected with HTTP " + create.statusCode() + ": "
            + JSON.readTree(create.body()).get("message").asText().replace('\n', ' '));
    }

    // ------------------------------------------------------------------ L8

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"default", "roundrobin"})
    void l8_manyTopics(String assignor) throws Exception {
        String p = prefix("l8" + assignor.charAt(0), FanInSinkConfig.OutputMode.HEADERS);
        Map<String, String> extra = "roundrobin".equals(assignor)
            ? Map.of("consumer.override.partition.assignment.strategy",
                "org.apache.kafka.clients.consumer.RoundRobinAssignor")
            : Map.of();
        String target = p + "-out";
        int topics = 500;
        int perTopic = 200;
        int valueSize = 1024;
        createTopic(target, 12);
        List<NewTopic> newTopics = new ArrayList<>();
        for (int i = 0; i < topics; i++) {
            newTopics.add(new NewTopic(String.format("%s.t%03d", p, i), 3, (short) 1));
        }
        long tc = System.nanoTime();
        admin.createTopics(newTopics).all().get();
        log("L8/" + assignor, String.format("%d topics x 3 partitions created in %.1fs", topics,
            (System.nanoTime() - tc) / 1e9));

        startConnector(p, regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 4, extra);
        // Let the group stabilise on all 1500 partitions before measuring.
        await("tasks assigned", 120_000, () -> {
            try {
                return admin.describeConsumerGroups(List.of("connect-" + p)).all().get().get("connect-" + p)
                    .members().stream().mapToInt(m -> m.assignment().topicPartitions().size()).sum() == topics * 3;
            } catch (Exception e) {
                return false;
            }
        });
        Map<String, Integer> perMember = new TreeMap<>();
        admin.describeConsumerGroups(List.of("connect-" + p)).all().get().get("connect-" + p).members()
            .forEach(m -> perMember.put(m.consumerId().replaceAll("-[0-9a-f-]{36}$", ""),
                m.assignment().topicPartitions().size()));
        log("L8/" + assignor, "partitions per task: " + perMember + ", tasks: " + tasksState(p));

        byte[] payload = new byte[valueSize];
        new Random(1).nextBytes(payload);
        long expected = (long) topics * perTopic;
        long t0 = System.nanoTime();
        for (int n = 0; n < perTopic; n++) {
            for (int i = 0; i < topics; i++) {
                send(String.format("%s.t%03d", p, i), n % 3, null, payload, null);
            }
        }
        producer.flush();
        double produceSecs = (System.nanoTime() - t0) / 1e9;
        List<ConsumerRecord<byte[], byte[]>> out = consume(target, (int) expected, 300_000);
        double secs = (System.nanoTime() - t0) / 1e9;
        assertEquals(expected, out.size());
        log("L8/" + assignor, String.format("PASS: %d records (%.0f MB) end to end in %.1fs = %.0f records/s, %.1f MB/s "
            + "(produced in %.1fs). Single-node docker, tasks.max=4", expected, expected * valueSize / 1e6, secs,
            expected / secs, expected * valueSize / 1e6 / secs, produceSecs));

        // time to pick up newly created topics
        List<Double> pickup = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String late = p + ".late" + i;
            long t1 = System.nanoTime();
            createTopic(late, 3);
            send(late, 0, null, "late".getBytes(StandardCharsets.UTF_8), null);
            producer.flush();
            int want = (int) expected + i + 1;
            consume(target, want, 120_000);
            pickup.add((System.nanoTime() - t1) / 1e9);
        }
        log("L8/" + assignor, "time until a record on a newly created topic arrives (500 topics subscribed, "
            + "metadata.max.age.ms=5000), seconds: " + pickup);
    }

    // ------------------------------------------------------------------ helpers

    static String prefix(String test, FanInSinkConfig.OutputMode mode) {
        return (test + mode.name().charAt(0) + RUN).toLowerCase();
    }

    static String regex(String prefix) {
        return "^" + prefix.replace(".", "\\.") + "\\..+";
    }

    void startConnector(String name, String regex, String target, FanInSinkConfig.OutputMode mode, int tasks,
                        Map<String, String> extra) throws Exception {
        ObjectNode cfg = connectorConfig(name, regex, target, mode, tasks, extra);
        HttpResponse<String> r = http("PUT", "/connectors/" + name + "/config", JSON.writeValueAsString(cfg));
        connectors.add(name);
        assertTrue(r.statusCode() < 300, "create connector failed: " + r.statusCode() + " " + r.body());
        await("connector " + name + " running", 60_000, () -> allTasksRunning(name, tasks));
    }

    static ObjectNode connectorConfig(String name, String regex, String target, FanInSinkConfig.OutputMode mode,
                                      int tasks, Map<String, String> extra) {
        ObjectNode cfg = JSON.createObjectNode();
        cfg.put("connector.class", "com.example.fanin.FanInSinkConnector");
        cfg.put("tasks.max", Integer.toString(tasks));
        cfg.put("topics.regex", regex);
        cfg.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        cfg.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        cfg.put("header.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        cfg.put("consumer.override.metadata.max.age.ms", "5000");
        cfg.put("fanin.target.topic", target);
        cfg.put("fanin.target.bootstrap.servers", "broker:9092");
        cfg.put("fanin.target.api.key", "local");
        cfg.put("fanin.target.api.secret", "local");
        cfg.put("fanin.output.mode", mode.name());
        cfg.put("fanin.producer.security.protocol", "PLAINTEXT");
        cfg.put("fanin.producer.max.request.size", "4194304");
        extra.forEach(cfg::put);
        return cfg;
    }

    static boolean allTasksRunning(String name, int tasks) {
        try {
            JsonNode status = JSON.readTree(http("GET", "/connectors/" + name + "/status", null).body());
            JsonNode t = status.get("tasks");
            if (t == null || t.size() != tasks) {
                return false;
            }
            for (JsonNode task : t) {
                if (!"RUNNING".equals(task.get("state").asText())) {
                    return false;
                }
            }
            return "RUNNING".equals(status.get("connector").get("state").asText());
        } catch (Exception e) {
            return false;
        }
    }

    static String tasksState(String name) throws Exception {
        List<String> states = new ArrayList<>();
        for (JsonNode task : JSON.readTree(http("GET", "/connectors/" + name + "/status", null).body())
            .get("tasks")) {
            states.add(task.get("state").asText());
        }
        return states.toString();
    }

    static boolean taskFailed(String name) {
        try {
            for (JsonNode task : JSON.readTree(http("GET", "/connectors/" + name + "/status", null).body())
                .get("tasks")) {
                if ("FAILED".equals(task.get("state").asText())) {
                    return true;
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return false;
    }

    static String taskTrace(String name) throws Exception {
        for (JsonNode task : JSON.readTree(http("GET", "/connectors/" + name + "/status", null).body())
            .get("tasks")) {
            if (task.has("trace")) {
                return task.get("trace").asText();
            }
        }
        return "";
    }

    static void createTopic(String name, int partitions) throws Exception {
        admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
    }

    static Future<RecordMetadata> send(String topic, Integer partition, byte[] key, byte[] value,
                                       Iterable<Header> headers) {
        return producer.send(new ProducerRecord<>(topic, partition, null, key, value, headers));
    }

    /** Reads the whole topic from the beginning until at least {@code atLeast} records or the timeout. */
    static List<ConsumerRecord<byte[], byte[]>> consume(String topic, int atLeast, long timeoutMs) {
        Properties c = new Properties();
        c.put("bootstrap.servers", BOOTSTRAP);
        c.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        c.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        c.put("max.partition.fetch.bytes", "8388608");
        c.put("fetch.max.bytes", "52428800");
        List<ConsumerRecord<byte[], byte[]>> all = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(c)) {
            List<TopicPartition> parts = new ArrayList<>();
            consumer.partitionsFor(topic).forEach(pi -> parts.add(new TopicPartition(topic, pi.partition())));
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (all.size() < atLeast && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(all::add);
            }
            // Read on to the end offsets seen now, so duplicates cannot hide records that are further back and
            // extra (unexpected) records are always seen.
            Map<TopicPartition, Long> end = consumer.endOffsets(parts);
            long endDeadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < endDeadline
                && parts.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                consumer.poll(Duration.ofMillis(200)).forEach(all::add);
            }
        }
        return all;
    }

    static List<ConsumerRecord<byte[], byte[]>> sortedByTargetOffset(List<ConsumerRecord<byte[], byte[]>> in) {
        List<ConsumerRecord<byte[], byte[]>> sorted = new ArrayList<>(in);
        sorted.sort((a, b) -> a.partition() != b.partition() ? Integer.compare(a.partition(), b.partition())
            : Long.compare(a.offset(), b.offset()));
        return sorted;
    }

    static String header(ConsumerRecord<byte[], byte[]> r, String key) {
        Header h = r.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    static void assertBytes(byte[] expected, byte[] actual, String what) {
        if (expected == null) {
            assertTrue(actual == null, what + ": expected null");
        } else {
            assertTrue(actual != null && java.util.Arrays.equals(expected, actual), what + ": bytes differ");
        }
    }

    static HttpResponse<String> http(String method, String path, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(CONNECT + path)).timeout(Duration.ofSeconds(30));
        if (body == null) {
            b.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            b.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    static void await(String what, long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("timeout waiting for " + what);
    }

    static void settle(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }

    static void run(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, p.waitFor(), String.join(" ", cmd) + ": " + out);
    }

    static void log(String id, String message) {
        System.out.println("RESULT " + id + " " + message);
    }

    // ------------------------------------------------------------------ L10-L14: fanin.start.timestamp

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static List<String> values(List<ConsumerRecord<byte[], byte[]>> records) {
        List<String> out = new ArrayList<>();
        records.forEach(r -> out.add(new String(r.value(), StandardCharsets.UTF_8)));
        return out;
    }

    static void produce(String topic, String valuePrefix, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            send(topic, i % 2, null, utf8(valuePrefix + i), null);
        }
        producer.flush();
    }

    /** Sum of the offsets the connector's consumer group has committed on a topic. */
    static long committedOn(String group, String topic) throws Exception {
        long sum = 0;
        for (var e : admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().entrySet()) {
            if (e.getKey().topic().equals(topic)) {
                sum += e.getValue().offset();
            }
        }
        return sum;
    }

    /** Topic with an old backlog, then a point in time, then newer records. Returns the point in time. */
    static long backlogThenNow(String topic, int oldCount, int newCount) throws Exception {
        produce(topic, "old-", oldCount);
        settle(1_500);
        long t = System.currentTimeMillis();
        settle(300);
        produce(topic, "new-", newCount);
        return t;
    }

    @Test
    void l10_startTimestampSkipsTheBacklog() throws Exception {
        String p = prefix("l10", FanInSinkConfig.OutputMode.HEADERS);
        String target = p + "-out";
        createTopic(p + ".a", 2);
        createTopic(target, 3);
        long t = backlogThenNow(p + ".a", 100, 50);

        startConnector(p, regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 1,
            Map.of("fanin.start.timestamp", Long.toString(t)));

        consume(target, 50, 60_000);
        settle(5_000);
        List<String> out = values(consume(target, 50, 5_000));
        assertEquals(50, out.size(), "only the records after the start timestamp");
        assertTrue(out.stream().allMatch(v -> v.startsWith("new-")), "no record from the old backlog: " + out);
        log("L10", "PASS: 100 old records skipped, the 50 records after the start timestamp delivered");
    }

    @Test
    void l11_restartResumesFromCommittedOffsetsAndDoesNotRewind() throws Exception {
        String p = prefix("l11", FanInSinkConfig.OutputMode.HEADERS);
        String target = p + "-out";
        createTopic(p + ".a", 2);
        createTopic(target, 3);
        long t0 = System.currentTimeMillis() - 60_000; // far before every record: a rewind would replay everything
        startConnector(p, regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 1,
            Map.of("fanin.start.timestamp", Long.toString(t0)));

        produce(p + ".a", "first-", 60);
        consume(target, 60, 60_000);
        await("offsets of the 60 records committed", 60_000, () -> {
            try {
                return committedOn("connect-" + p, p + ".a") >= 60;
            } catch (Exception e) {
                return false;
            }
        });

        HttpResponse<String> restart = http("POST", "/connectors/" + p + "/restart?includeTasks=true&onlyFailed=false",
            null);
        assertTrue(restart.statusCode() < 300, "restart failed: " + restart.statusCode() + " " + restart.body());
        await("tasks running after restart", 60_000, () -> allTasksRunning(p, 1));
        produce(p + ".a", "second-", 20);

        consume(target, 80, 60_000);
        settle(5_000);
        List<ConsumerRecord<byte[], byte[]>> out = consume(target, 80, 5_000);
        Set<String> origins = new HashSet<>();
        out.forEach(r -> origins.add(header(r, "fanin.source.partition") + "@" + header(r, "fanin.source.offset")));
        assertEquals(80, out.size(), "no replay after the restart and no loss");
        assertEquals(80, origins.size(), "no duplicates");
        log("L11", "PASS: after a restart the connector resumed at the committed offsets: 80 records, 0 duplicates, "
            + "although the start timestamp lies before every record");
    }

    @Test
    void l12_newTopicIsReadFromItsFirstRecordEvenWithOffsetResetLatest() throws Exception {
        String p = prefix("l12", FanInSinkConfig.OutputMode.HEADERS);
        String target = p + "-out";
        createTopic(p + ".a", 1);
        createTopic(target, 3);
        long t = System.currentTimeMillis();
        // metadata.max.age.ms is 5 s in these tests, so the new topic is found a few seconds after it is created.
        startConnector(p, regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 1,
            Map.of("fanin.start.timestamp", Long.toString(t), "consumer.override.auto.offset.reset", "latest"));
        send(p + ".a", null, null, utf8("warmup"), null);
        producer.flush();
        consume(target, 1, 60_000);

        createTopic(p + ".d", 2);
        produce(p + ".d", "late-", 20); // produced before the connector has found the topic
        List<String> out = values(consume(target, 21, 90_000));
        long late = out.stream().filter(v -> v.startsWith("late-")).count();
        assertEquals(20, late, "records produced before the topic was discovered must not be skipped");
        log("L12", "PASS: with auto.offset.reset=latest the 20 records of a new topic were still delivered "
            + "from the first record");
    }

    @Test
    void l13_recreatedConnectorStartsAtTheTimestampNotAtTheBeginning() throws Exception {
        String p = prefix("l13", FanInSinkConfig.OutputMode.HEADERS);
        String target = p + "-out";
        createTopic(p + ".a", 2);
        createTopic(target, 3);
        long t = backlogThenNow(p + ".a", 30, 20);
        Map<String, String> extra = Map.of("fanin.start.timestamp", Long.toString(t));

        startConnector(p + "x", regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 1, extra);
        consume(target, 20, 60_000);
        http("DELETE", "/connectors/" + p + "x", null);
        settle(3_000);
        startConnector(p + "y", regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 1, extra); // new group

        consume(target, 40, 60_000);
        settle(5_000);
        List<String> out = values(consume(target, 40, 5_000));
        assertEquals(40, out.size(), "the 20 new records twice (a new group has no offsets), none of the 30 old ones");
        assertTrue(out.stream().allMatch(v -> v.startsWith("new-")), "no record before the timestamp: " + out);
        log("L13", "PASS: a recreated connector (new group) replayed only from the start timestamp: 40 = 20 + 20 "
            + "duplicates, 0 of the 30 old records");
    }

    @Test
    void l14_wrongGroupNameIsIgnoredWithoutRewinding() throws Exception {
        String p = prefix("l14", FanInSinkConfig.OutputMode.HEADERS);
        String target = p + "-out";
        createTopic(p + ".a", 2);
        createTopic(target, 3);
        long t = backlogThenNow(p + ".a", 30, 20);

        startConnector(p, regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 1,
            Map.of("fanin.start.timestamp", Long.toString(t), "fanin.start.consumer.group", "no-such-group"));

        consume(target, 50, 60_000);
        settle(5_000);
        List<String> out = values(consume(target, 50, 5_000));
        assertEquals(50, out.size(), "unknown group: the setting is ignored and the default start (earliest) applies");
        assertFalse(taskFailed(p), "the task must not fail because of the unknown group");
        log("L14", "PASS: an unknown consumer group name is ignored (warning), the task keeps running and "
            + "starts from the default position");
    }

    // ------------------------------------------------------------------ L15, L16: fanin.timestamp.mode

    /** 2026-01-01T00:00Z: far in the past compared with the time of the test. */
    static final long OLD_TS = 1_767_225_600_000L;

    List<ConsumerRecord<byte[], byte[]>> copyThreeRecordsWithTimestamp(String test, Map<String, String> extra)
        throws Exception {
        String p = prefix(test, FanInSinkConfig.OutputMode.HEADERS);
        String target = p + "-out";
        createTopic(p + ".a", 1);
        createTopic(target, 1);
        startConnector(p, regex(p), target, FanInSinkConfig.OutputMode.HEADERS, 1, extra);
        for (int i = 0; i < 3; i++) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(p + ".a", 0, OLD_TS, null,
                utf8("v" + i))).get();
        }
        producer.flush();
        // the broker deletes records with old timestamps soon, so read at once
        List<ConsumerRecord<byte[], byte[]>> out = consume(target, 3, 60_000);
        assertEquals(3, out.size());
        out.forEach(r -> assertEquals(Long.toString(OLD_TS), header(r, "fanin.source.timestamp"),
            "the header keeps the original time in every mode"));
        return out;
    }

    @Test
    void l15_timestampModeSourceKeepsTheSourceTime() throws Exception {
        List<ConsumerRecord<byte[], byte[]>> out = copyThreeRecordsWithTimestamp("l15", Map.of());
        out.forEach(r -> assertEquals(OLD_TS, r.timestamp(), "default SOURCE: the source timestamp is kept"));
        log("L15", "PASS: default timestamp mode SOURCE kept the source timestamp " + OLD_TS + " on all 3 records");
    }

    @Test
    void l16_timestampModeCopyUsesTheTimeOfTheCopy() throws Exception {
        long before = System.currentTimeMillis();
        List<ConsumerRecord<byte[], byte[]>> out =
            copyThreeRecordsWithTimestamp("l16", Map.of("fanin.timestamp.mode", "COPY"));
        long after = System.currentTimeMillis();
        out.forEach(r -> assertTrue(r.timestamp() >= before && r.timestamp() <= after,
            "COPY: the timestamp is the time of the copy, was " + r.timestamp() + " not in [" + before + ", " + after
                + "]"));
        log("L16", "PASS: timestamp mode COPY gave the time of the copy on all 3 records, the header kept " + OLD_TS);
    }

    @Test
    void zz_sanityNoLeftoverFailedConnectors() throws Exception {
        // every connector created by this class is deleted in @AfterEach; assert the worker is still healthy
        assertFalse(http("GET", "/connectors", null).body().isBlank());
    }
}
