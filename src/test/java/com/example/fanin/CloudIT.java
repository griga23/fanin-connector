package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Confluent Cloud tests C1 to C5 and C7 (C6, the 50 MB/s test, was dropped on request).
 * Run with scripts/cc-test.sh. It needs a deployed connector (scripts/cc-deploy.sh) and these variables:
 * FANIN_API_KEY, FANIN_API_SECRET, FANIN_BOOTSTRAP, FANIN_ENV, FANIN_CLUSTER, FANIN_CONNECTOR_ID.
 * The tests share one connector and run in order. C1, C2 and C3 share one batch of new topics, so one
 * topic-discovery wait covers all three.
 */
@Tag("cloud")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CloudIT {

    static final String TARGET = "fanin-test-out";
    static final String RUN = System.getenv("FANIN_RUN") != null ? System.getenv("FANIN_RUN")
        : Long.toString(System.currentTimeMillis() % 100_000_000L, 36);
    static final String ENV_ID = env("FANIN_ENV");
    static final String CLUSTER = env("FANIN_CLUSTER");
    static final String BOOTSTRAP = env("FANIN_BOOTSTRAP");

    static String connectorId = env("FANIN_CONNECTOR_ID");
    static KafkaProducer<byte[], byte[]> producer;
    static Tail tail;

    static final String C1 = "fanin-test-src.c1" + RUN + "-";
    static final String C1_NOMATCH = "fanin-test-nomatch.c1" + RUN + "-";
    static final String C2 = "fanin-test-src.c2" + RUN + "-";
    static final String C3 = "fanin-test-src.c3" + RUN;
    static final String C4 = "fanin-test-src.c4" + RUN;
    static final String C5 = "fanin-test-src.c5" + RUN;
    static final String C7 = "fanin-test-src.c7" + RUN;

    static String env(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("environment variable " + name + " is not set");
        }
        return v;
    }

    @BeforeAll
    static void setUp() throws Exception {
        producer = new KafkaProducer<>(clientProps(true));
        tail = new Tail();
        log("setup", "run id " + RUN + ", connector " + connectorId);
    }

    @AfterAll
    static void tearDown() {
        if (tail != null) {
            tail.close();
        }
        producer.close(Duration.ofSeconds(10));
    }

    // ------------------------------------------------------------------ C1, C2, C3 (one discovery batch)

    @Test
    @Order(1)
    void c1_c2_c3_prefixDiscoveryAndOrdering() throws Exception {
        List<String> create = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            create.add(C1 + i + ":3");
        }
        for (int i = 0; i < 2; i++) {
            create.add(C1_NOMATCH + i + ":3");
        }
        for (int i = 0; i < 20; i++) {
            create.add(C2 + i + ":3");
        }
        create.add(C3 + ":3");
        create.add(C4 + ":3");
        create.add(C5 + ":6");
        create.add(C7 + ":3");
        createTopics(create);
        long t0 = System.nanoTime();
        log("C1-C3", "created " + create.size() + " topics (5 matching + 2 non-matching C1, 20 C2, C3, C4, C5, C7)");

        // C1: 20 records per topic. C2: 5 per topic. Warm-up records for C4, C5, C7 prove they are subscribed.
        for (int i = 0; i < 5; i++) {
            for (int n = 0; n < 20; n++) {
                send(C1 + i, null, "k" + n, "c1-" + i + "-" + n);
            }
        }
        for (int i = 0; i < 2; i++) {
            for (int n = 0; n < 20; n++) {
                send(C1_NOMATCH + i, null, "k" + n, "nomatch-" + i + "-" + n);
            }
        }
        for (int i = 0; i < 20; i++) {
            for (int n = 0; n < 5; n++) {
                send(C2 + i, null, "k" + n, "c2-" + i + "-" + n);
            }
        }
        for (String warm : List.of(C4, C5, C7)) {
            send(warm, 0, "warm", "warm");
        }

        // C3: an initial burst of 5,000 records per source partition, then a steady 300 records/s on top.
        Seq seq = new Seq(C3, 3);
        seq.burst(5_000);
        Thread feeder = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    seq.burst(100);
                    Thread.sleep(1_000);
                }
            } catch (InterruptedException e) {
                // stop
            }
        });
        feeder.start();
        producer.flush();

        // Wait until C1, C2 and the warm-up records arrived, then stop the C3 feeder and drain.
        await("C1/C2/warm-up records", 25 * 60_000L, () ->
            tail.count(C1) >= 100 && tail.topicCount(C2) >= 20 && tail.count(C2) >= 100
                && tail.count(C4) >= 1 && tail.count(C5) >= 1 && tail.count(C7) >= 1);
        feeder.interrupt();
        feeder.join();
        producer.flush();
        long produced = seq.total();
        await("C3 drained", 15 * 60_000L, () -> tail.distinct(C3) >= produced);
        Thread.sleep(60_000); // look for records that should not be there

        // C1
        long match = tail.count(C1);
        long noMatch = tail.count(C1_NOMATCH);
        assertEquals(100, match, "C1: records from the 5 matching topics (exactly 100, no duplicates)");
        assertEquals(0, noMatch, "C1: records from non-matching topics");
        log("C1", "PASS: " + match + " records from 5 matching topics, " + noMatch + " from 2 non-matching topics");

        // C2
        List<Double> delays = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Long seen = tail.firstSeen.get(C2 + i);
            assertTrue(seen != null, "C2 topic " + i + " never arrived");
            delays.add((seen - t0) / 1e9);
        }
        Collections.sort(delays);
        assertEquals(100, tail.count(C2));
        log("C2", String.format("PASS: all 20 new topics (100 records) picked up. Seconds from topic creation to first "
            + "record in the fan-in topic: min %.0f, median %.0f, max %.0f", delays.get(0), delays.get(10),
            delays.get(19)));

        // C3
        checkOrdering("C3", C3, 3, seq);
    }

    // ------------------------------------------------------------------ C4

    @Test
    @Order(2)
    void c4_restart() throws Exception {
        Seq seq = new Seq(C4, 3);

        // Phase 1: pause and resume under load.
        Thread feeder = feeder(seq, 125, 500, 20_000);
        await("first C4 records delivered", 10 * 60_000L, () -> tail.distinct(C4) >= 3_000);
        cli("connect", "cluster", "pause", connectorId, "--cluster", CLUSTER, "--environment", ENV_ID);
        await("connector PAUSED", 10 * 60_000L, () -> "PAUSED".equals(status()[0]));
        Thread.sleep(20_000); // in-flight records
        long atPause = tail.count(C4);
        Thread.sleep(60_000);
        long duringPause = tail.count(C4) - atPause;
        log("C4", "paused: " + duringPause + " records delivered during 60 s of pause (source kept producing)");
        feeder.join();
        cli("connect", "cluster", "resume", connectorId, "--cluster", CLUSTER, "--environment", ENV_ID);
        await("connector RUNNING", 10 * 60_000L, () -> "RUNNING".equals(status()[0]));
        long expected1 = seq.total();
        await("C4 all delivered after resume", 10 * 60_000L, () -> tail.distinct(C4) >= expected1);
        Thread.sleep(30_000);
        long dups1 = tail.count(C4) - tail.distinct(C4);
        assertEquals(expected1, tail.distinct(C4), "C4 pause/resume: records lost");
        log("C4", "PASS pause/resume: no loss, " + expected1 + " produced, " + tail.count(C4) + " in target, duplicates = "
            + dups1);

        // Phase 2: redeploy (delete and create again under the same name) while records keep coming.
        seq.burst(5_000, 1);
        producer.flush();
        cli("connect", "cluster", "delete", connectorId, "--cluster", CLUSTER, "--environment", ENV_ID, "--force");
        log("C4", "connector deleted; producing 5,000 records while it does not exist");
        seq.burst(5_000, 1);
        producer.flush();
        redeploy(Map.of());
        seq.burst(5_000, 1);
        producer.flush();
        long expected2 = seq.total();
        await("C4 all delivered after redeploy", 15 * 60_000L, () -> tail.distinct(C4) >= expected2);
        Thread.sleep(60_000);
        long dups2 = tail.count(C4) - tail.distinct(C4);
        assertEquals(expected2, tail.distinct(C4), "C4 redeploy: records lost");
        log("C4", "PASS redeploy: no loss, " + expected2 + " produced in total, " + tail.count(C4)
            + " in target, duplicates since the pause/resume check = " + (dups2 - dups1));
    }

    // ------------------------------------------------------------------ C5

    @Test
    @Order(3)
    void c5_scaleTasks() throws Exception {
        Seq seq = new Seq(C5, 6);
        log("C5", "tasks before: " + java.util.Arrays.toString(status()));
        seq.burst(2_000, 1);
        producer.flush();
        await("C5 initial records", 10 * 60_000L, () -> tail.distinct(C5) >= 2_001);

        Thread feeder = feeder(seq, 100, 500, 20_000);
        long t0 = System.nanoTime();
        redeployInPlace(Map.of("TASKS", "4"));
        await("4 tasks RUNNING", 15 * 60_000L, () -> {
            String[] s = status();
            return "RUNNING".equals(s[0]) && s.length == 5 && "RUNNING".equals(s[1]) && "RUNNING".equals(s[2])
                && "RUNNING".equals(s[3]) && "RUNNING".equals(s[4]);
        });
        log("C5", String.format("%.0f s from the update call until connector and 4 tasks are RUNNING",
            (System.nanoTime() - t0) / 1e9));
        feeder.join();
        long expected = seq.total();
        await("C5 all delivered", 15 * 60_000L, () -> tail.distinct(C5) >= expected);
        Thread.sleep(30_000);
        assertEquals(expected, tail.distinct(C5), "C5: records lost");
        log("C5", "PASS: tasks.max 1 -> 4, no loss, " + expected + " produced, " + tail.count(C5) + " in target, "
            + "duplicates = " + (tail.count(C5) - tail.distinct(C5)));
        try {
            String g = "connect-" + connectorId;
            String desc = cli("kafka", "consumer", "group", "describe", g, "--cluster", CLUSTER, "--environment",
                ENV_ID);
            log("C5", "consumer group " + g + ": " + desc.replace('\n', ' ').replaceAll("\\s+", " "));
        } catch (RuntimeException e) {
            log("C5", "consumer group describe failed: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------ C7

    @Test
    @Order(4)
    void c7_largeRecords() throws Exception {
        byte[] big = new byte[2_000_000];
        new Random(7).nextBytes(big);
        try {
            log("C7", "target topic config: " + cli("kafka", "topic", "configuration", "list", TARGET, "--cluster",
                CLUSTER, "--environment", ENV_ID).lines().filter(l -> l.contains("max.message.bytes"))
                .findFirst().orElse("?").replaceAll("\\s+", " "));
        } catch (RuntimeException e) {
            log("C7", "could not read topic config: " + e.getMessage());
        }
        producer.send(new ProducerRecord<>(C7, 1, null, "big".getBytes(StandardCharsets.UTF_8), big)).get();
        log("C7", "2,000,000-byte record produced to the source topic (source accepts it)");

        // With the default fanin.producer.max.request.size (1 MB) the connector's producer must reject it.
        boolean arrived = false;
        boolean failed = false;
        long deadline = System.currentTimeMillis() + 8 * 60_000L;
        while (System.currentTimeMillis() < deadline && !arrived && !failed) {
            arrived = tail.hasBigRecord();
            failed = java.util.Arrays.asList(status()).contains("FAILED")
                || java.util.Arrays.asList(status()).contains("SYSTEM_ERROR");
            Thread.sleep(5_000);
        }
        log("C7", "default config (fanin.producer.max.request.size 1 MB): " + (arrived ? "record arrived"
            : failed ? "task(s) failed " + java.util.Arrays.toString(status())
            : "no arrival and no failure after 8 min"));
        try {
            String logs = cli("connect", "logs", connectorId, "--level", "ERROR", "--start-time",
                Instant.now().minus(Duration.ofMinutes(20)).truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString(),
                "--end-time", Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString(),
                "--cluster", CLUSTER, "--environment", ENV_ID);
            String hit = logs.lines().filter(l -> l.matches("(?i).*(too large|max\\.request\\.size|RecordTooLarge).*"))
                .findFirst().orElse("(no matching line)");
            log("C7", "error log: " + hit.trim().replaceAll("\\s+", " "));
        } catch (RuntimeException e) {
            log("C7", "could not read logs: " + e.getMessage());
        }

        // With a larger producer limit it must work. (TASKS=4 keeps the scale set in C5.)
        redeployInPlace(Map.of("TASKS", "4", "EXTRA_JSON", "{\"fanin.producer.max.request.size\":\"4194304\"}"));
        verifyBigRecord(big);
    }

    /** Verifies, without changing anything, that the 2,000,000-byte record of C7 is in the fan-in topic. */
    @Test
    @Order(5)
    void c7b_verifyBigRecordArrived() throws Exception {
        byte[] big = new byte[2_000_000];
        new Random(7).nextBytes(big);
        verifyBigRecord(big);
    }

    static void verifyBigRecord(byte[] big) throws Exception {
        await("C7 big record delivered", 20 * 60_000L, () -> tail.hasBigRecord());
        byte[] got = null;
        for (Rec r : tail.recs()) {
            if (r.topic.equals(C7) && r.srcPartition == 1) {
                got = r.value;
            }
        }
        assertTrue(got != null && java.util.Arrays.equals(big, got), "C7: 2,000,000-byte value differs");
        log("C7", "PASS with fanin.producer.max.request.size=4194304: the 2,000,000-byte record arrives byte-identical");
    }

    // ------------------------------------------------------------------ helpers

    static Properties clientProps(boolean producer) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", "PLAIN");
        p.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
            + env("FANIN_API_KEY") + "\" password=\"" + env("FANIN_API_SECRET") + "\";");
        if (producer) {
            p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            p.put("max.request.size", "5242880");
            p.put("linger.ms", "10");
            p.put("batch.size", "262144");
            p.put("compression.type", "lz4");
        } else {
            p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put("max.partition.fetch.bytes", "8388608");
        }
        return p;
    }

    static void send(String topic, Integer partition, String key, String value) {
        producer.send(new ProducerRecord<>(topic, partition, null, key.getBytes(StandardCharsets.UTF_8),
            value.getBytes(StandardCharsets.UTF_8)));
    }

    /** Numbered records over the partitions of one topic; remembers the highest acknowledged offset. */
    static class Seq {
        final String topic;
        final int partitions;
        final AtomicLong[] acked;
        final AtomicLong[] nextSeq;

        Seq(String topic, int partitions) {
            this.topic = topic;
            this.partitions = partitions;
            this.acked = new AtomicLong[partitions];
            this.nextSeq = new AtomicLong[partitions];
            for (int i = 0; i < partitions; i++) {
                acked[i] = new AtomicLong(-1);
                nextSeq[i] = new AtomicLong();
            }
        }

        /** {@code perPartition} records into every partition. */
        void burst(int perPartition) {
            burst(perPartition, partitions);
        }

        /** {@code perPartition} records into each of the first {@code usePartitions} partitions. */
        void burst(int perPartition, int usePartitions) {
            byte[] payload = new byte[100];
            for (int n = 0; n < perPartition; n++) {
                for (int p = 0; p < usePartitions; p++) {
                    long s = nextSeq[p].getAndIncrement();
                    byte[] v = ByteBuffer.allocate(108).putLong(s).put(payload).array();
                    final int part = p;
                    producer.send(new ProducerRecord<>(topic, part, null, null, v), (md, e) -> {
                        if (e == null) {
                            acked[part].accumulateAndGet(md.offset(), Math::max);
                        }
                    });
                }
            }
        }

        /** Records acknowledged so far, i.e. the count that must reach the fan-in topic. */
        long total() {
            producer.flush();
            long t = 0;
            for (AtomicLong a : acked) {
                t += a.get() + 1;
            }
            return t;
        }
    }

    /** Sends {@code perBatch} records to each partition every {@code intervalMs} until {@code maxPerPartition}. */
    static Thread feeder(Seq seq, int perBatch, long intervalMs, int maxPerPartition) {
        Thread t = new Thread(() -> {
            try {
                for (int sent = 0; sent < maxPerPartition; sent += perBatch) {
                    seq.burst(perBatch);
                    Thread.sleep(intervalMs);
                }
                producer.flush();
            } catch (InterruptedException e) {
                // stop
            }
        });
        t.start();
        return t;
    }

    void checkOrdering(String id, String topic, int partitions, Seq seq) {
        Map<String, Long> last = new HashMap<>();
        Map<String, Integer> targetPartition = new HashMap<>();
        Map<String, Long> counts = new TreeMap<>();
        List<Rec> sorted = new ArrayList<>(tail.recs());
        sorted.removeIf(r -> !r.topic.equals(topic));
        sorted.sort((a, b) -> a.partition != b.partition ? Integer.compare(a.partition, b.partition)
            : Long.compare(a.offset, b.offset));
        long outOfOrder = 0;
        long splits = 0;
        for (Rec r : sorted) {
            String src = r.topic + "-" + r.srcPartition;
            Long prev = last.put(src, r.srcOffset);
            if (prev != null && r.srcOffset <= prev) {
                outOfOrder++;
            }
            Integer tp = targetPartition.putIfAbsent(src, r.partition);
            if (tp != null && tp != r.partition) {
                splits++;
            }
            counts.merge(src, 1L, Long::sum);
        }
        assertEquals(0, outOfOrder, id + ": out-of-order records");
        assertEquals(0, splits, id + ": source partition spread over several target partitions");
        assertEquals(seq.total(), tail.distinct(topic), id + ": distinct records");
        assertEquals(seq.total(), sorted.size(), id + ": records including duplicates");
        log(id, "PASS: " + sorted.size() + " records over " + partitions + " source partitions " + counts
            + ", order preserved, each source partition on one target partition " + targetPartition.values());
    }

    static void createTopics(List<String> nameAndPartitions) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<String>> results = new ArrayList<>();
        for (String np : nameAndPartitions) {
            String[] parts = np.split(":");
            results.add(pool.submit(() -> cli("kafka", "topic", "create", parts[0], "--partitions", parts[1],
                "--environment", ENV_ID, "--cluster", CLUSTER)));
        }
        for (Future<String> f : results) {
            f.get();
        }
        pool.shutdown();
    }

    /** Returns [connector status, task 0 state, task 1 state, ...]. */
    static String[] status() {
        String out = cli("connect", "cluster", "describe", connectorId, "--cluster", CLUSTER, "--environment", ENV_ID);
        List<String> states = new ArrayList<>();
        Matcher s = Pattern.compile("\\|\\s*Status\\s*\\|\\s*(\\w+)").matcher(out);
        states.add(s.find() ? s.group(1) : "UNKNOWN");
        Matcher t = Pattern.compile("(?m)^\\s+(\\d+)\\s*\\|\\s*(\\w+)\\s*$").matcher(out);
        while (t.find()) {
            states.add(t.group(2));
        }
        return states.toArray(new String[0]);
    }

    /** Deletes nothing: creates the connector again (the old one must have been deleted) and waits for RUNNING. */
    static void redeploy(Map<String, String> extraEnv) throws Exception {
        runScript(extraEnv);
        String json = new String(java.nio.file.Files.readAllBytes(
            new File("cc-state/connector-fanin-test-connector.json").toPath()), StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("\"id\"\\s*:\\s*\"(clcc-[^\"]+)\"").matcher(json);
        assertTrue(m.find(), "no connector id after redeploy");
        connectorId = m.group(1);
        long t0 = System.nanoTime();
        await("redeployed connector RUNNING", 15 * 60_000L, () -> "RUNNING".equals(status()[0]));
        log("C4", String.format("new connector %s RUNNING %.0f s after the create call", connectorId,
            (System.nanoTime() - t0) / 1e9));
    }

    static void redeployInPlace(Map<String, String> extraEnv) throws Exception {
        Map<String, String> env = new HashMap<>(extraEnv);
        env.put("UPDATE_ID", connectorId);
        runScript(env);
    }

    static void runScript(Map<String, String> extraEnv) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("scripts/cc-deploy.sh").redirectErrorStream(true);
        pb.environment().putAll(extraEnv);
        // The script creates its own API key; make sure it does not use ours.
        pb.environment().remove("FANIN_API_KEY");
        pb.environment().remove("FANIN_API_SECRET");
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, p.waitFor(), "cc-deploy.sh failed: " + out.replaceAll("(?i)secret[^\\n]*", "<redacted>"));
    }

    static String cli(String... args) {
        List<String> cmd = new ArrayList<>(List.of("confluent"));
        cmd.addAll(List.of(args));
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (p.waitFor() != 0) {
                throw new IllegalStateException("confluent " + String.join(" ", args) + " failed: " + out);
            }
            return out;
        } catch (java.io.IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    static void await(String what, long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (IllegalStateException e) {
                // a transient CLI failure (for example a network blip): try again
            }
            Thread.sleep(5_000);
        }
        throw new AssertionError("timeout waiting for " + what);
    }

    static void log(String id, String message) {
        System.out.println("RESULT " + id + " " + message);
    }

    // ------------------------------------------------------------------ reader of the fan-in topic

    record Rec(String topic, int srcPartition, long srcOffset, int partition, long offset, byte[] value) {
    }

    /** Reads the fan-in topic continuously from the beginning and remembers when each source topic first arrived. */
    static class Tail implements AutoCloseable {
        private final List<Rec> recs = Collections.synchronizedList(new ArrayList<>());
        final Map<String, Long> firstSeen = new ConcurrentHashMap<>();
        private final Thread thread;
        private volatile boolean stop;

        Tail() {
            thread = new Thread(this::run, "tail");
            thread.setDaemon(true);
            thread.start();
        }

        private void run() {
            try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(clientProps(false))) {
                List<TopicPartition> parts = new ArrayList<>();
                c.partitionsFor(TARGET).forEach(pi -> parts.add(new TopicPartition(TARGET, pi.partition())));
                c.assign(parts);
                c.seekToBeginning(parts);
                while (!stop) {
                    for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) {
                        long now = System.nanoTime();
                        String topic = header(r, "fanin.source.topic");
                        if (topic == null) {
                            continue;
                        }
                        boolean keepValue = topic.startsWith(C7);
                        recs.add(new Rec(topic, Integer.parseInt(header(r, "fanin.source.partition")),
                            Long.parseLong(header(r, "fanin.source.offset")), r.partition(), r.offset(),
                            keepValue ? r.value() : null));
                        firstSeen.putIfAbsent(topic, now);
                    }
                }
            } catch (RuntimeException e) {
                System.out.println("RESULT tail reader stopped: " + e);
            }
        }

        List<Rec> recs() {
            synchronized (recs) {
                return new ArrayList<>(recs);
            }
        }

        /** Records (duplicates included) whose source topic starts with the prefix. */
        long count(String prefix) {
            synchronized (recs) {
                return recs.stream().filter(r -> r.topic.startsWith(prefix)).count();
            }
        }

        long distinct(String prefix) {
            Set<String> s = new HashSet<>();
            synchronized (recs) {
                recs.stream().filter(r -> r.topic.startsWith(prefix))
                    .forEach(r -> s.add(r.topic + ":" + r.srcPartition + ":" + r.srcOffset));
            }
            return s.size();
        }

        /** True once the large C7 record (source partition 1) is in the fan-in topic. */
        boolean hasBigRecord() {
            synchronized (recs) {
                return recs.stream().anyMatch(r -> r.topic.equals(C7) && r.srcPartition == 1);
            }
        }

        long topicCount(String prefix) {
            return firstSeen.keySet().stream().filter(t -> t.startsWith(prefix)).count();
        }

        private static String header(ConsumerRecord<byte[], byte[]> r, String key) {
            Header h = r.headers().lastHeader(key);
            return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
            stop = true;
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
