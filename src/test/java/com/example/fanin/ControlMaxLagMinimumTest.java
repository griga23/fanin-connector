package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

/** The minimum of fanin.control.max.lag.ms: a healthy reader must never look further behind than it. */
class ControlMaxLagMinimumTest {

    private static Map<String, String> props(String maxLag) {
        Map<String, String> p = new HashMap<>(Map.of(FanInSinkConfig.TARGET_TOPIC, "t",
            FanInSinkConfig.TARGET_BOOTSTRAP_SERVERS, "x", FanInSinkConfig.TARGET_API_KEY, "k",
            FanInSinkConfig.TARGET_API_SECRET, "s"));
        p.put(FanInSinkConfig.CONTROL_MAX_LAG_MS, maxLag);
        return p;
    }

    /**
     * A healthy reader updates caughtUpAt only every ControlReader.CHECK_MS (5 s). With max.lag.ms below that
     * the task would stall (pause all, drop all, seek back) every few seconds although the reader is fine.
     *
     * <p>Adapted: the first version of this test asserted that a healthy reader is never more than 1000 ms behind.
     * That is not the design (the reader measures every 5 s). The fix rejects max.lag.ms values below the time a
     * healthy reader can look behind, and this test checks both: 1000 is rejected, and a healthy reader stays below
     * the minimum.
     */
    @Test
    void healthyReader_caughtUpAtIsUpTo5sOld_soSmallMaxLagStallsTheTask() throws Exception {
        ConfigException e = assertThrows(ConfigException.class, () -> new FanInSinkConfig(props("1000")));
        assertTrue(e.getMessage().contains(String.valueOf(FanInSinkConfig.MIN_CONTROL_MAX_LAG_MS)), e.getMessage());
        assertEquals(FanInSinkConfig.MIN_CONTROL_MAX_LAG_MS, new FanInSinkConfig(
            props(String.valueOf(FanInSinkConfig.MIN_CONTROL_MAX_LAG_MS))).controlMaxLagMs());

        TopicPartition ctl0 = new TopicPartition("ctl", 0);
        MockConsumer<byte[], byte[]> c = new MockConsumer<>("earliest");
        c.updatePartitions("ctl", List.of(new PartitionInfo("ctl", 0, null, null, null)));
        c.updateBeginningOffsets(Map.of(ctl0, 0L));
        c.updateEndOffsets(Map.of(ctl0, 0L));
        ControlReader r = new ControlReader(c, "ctl", new ControlState(), System::currentTimeMillis);
        r.loadInitial(Duration.ofSeconds(5));
        r.startBackground();
        long maxAge = 0;
        long end = System.currentTimeMillis() + 7_000;
        while (System.currentTimeMillis() < end) {
            maxAge = Math.max(maxAge, System.currentTimeMillis() - r.caughtUpAt());
            Thread.sleep(50);
        }
        r.close();
        assertTrue(maxAge <= FanInSinkConfig.MIN_CONTROL_MAX_LAG_MS, "a healthy reader looked " + maxAge
            + " ms behind; the smallest allowed max.lag.ms is " + FanInSinkConfig.MIN_CONTROL_MAX_LAG_MS);
    }
}
