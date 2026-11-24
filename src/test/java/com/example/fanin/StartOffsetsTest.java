package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class StartOffsetsTest {

    private static final String GROUP = "connect-fanin";
    private static final long START = 1_000L;

    private static final TopicPartition A0 = new TopicPartition("a", 0);
    private static final TopicPartition A1 = new TopicPartition("a", 1);
    private static final TopicPartition B0 = new TopicPartition("b", 0);

    /** Fake broker state. */
    static class FakeLookup implements StartOffsets.Lookup {
        Map<TopicPartition, Long> committed = new HashMap<>();
        boolean groupExists = true;
        Map<TopicPartition, Long> atTime = new HashMap<>();
        Map<TopicPartition, Long> ends = new HashMap<>();
        final List<Collection<TopicPartition>> timeQueries = new ArrayList<>();
        final List<Collection<TopicPartition>> endQueries = new ArrayList<>();
        long lastTimestamp = Long.MIN_VALUE;

        @Override
        public Map<TopicPartition, Long> committed(String groupId) {
            assertEquals(GROUP, groupId);
            return groupExists ? committed : null;
        }

        @Override
        public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> partitions, long timestampMs) {
            timeQueries.add(new ArrayList<>(partitions));
            lastTimestamp = timestampMs;
            Map<TopicPartition, Long> r = new HashMap<>();
            for (TopicPartition tp : partitions) {
                r.put(tp, atTime.getOrDefault(tp, -1L));
            }
            return r;
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            endQueries.add(new ArrayList<>(partitions));
            Map<TopicPartition, Long> r = new HashMap<>();
            for (TopicPartition tp : partitions) {
                r.put(tp, ends.getOrDefault(tp, 0L));
            }
            return r;
        }

        @Override
        public void close() {
        }
    }

    @Test
    void partitionsWithCommittedOffsetAreNeverMoved() throws Exception {
        FakeLookup lookup = new FakeLookup();
        lookup.committed.put(A0, 500L);
        lookup.committed.put(A1, 0L); // committed at 0 is still committed
        lookup.atTime.put(A0, 10L);
        lookup.atTime.put(A1, 10L);

        Map<TopicPartition, Long> moves = StartOffsets.compute(lookup, GROUP, List.of(A0, A1), START);

        assertTrue(moves.isEmpty(), "a restart must resume from the committed offsets");
        assertTrue(lookup.timeQueries.isEmpty(), "no timestamp lookup is needed when everything is committed");
    }

    @Test
    void uncommittedPartitionsStartAtTheFirstRecordAtOrAfterTheTimestamp() throws Exception {
        FakeLookup lookup = new FakeLookup();
        lookup.atTime.put(A0, 120L);
        lookup.atTime.put(B0, 7L);

        Map<TopicPartition, Long> moves = StartOffsets.compute(lookup, GROUP, List.of(A0, B0), START);

        assertEquals(Map.of(A0, 120L, B0, 7L), moves);
        assertEquals(START, lookup.lastTimestamp);
        assertTrue(lookup.endQueries.isEmpty());
    }

    @Test
    void onlyTheUncommittedPartitionsOfAMixedAssignmentAreMoved() throws Exception {
        FakeLookup lookup = new FakeLookup();
        lookup.committed.put(A0, 900L);
        lookup.atTime.put(A1, 33L);
        lookup.atTime.put(A0, 1L); // would be a rewind if it were applied

        Map<TopicPartition, Long> moves = StartOffsets.compute(lookup, GROUP, List.of(A0, A1), START);

        assertEquals(Map.of(A1, 33L), moves);
        assertEquals(List.of(List.of(A1)), lookup.timeQueries);
    }

    @Test
    void aPartitionWithNoRecordAtOrAfterTheTimestampStartsAtItsEnd() throws Exception {
        FakeLookup lookup = new FakeLookup();
        lookup.atTime.put(A0, 50L);
        // A1 has only older records: the timestamp lookup answers -1
        lookup.ends.put(A1, 4_000L);

        Map<TopicPartition, Long> moves = StartOffsets.compute(lookup, GROUP, List.of(A0, A1), START);

        assertEquals(Map.of(A0, 50L, A1, 4_000L), moves);
        assertEquals(1, lookup.endQueries.size());
        assertEquals(List.of(A1), lookup.endQueries.get(0));
    }

    @Test
    void aNewEmptyTopicStartsAtZeroSoItsFirstRecordIsNotSkipped() throws Exception {
        FakeLookup lookup = new FakeLookup();
        lookup.ends.put(B0, 0L);

        Map<TopicPartition, Long> moves = StartOffsets.compute(lookup, GROUP, List.of(B0), START);

        assertEquals(Map.of(B0, 0L), moves);
    }

    @Test
    void anUnknownGroupMovesNothingSoARestartCannotRewindEverything() throws Exception {
        FakeLookup lookup = new FakeLookup();
        lookup.groupExists = false;
        lookup.atTime.put(A0, 1L);

        Map<TopicPartition, Long> moves = StartOffsets.compute(lookup, GROUP, List.of(A0), START);

        assertTrue(moves.isEmpty());
        assertTrue(lookup.timeQueries.isEmpty());
    }

    @Test
    void noAssignedPartitionsMeansNoLookup() throws Exception {
        FakeLookup lookup = new FakeLookup() {
            @Override
            public Map<TopicPartition, Long> committed(String groupId) {
                throw new AssertionError("must not be called");
            }
        };

        assertTrue(StartOffsets.compute(lookup, GROUP, List.of(), START).isEmpty());
    }

    @Test
    void anEndOffsetThatCannotBeReadLeavesThePartitionAlone() throws Exception {
        FakeLookup lookup = new FakeLookup() {
            @Override
            public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
                return Map.of(); // broker returned nothing for the partition
            }
        };

        Map<TopicPartition, Long> moves = StartOffsets.compute(lookup, GROUP, List.of(A0), START);

        assertTrue(moves.isEmpty());
    }
}
