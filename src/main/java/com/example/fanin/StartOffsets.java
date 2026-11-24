package com.example.fanin;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Works out where partitions that have no committed offset should start, so that a new deployment does not read
 * the whole backlog of the input topics.
 *
 * <p>Only partitions without a committed offset in the connector's consumer group are touched. Partitions with a
 * committed offset always resume from it, so a restart never rewinds. A partition that has no record at or after the
 * start timestamp starts at its end.
 */
final class StartOffsets {

    private static final Logger log = LoggerFactory.getLogger(StartOffsets.class);

    /** The few broker lookups needed, kept small so tests can replace them. */
    interface Lookup extends AutoCloseable {

        /**
         * Offsets committed by the group, or {@code null} if the group does not exist. A group that does not exist
         * means the group id is wrong (the task's own consumer is a member by the time this is called), so the caller
         * must not treat every partition as uncommitted.
         */
        Map<TopicPartition, Long> committed(String groupId) throws Exception;

        /** For each partition the first offset whose record timestamp is at or after the timestamp, or -1 if none. */
        Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> partitions, long timestampMs)
            throws Exception;

        /** End offset of each partition. */
        Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) throws Exception;

        @Override
        void close();
    }

    private StartOffsets() {
    }

    /**
     * @return partition to the offset it should be moved to; empty if nothing needs to move
     */
    static Map<TopicPartition, Long> compute(Lookup lookup, String groupId, Collection<TopicPartition> assigned,
                                             long startTimestampMs) throws Exception {
        if (assigned.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, Long> committed = lookup.committed(groupId);
        if (committed == null) {
            log.warn("Consumer group '{}' was not found, so start positions are not applied. Check "
                + "fanin.start.consumer.group.", groupId);
            return Map.of();
        }
        Set<TopicPartition> uncommitted = new HashSet<>();
        for (TopicPartition tp : assigned) {
            Long offset = committed.get(tp);
            if (offset == null || offset < 0) {
                uncommitted.add(tp);
            }
        }
        if (uncommitted.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, Long> atTime = lookup.offsetsAtOrAfter(uncommitted, startTimestampMs);
        Set<TopicPartition> noRecordAfter = new HashSet<>();
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : uncommitted) {
            Long offset = atTime.get(tp);
            if (offset == null || offset < 0) {
                noRecordAfter.add(tp);
            } else {
                result.put(tp, offset);
            }
        }
        if (!noRecordAfter.isEmpty()) {
            Map<TopicPartition, Long> ends = lookup.endOffsets(noRecordAfter);
            for (TopicPartition tp : noRecordAfter) {
                Long end = ends.get(tp);
                if (end != null && end >= 0) {
                    result.put(tp, end);
                }
            }
        }
        return result;
    }
}
