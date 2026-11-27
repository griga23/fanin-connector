package com.example.fanin;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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

        /**
         * Like {@link #committed}, with the metadata of each committed offset. The default has no metadata.
         */
        default Map<TopicPartition, OffsetAndMetadata> committedWithMetadata(String groupId) throws Exception {
            Map<TopicPartition, Long> offsets = committed(groupId);
            if (offsets == null) {
                return null;
            }
            Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
            offsets.forEach((tp, offset) -> {
                if (offset != null && offset >= 0) {
                    result.put(tp, new OffsetAndMetadata(offset));
                }
            });
            return result;
        }

        /**
         * For each partition the first offset whose record timestamp is at or after the timestamp given for that
         * partition, or -1 if none. The default asks once per distinct timestamp; the Admin lookup asks once.
         */
        default Map<TopicPartition, Long> offsetsForTimes(Map<TopicPartition, Long> timestamps) throws Exception {
            Map<Long, Set<TopicPartition>> byTime = new HashMap<>();
            timestamps.forEach((tp, ts) -> byTime.computeIfAbsent(ts, k -> new HashSet<>()).add(tp));
            Map<TopicPartition, Long> result = new HashMap<>();
            for (Map.Entry<Long, Set<TopicPartition>> e : byTime.entrySet()) {
                result.putAll(offsetsAtOrAfter(e.getValue(), e.getKey()));
            }
            return result;
        }

        /**
         * Like {@link #offsetsForTimes}, but per partition: a partition whose lookup fails is left out of the result
         * and its error is put into {@code failed}. Other partitions still get their offset. The default asks for
         * all partitions at once and, if that fails, for each partition alone.
         */
        default Map<TopicPartition, Long> offsetsForTimesEach(Map<TopicPartition, Long> timestamps,
                                                              Map<TopicPartition, Exception> failed)
            throws InterruptedException {
            try {
                return offsetsForTimes(timestamps);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (timestamps.size() == 1) {
                    failed.put(timestamps.keySet().iterator().next(), e);
                    return Map.of();
                }
            }
            Map<TopicPartition, Long> result = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> e : timestamps.entrySet()) {
                try {
                    result.putAll(offsetsForTimes(Map.of(e.getKey(), e.getValue())));
                } catch (InterruptedException ie) {
                    throw ie;
                } catch (Exception ex) {
                    failed.put(e.getKey(), ex);
                }
            }
            return result;
        }

        /** Like {@link #endOffsets}, but per partition, as {@link #offsetsForTimesEach}. */
        default Map<TopicPartition, Long> endOffsetsEach(Collection<TopicPartition> partitions,
                                                         Map<TopicPartition, Exception> failed)
            throws InterruptedException {
            try {
                return endOffsets(partitions);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (partitions.size() == 1) {
                    failed.put(partitions.iterator().next(), e);
                    return Map.of();
                }
            }
            Map<TopicPartition, Long> result = new HashMap<>();
            for (TopicPartition tp : partitions) {
                try {
                    result.putAll(endOffsets(Set.of(tp)));
                } catch (InterruptedException ie) {
                    throw ie;
                } catch (Exception ex) {
                    failed.put(tp, ex);
                }
            }
            return result;
        }

        /**
         * Limits the time of the following calls: each call waits at most until this {@link System#nanoTime} value.
         * {@link Long#MAX_VALUE} removes the limit. The default ignores it.
         */
        default void deadline(long deadlineNanos) {
        }

        /** For each partition the first offset whose record timestamp is at or after the timestamp, or -1 if none. */
        Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> partitions, long timestampMs)
            throws Exception;

        /** End offset of each partition. */
        Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) throws Exception;

        /** Names of the topics that the client can see. */
        Set<String> topicNames() throws Exception;

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
