package com.example.fanin;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

/** {@link StartOffsets.Lookup} on top of a Kafka {@link Admin} client. */
final class AdminLookup implements StartOffsets.Lookup {

    static final long TIMEOUT_SECONDS = 30;

    private final Admin admin;
    private long deadlineNanos = Long.MAX_VALUE;

    AdminLookup(Admin admin) {
        this.admin = admin;
    }

    @Override
    public void deadline(long deadlineNanos) {
        this.deadlineNanos = deadlineNanos;
    }

    /** Time for one call in milliseconds: at most {@link #TIMEOUT_SECONDS}, and not past the deadline. */
    private long timeoutMs() throws TimeoutException {
        long max = TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS);
        if (deadlineNanos == Long.MAX_VALUE) {
            return max;
        }
        long left = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (left <= 0) {
            throw new TimeoutException("The time for the offset lookups is used up");
        }
        return Math.min(max, left);
    }

    @Override
    public Map<TopicPartition, Long> committed(String groupId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = committedWithMetadata(groupId);
        if (offsets == null) {
            return null;
        }
        Map<TopicPartition, Long> result = new HashMap<>();
        offsets.forEach((tp, om) -> result.put(tp, om.offset()));
        return result;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committedWithMetadata(String groupId) throws Exception {
        ConsumerGroupDescription description;
        try {
            description = admin.describeConsumerGroups(List.of(groupId)).describedGroups().get(groupId)
                .get(timeoutMs(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                return null;
            }
            throw e;
        }
        // The task's own consumer is a member of its group by the time partitions are assigned, so a group without
        // members does not exist (older brokers answer with an empty DEAD group instead of an error). Only methods
        // that every client version has are used: the Confluent Cloud runtime has no groupState().
        if (description.members().isEmpty()) {
            return null;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata().get(timeoutMs(), TimeUnit.MILLISECONDS);
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        offsets.forEach((tp, om) -> {
            if (om != null) {
                result.put(tp, om);
            }
        });
        return result;
    }

    @Override
    public Map<TopicPartition, Long> offsetsForTimes(Map<TopicPartition, Long> timestamps) throws Exception {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        timestamps.forEach((tp, ts) -> request.put(tp, OffsetSpec.forTimestamp(ts)));
        return listOffsets(request);
    }

    @Override
    public Map<TopicPartition, Long> offsetsAtOrAfter(Collection<TopicPartition> partitions, long timestampMs)
        throws Exception {
        return listOffsets(partitions, OffsetSpec.forTimestamp(timestampMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) throws Exception {
        return listOffsets(partitions, OffsetSpec.latest());
    }

    @Override
    public Map<TopicPartition, Long> offsetsForTimesEach(Map<TopicPartition, Long> timestamps,
                                                         Map<TopicPartition, Exception> failed)
        throws InterruptedException {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        timestamps.forEach((tp, ts) -> request.put(tp, OffsetSpec.forTimestamp(ts)));
        return listOffsetsEach(request, failed);
    }

    @Override
    public Map<TopicPartition, Long> endOffsetsEach(Collection<TopicPartition> partitions,
                                                    Map<TopicPartition, Exception> failed)
        throws InterruptedException {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition tp : partitions) {
            request.put(tp, OffsetSpec.latest());
        }
        return listOffsetsEach(request, failed);
    }

    /**
     * One ListOffsets request; the result of each partition is read from its own future, so a partition that fails
     * (for example no DESCRIBE on its topic, or a deleted topic) does not fail the others.
     */
    private Map<TopicPartition, Long> listOffsetsEach(Map<TopicPartition, OffsetSpec> request,
                                                      Map<TopicPartition, Exception> failed)
        throws InterruptedException {
        if (request.isEmpty()) {
            return Map.of();
        }
        long endNanos;
        try {
            endNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs());
        } catch (TimeoutException e) {
            request.keySet().forEach(tp -> failed.put(tp, e));
            return Map.of();
        }
        ListOffsetsResult response = admin.listOffsets(request);
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : request.keySet()) {
            try {
                long left = Math.max(0L, endNanos - System.nanoTime());
                result.put(tp, response.partitionResult(tp).get(left, TimeUnit.NANOSECONDS).offset());
            } catch (ExecutionException e) {
                failed.put(tp, e.getCause() instanceof Exception cause ? cause : e);
            } catch (TimeoutException e) {
                failed.put(tp, e);
            }
        }
        return result;
    }

    @Override
    public Set<String> topicNames() throws Exception {
        return admin.listTopics().names().get(timeoutMs(), TimeUnit.MILLISECONDS);
    }

    private Map<TopicPartition, Long> listOffsets(Collection<TopicPartition> partitions, OffsetSpec spec)
        throws Exception {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition tp : partitions) {
            request.put(tp, spec);
        }
        return listOffsets(request);
    }

    private Map<TopicPartition, Long> listOffsets(Map<TopicPartition, OffsetSpec> request) throws Exception {
        if (request.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, ListOffsetsResultInfo> response = admin.listOffsets(request).all()
            .get(timeoutMs(), TimeUnit.MILLISECONDS);
        Map<TopicPartition, Long> result = new HashMap<>();
        response.forEach((tp, info) -> result.put(tp, info.offset()));
        return result;
    }

    @Override
    public void close() {
        admin.close(java.time.Duration.ofSeconds(10));
    }
}
