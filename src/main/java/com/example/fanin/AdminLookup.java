package com.example.fanin;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

/** {@link StartOffsets.Lookup} on top of a Kafka {@link Admin} client. */
final class AdminLookup implements StartOffsets.Lookup {

    static final long TIMEOUT_SECONDS = 30;

    private final Admin admin;

    AdminLookup(Admin admin) {
        this.admin = admin;
    }

    @Override
    public Map<TopicPartition, Long> committed(String groupId) throws Exception {
        ConsumerGroupDescription description;
        try {
            description = admin.describeConsumerGroups(List.of(groupId)).describedGroups().get(groupId)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
            .partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Map<TopicPartition, Long> result = new HashMap<>();
        offsets.forEach((tp, om) -> {
            if (om != null) {
                result.put(tp, om.offset());
            }
        });
        return result;
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

    private Map<TopicPartition, Long> listOffsets(Collection<TopicPartition> partitions, OffsetSpec spec)
        throws Exception {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition tp : partitions) {
            request.put(tp, spec);
        }
        Map<TopicPartition, ListOffsetsResultInfo> response = admin.listOffsets(request).all()
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Map<TopicPartition, Long> result = new HashMap<>();
        response.forEach((tp, info) -> result.put(tp, info.offset()));
        return result;
    }

    @Override
    public void close() {
        admin.close(java.time.Duration.ofSeconds(10));
    }
}
