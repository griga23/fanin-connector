package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

/** The per-partition offset lookups of {@link AdminLookup}. */
class AdminLookupTest {

    private static final TopicPartition A0 = new TopicPartition("in.a", 0);
    private static final TopicPartition B0 = new TopicPartition("in.b", 0);

    /** An Admin whose ListOffsets answers A0 with offset 7 and fails B0 with an authorization error. */
    private static Admin admin(List<Map<TopicPartition, OffsetSpec>> requests) {
        return (Admin) Proxy.newProxyInstance(AdminLookupTest.class.getClassLoader(), new Class<?>[] {Admin.class},
            (proxy, method, args) -> {
                if (!method.getName().equals("listOffsets")) {
                    throw new UnsupportedOperationException(method.getName());
                }
                @SuppressWarnings("unchecked")
                Map<TopicPartition, OffsetSpec> request = (Map<TopicPartition, OffsetSpec>) args[0];
                requests.add(request);
                Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new HashMap<>();
                for (TopicPartition tp : request.keySet()) {
                    KafkaFutureImpl<ListOffsetsResultInfo> f = new KafkaFutureImpl<>();
                    if (tp.equals(B0)) {
                        f.completeExceptionally(new TopicAuthorizationException("no DESCRIBE on in.b"));
                    } else {
                        f.complete(new ListOffsetsResultInfo(7L, 100L, Optional.empty()));
                    }
                    futures.put(tp, f);
                }
                return new ListOffsetsResult(futures);
            });
    }

    @Test
    void offsetsForTimesEach_oneFailingPartitionDoesNotFailTheOthers() throws Exception {
        List<Map<TopicPartition, OffsetSpec>> requests = new java.util.ArrayList<>();
        AdminLookup lookup = new AdminLookup(admin(requests));
        Map<TopicPartition, Exception> failed = new HashMap<>();

        Map<TopicPartition, Long> result = lookup.offsetsForTimesEach(Map.of(A0, 100L, B0, 100L), failed);

        assertEquals(1, requests.size(), "one ListOffsets request for both partitions");
        assertEquals(Map.of(A0, 7L), result);
        assertEquals(List.of(B0), List.copyOf(failed.keySet()));
        assertTrue(failed.get(B0) instanceof TopicAuthorizationException, failed.toString());
    }

    @Test
    void endOffsetsEach_oneFailingPartitionDoesNotFailTheOthers() throws Exception {
        List<Map<TopicPartition, OffsetSpec>> requests = new java.util.ArrayList<>();
        AdminLookup lookup = new AdminLookup(admin(requests));
        Map<TopicPartition, Exception> failed = new HashMap<>();

        Map<TopicPartition, Long> result = lookup.endOffsetsEach(List.of(A0, B0), failed);

        assertEquals(Map.of(A0, 7L), result);
        assertTrue(failed.get(B0) instanceof TopicAuthorizationException, failed.toString());
    }
}
