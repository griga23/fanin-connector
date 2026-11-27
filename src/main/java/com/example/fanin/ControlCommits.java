package com.example.fanin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides what the task commits with the control topic: the offset of each partition, held back where records were
 * dropped that may still be needed, and the ledger of the partition in the offset metadata (see
 * {@link ControlLedger}).
 */
final class ControlCommits {

    /** Logs under the task's logger, so log lines and log configuration stay as they were. */
    private static final Logger log = LoggerFactory.getLogger(FanInSinkTask.class);

    private final ControlPartitions p;
    private final LongSupplier clock;
    private long lastHeldWarn = Long.MIN_VALUE;

    ControlCommits(ControlPartitions p, LongSupplier clock) {
        this.p = p;
        this.clock = clock;
    }

    /**
     * Returns the offsets to commit. Each offset carries the ledger of its partition in its metadata. A change of the
     * metadata alone makes the framework commit, because {@link OffsetAndMetadata#equals} compares the metadata. An
     * offset is not committed past a record that was dropped while its partition was pending, or past a seek that the
     * framework has not applied yet. A partition that is not assigned is left out. If the ledger of an assigned
     * partition is not known (open() could not look up the committed offsets), nothing is committed, because a commit
     * would replace the committed ledger with an empty one.
     */
    Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets,
                                                     Set<TopicPartition> assigned) {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        List<TopicPartition> unknown = new ArrayList<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : currentOffsets.entrySet()) {
            TopicPartition tp = e.getKey();
            if (!assigned.contains(tp)) {
                // The framework keeps a partition that was revoked earlier in its current offsets and passes it again
                // when the task stops (WorkerSinkTask.closeAllPartitions). It ignores an offset for a partition that
                // is not assigned, so this task leaves it out.
                continue;
            }
            if (!p.committedLedger.containsKey(tp)) {
                unknown.add(tp);
                continue;
            }
            OffsetAndMetadata current = e.getValue();
            long offset = current.offset();
            Long hold = p.held.get(tp);
            if (hold != null && hold < offset) {
                offset = hold;
            }
            Long removedFrom = p.removedHold.get(tp);
            if (removedFrom != null && removedFrom < offset) {
                offset = removedFrom;
            }
            Long seek = p.issuedSeeks.get(tp);
            if (seek != null && seek < offset) {
                offset = seek;
            }
            ControlLedger ledger = ledgerFor(tp, offset);
            String metadata = ledger == null ? current.metadata() : ledger.encode();
            result.put(tp, new OffsetAndMetadata(offset, current.leaderEpoch(), metadata));
        }
        if (unknown.isEmpty()) {
            p.ownCommits.putAll(result);
        }
        warnHeldCommits(unknown);
        return unknown.isEmpty() ? result : Map.of();
    }

    /**
     * The ledger to commit with this offset of the partition, or null to keep the metadata the framework gave.
     */
    private ControlLedger ledgerFor(TopicPartition tp, long offset) {
        String topic = tp.topic();
        ControlPartitions.RemoveNeed need = p.removeCutNeeded.get(tp);
        if (need != null) {
            // The remove cut is not known yet: keep the ledger from before the remove, so that a restart looks the
            // cut up again. The commit is held at the first held record.
            return need.before();
        }
        ControlLedger removedWithCut = p.removeCut.get(tp);
        if (removedWithCut != null) {
            // A removed topic, or a re-added one that has not reached its remove cut yet.
            return removedWithCut;
        }
        ControlLedger committed = p.committedLedger.get(tp);
        Long removed = p.removeTimes.get(topic);
        if (!p.allowed.contains(topic)) {
            if (removed != null) {
                return ControlLedger.removedAt(removed);
            }
            if (committed != null && !committed.allowed()) {
                return committed;
            }
            // The remove record was compacted away; the topic was removed after the add in the old ledger.
            return committed != null ? ControlLedger.removedAt(committed.timeMs()) : null;
        }
        Long floor = p.dropBelow.get(tp);
        if (p.pending.containsKey(tp) || (floor != null && offset < floor)) {
            if (removed != null
                && (committed == null || (committed.allowed() && p.removedSinceLedger(topic, committed)))) {
                return ControlLedger.removedAt(removed);
            }
            return committed;
        }
        return ControlLedger.allowedAt(p.addTimes.get(topic));
    }

    /**
     * At most once per {@link ControlCoordinator#CONTROL_CHECK_MS}: a WARN line that names the partitions whose commit
     * does not move and why, so that a lookup that keeps failing is visible.
     */
    private void warnHeldCommits(List<TopicPartition> unknown) {
        if (unknown.isEmpty() && p.pending.isEmpty() && p.removeCutNeeded.isEmpty()) {
            return;
        }
        long now = clock.getAsLong();
        if (lastHeldWarn != Long.MIN_VALUE && now - lastHeldWarn < ControlCoordinator.CONTROL_CHECK_MS) {
            return;
        }
        lastHeldWarn = now;
        if (!unknown.isEmpty()) {
            Map<TopicPartition, String> reasons = new HashMap<>();
            unknown.forEach(tp -> reasons.put(tp, p.pendingReason.getOrDefault(tp, "not looked up yet")));
            log.warn("Control topic: no offsets are committed for this task, because the committed ledger of {} is "
                + "not known: {}. A commit would replace it with empty metadata. The lookup is tried again every {} "
                + "ms.", unknown, reasons, ControlCuts.CONTROL_RETRY_MS);
        } else {
            Set<TopicPartition> undecided = new HashSet<>(p.pending.keySet());
            undecided.addAll(p.removeCutNeeded.keySet());
            log.warn("Control topic: the start position or remove cut of {} is not decided; these partitions stay "
                + "paused and their commits do not move. Other partitions forward and commit. Reasons: {}. The lookup "
                + "is tried again every {} ms.", undecided, p.pendingReason, ControlCuts.CONTROL_RETRY_MS);
        }
    }
}
