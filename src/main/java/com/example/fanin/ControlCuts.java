package com.example.fanin;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides where partitions start and where a removed topic stops: the add cut, the remove cut and the start
 * timestamp. It reads the committed offsets with their ledger and the offsets for the cut times from the broker,
 * through {@link StartOffsets.Lookup}, and writes the result into {@link ControlPartitions}. It makes no consumer
 * calls itself: the moves of open() go to the callback that open() passes, and the seeks of a running partition go
 * to {@link ControlPartitions#seeksToIssue}.
 */
final class ControlCuts {

    /** Logs under the task's logger, so log lines and log configuration stay as they were. */
    private static final Logger log = LoggerFactory.getLogger(FanInSinkTask.class);

    /** Wait before a failed add-time lookup is tried again. */
    static final long CONTROL_RETRY_MS = Duration.ofSeconds(10).toMillis();
    /** Total time for the broker lookups of one open() or one retry. */
    static final long LOOKUP_BUDGET_MS = Duration.ofSeconds(20).toMillis();

    private final ControlPartitions p;
    private final Supplier<StartOffsets.Lookup> lookups;
    private final LongSupplier clock;
    private final String consumerGroup;
    private final long startTimestamp;

    private long nextCutRetry = Long.MIN_VALUE;
    private boolean groupNotFoundWarned;

    ControlCuts(ControlPartitions p, Supplier<StartOffsets.Lookup> lookups, LongSupplier clock, String consumerGroup,
                long startTimestamp) {
        this.p = p;
        this.lookups = lookups;
        this.clock = clock;
        this.consumerGroup = consumerGroup;
        this.startTimestamp = startTimestamp;
    }

    /**
     * Decides the start position of each opened partition with one batch of broker lookups (see {@link #decide}). If
     * the committed offset lookup fails or takes longer than {@link #LOOKUP_BUDGET_MS}, all the partitions become
     * pending. If only the offset lookup of some partitions fails, only those become pending. Pending partitions stay
     * paused, and put() tries again.
     *
     * <p>The moves are passed to {@code applyMoves} inside this decision, before the log lines about them. If
     * {@code applyMoves} throws, the failure is handled like a failed committed offset lookup: all the partitions
     * become pending.
     *
     * @param applyMoves passes the moves of open() to the consumer; called only with a non-empty map
     */
    void decideOnOpen(Collection<TopicPartition> partitions, Consumer<Map<TopicPartition, Long>> applyMoves) {
        try {
            decide(partitions, applyMoves);
        } catch (Exception | LinkageError e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            for (TopicPartition tp : partitions) {
                p.pending.putIfAbsent(tp, false);
                p.pendingReason.put(tp, "committed offset lookup failed: " + e);
            }
            nextCutRetry = clock.getAsLong() + CONTROL_RETRY_MS;
            log.warn("Could not look up the committed offsets of {} assigned partitions; they stay paused and the "
                + "lookup is tried again in {} ms: {}", partitions.size(), CONTROL_RETRY_MS, e.toString());
        }
    }

    /** A new cut is needed: the next put() looks it up without waiting for the retry interval. */
    void retrySoon() {
        nextCutRetry = Long.MIN_VALUE;
    }

    /** Looks up the pending start positions and remove cuts, unless a failed lookup waits for its retry time. */
    void retryPendingIfDue() {
        if ((!p.pending.isEmpty() || !p.removeCutNeeded.isEmpty())
            && (nextCutRetry == Long.MIN_VALUE || clock.getAsLong() >= nextCutRetry)) {
            applyPendingCuts();
        }
    }

    private void applyPendingCuts() {
        Set<TopicPartition> all = new HashSet<>(p.pending.keySet());
        all.addAll(p.removeCutNeeded.keySet());
        List<TopicPartition> tps = new ArrayList<>(all);
        nextCutRetry = Long.MIN_VALUE;
        try {
            decide(tps, null);
        } catch (Exception | LinkageError e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            nextCutRetry = clock.getAsLong() + CONTROL_RETRY_MS;
            for (TopicPartition tp : tps) {
                p.pendingReason.put(tp, "committed offset lookup failed: " + e);
            }
            log.debug("Could not look up the committed offsets for {}; the partitions stay paused and the lookup is "
                + "tried again in {} ms: {}", tps, CONTROL_RETRY_MS, e.toString());
        }
    }

    /**
     * Decides the start position of the partitions with at most four broker calls, in this order: committed offsets
     * with their metadata, end offsets, offsets for the remove times, offsets for the add times. The end offsets are
     * read before the time lookups, so that the end-offset fallback of a time lookup that finds no record never
     * includes a record appended after that lookup.
     *
     * <p>If the committed offset lookup fails, this throws and nothing changes. After it, the ledger of every
     * partition is known. The offset lookups are per partition: a partition whose lookup fails becomes (or stays)
     * pending, with the reason in {@link ControlPartitions#pendingReason}, and the others are decided.
     *
     * <p>From open(), and for a partition that open() could not decide: a partition whose committed offset is at or
     * past the target is not moved, so a partition is never moved back below its committed offset. A retry uses the
     * committed offset and ledger that open() saw, not what this task committed since. For a topic that was added
     * while this task ran: the partition moves to the cut, but never below its committed offset and never below the
     * offset after the last record this task forwarded.
     *
     * @param applyMoves from open(), passes the moves to the consumer before the log lines; null for a decision in
     *     put()
     */
    private void decide(Collection<TopicPartition> partitions, Consumer<Map<TopicPartition, Long>> applyMoves)
        throws Exception {
        boolean fromOpen = applyMoves != null;
        StartOffsets.Lookup lookup = lookups.get();
        long deadline = clock.getAsLong() + LOOKUP_BUDGET_MS;
        lookup.deadline(System.nanoTime() + Duration.ofMillis(LOOKUP_BUDGET_MS).toNanos());
        Map<TopicPartition, Long> times = new HashMap<>();
        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        Set<TopicPartition> cuts = new HashSet<>();
        Map<TopicPartition, Long> targets = new HashMap<>();
        Map<TopicPartition, Long> removeTargets = new HashMap<>();
        Map<TopicPartition, Exception> failed = new HashMap<>();
        // Partitions whose remove cut was taken from the committed ledger in this call; capped at the end offset.
        Set<TopicPartition> ledgerCuts = new HashSet<>();
        Set<TopicPartition> ledgerCutsChecked = new HashSet<>();
        try {
            boolean fresh = false;
            for (TopicPartition tp : partitions) {
                fresh |= p.needsFreshLedger(tp);
            }
            Map<TopicPartition, OffsetAndMetadata> committed = null;
            boolean groupFound = true;
            if (fresh) {
                committed = lookup.committedWithMetadata(consumerGroup);
                checkBudget(deadline);
                groupFound = committed != null;
                if (!groupFound) {
                    if (!groupNotFoundWarned) {
                        groupNotFoundWarned = true;
                        log.warn("Consumer group '{}' was not found, although this task's consumer is a member of "
                            + "its group. Check {}: it must be the group of this connector (default connect-<connector "
                            + "name>). Without it the committed ledger cannot be read: partitions without a ledger in "
                            + "this task's memory start at the add time of their topic, and the start timestamp is not "
                            + "applied. This task remembers the cuts it applied, so later rebalances do not apply them "
                            + "again. This warning is logged once per task.", consumerGroup,
                            FanInSinkConfig.START_CONSUMER_GROUP);
                    }
                    committed = p.ownCommits;
                }
            }
            for (TopicPartition tp : partitions) {
                Boolean pendingKind = p.pending.get(tp);
                ControlLedger ledger;
                Long committedOffset;
                if (!p.needsFreshLedger(tp)) {
                    // A retry of the open() decision, or a remove cut of a running partition: use what open() saw.
                    // This task may have committed the position of the paused partition since then.
                    ledger = p.committedLedger.get(tp);
                    committedOffset = p.committedAtOpen.get(tp);
                } else {
                    OffsetAndMetadata c = committed == null ? null : committed.get(tp);
                    committedOffset = c != null && c.offset() >= 0 ? c.offset() : null;
                    ControlLedger raw = c == null ? null : ControlLedger.parse(c.metadata());
                    // An add while running reads the ledger again; this task may have written it since, so its remove
                    // cut is only taken when this task did not know the ledger before (open() could not read it).
                    boolean firstRead = !p.committedLedger.containsKey(tp);
                    if ((firstRead || !Boolean.TRUE.equals(pendingKind))
                        && removeCutFromLedger(tp, raw, committedOffset)) {
                        ledgerCuts.add(tp);
                    }
                    ledger = p.observedLedger(tp, raw);
                    p.committedLedger.put(tp, ledger);
                    if (firstRead || !Boolean.TRUE.equals(pendingKind)) {
                        if (committedOffset != null) {
                            p.committedAtOpen.put(tp, committedOffset);
                        } else {
                            p.committedAtOpen.remove(tp);
                        }
                    }
                }
                if (committedOffset != null) {
                    committedOffsets.put(tp, committedOffset);
                }
                if (!fromOpen && pendingKind == null) {
                    continue; // only its remove cut is looked up
                }
                long time = -1;
                if (startTimestamp >= 0 && groupFound && committedOffset == null) {
                    time = startTimestamp;
                }
                String topic = tp.topic();
                if (p.allowed.contains(topic) && (Boolean.TRUE.equals(pendingKind) || p.needsCut(topic, ledger))) {
                    cuts.add(tp);
                    time = Math.max(time, p.addTimes.get(topic));
                }
                if (time >= 0) {
                    times.put(tp, time);
                }
            }
            Map<TopicPartition, Long> removeTimesOf = new HashMap<>();
            for (TopicPartition tp : partitions) {
                ControlPartitions.RemoveNeed need = p.removeCutNeeded.get(tp);
                if (need != null) {
                    removeTimesOf.put(tp, need.removeTime());
                }
            }
            // End offsets first: a time lookup that finds no record at or after its time falls back to this end
            // offset. Read after the time lookup, the end offset could include records appended in between, with
            // times at or after the time asked for. Remove times are asked before add times, so that an add cut
            // in the same interval is never below its remove cut.
            Set<TopicPartition> needEnd = new HashSet<>(times.keySet());
            needEnd.addAll(removeTimesOf.keySet());
            needEnd.addAll(ledgerCuts);
            Map<TopicPartition, Exception> endFailed = new HashMap<>();
            Map<TopicPartition, Long> ends = needEnd.isEmpty() ? Map.of() : lookup.endOffsetsEach(needEnd, endFailed);
            failAllIfOverBudget(deadline, needEnd, failed);
            for (TopicPartition tp : ledgerCuts) {
                Long endOffset = ends.get(tp);
                if (failed.containsKey(tp)) {
                    continue;
                }
                if (endOffset == null || endOffset < 0) {
                    failed.put(tp, endFailed.getOrDefault(tp, new IllegalStateException("No end offset for " + tp)));
                } else {
                    capLedgerCut(tp, endOffset, committedOffsets.get(tp));
                    ledgerCutsChecked.add(tp);
                }
            }
            Map<TopicPartition, Long> atRemove = removeTimesOf.isEmpty() ? Map.of()
                : lookup.offsetsForTimesEach(removeTimesOf, failed);
            failAllIfOverBudget(deadline, removeTimesOf.keySet(), failed);
            Map<TopicPartition, Long> atTime = times.isEmpty() ? Map.of() : lookup.offsetsForTimesEach(times, failed);
            failAllIfOverBudget(deadline, times.keySet(), failed);
            for (TopicPartition tp : times.keySet()) {
                if (failed.containsKey(tp)) {
                    continue;
                }
                Long offset = atTime.get(tp);
                if (offset != null && offset >= 0) {
                    targets.put(tp, offset);
                    continue;
                }
                Long endOffset = ends.get(tp);
                if (endOffset != null && endOffset >= 0) {
                    targets.put(tp, endOffset);
                } else if (cuts.contains(tp)) {
                    failed.put(tp, endFailed.getOrDefault(tp, new IllegalStateException("No end offset for " + tp)));
                }
            }
            for (TopicPartition tp : removeTimesOf.keySet()) {
                if (failed.containsKey(tp)) {
                    continue;
                }
                Long offset = atRemove.get(tp);
                if (offset != null && offset >= 0) {
                    removeTargets.put(tp, offset);
                    continue;
                }
                Long endOffset = ends.get(tp);
                if (endOffset != null && endOffset >= 0) {
                    // No record at or after the remove time yet: records written from now on are dropped.
                    removeTargets.put(tp, endOffset);
                } else {
                    failed.put(tp, endFailed.getOrDefault(tp, new IllegalStateException("No end offset for " + tp)));
                }
            }
        } finally {
            lookup.deadline(Long.MAX_VALUE);
        }

        // Apply the result of each partition whose lookups succeeded.
        Map<TopicPartition, Long> openMoves = new HashMap<>();
        Map<TopicPartition, Long> cutLog = new HashMap<>();
        Map<TopicPartition, Long> gapLog = new HashMap<>();
        Map<TopicPartition, Long> removeLog = new HashMap<>();
        for (TopicPartition tp : partitions) {
            Exception error = failed.get(tp);
            boolean inPending = p.pending.containsKey(tp);
            if (error != null) {
                targets.remove(tp);
                if (ledgerCuts.contains(tp) && !ledgerCutsChecked.contains(tp)) {
                    // The ledger cut could not be checked against the end offset: read the ledger again on retry.
                    p.removeCut.remove(tp);
                    p.removeReached.remove(tp);
                    p.committedLedger.remove(tp);
                    p.committedAtOpen.remove(tp);
                }
                if (fromOpen || inPending) {
                    p.pending.putIfAbsent(tp, false);
                }
                p.pendingReason.put(tp, "offset lookup failed: " + error);
                continue;
            }
            Long committedOffset = committedOffsets.get(tp);
            if (committedOffset != null) {
                if (fromOpen) {
                    p.position.put(tp, committedOffset);
                } else if (Boolean.FALSE.equals(p.pending.get(tp))) {
                    p.position.putIfAbsent(tp, committedOffset); // a retry of open()
                }
            }
            ControlPartitions.RemoveNeed need = p.removeCutNeeded.remove(tp);
            if (need != null) {
                applyRemoveCut(tp, ControlLedger.removedAt(need.removeTime(), removeTargets.get(tp)));
                removeLog.put(tp, removeTargets.get(tp));
            }
            if (!fromOpen && !inPending) {
                p.pendingReason.remove(tp);
                continue;
            }
            boolean live = Boolean.TRUE.equals(p.pending.remove(tp));
            p.pendingReason.remove(tp);
            p.committedAtOpen.remove(tp);
            if (p.allowed.contains(tp.topic())) {
                p.removedHold.remove(tp);
            }
            Long target = targets.get(tp);
            ControlLedger removed = p.removeCut.get(tp);
            if (removed != null && p.allowed.contains(tp.topic())) {
                // A re-add while the partition has not reached its remove cut: the records before the remove cut
                // are forwarded first. The partition moves to the add cut when it reaches the remove cut.
                Long pos = live ? p.firstUnhandled(tp) : committedOffset;
                boolean reached = p.removeReached.contains(tp) || (pos != null && pos >= removed.cut());
                if (!reached) {
                    if (target != null && cuts.contains(tp) && target > removed.cut()) {
                        p.gapEnd.put(tp, target);
                        gapLog.put(tp, target);
                    } else {
                        // The add cut is at or before the remove cut: every record from here on is forwarded.
                        p.removeCut.remove(tp);
                        p.gapEnd.remove(tp);
                    }
                    Long h = p.held.remove(tp);
                    if (h != null) {
                        p.seeksToIssue.merge(tp, h, Math::min);
                    }
                    continue;
                }
                p.removeCut.remove(tp);
                p.removeReached.remove(tp);
                p.gapEnd.remove(tp);
            }
            if (target == null) {
                rereadHeld(tp, fromOpen);
                continue;
            }
            long move;
            if (live) {
                move = Math.max(target, Math.max(committedOffset == null ? 0 : committedOffset,
                    p.forwardedNext.getOrDefault(tp, 0L)));
            } else if (committedOffset != null && committedOffset >= target) {
                rereadHeld(tp, fromOpen);
                continue;
            } else {
                move = target;
            }
            if (cuts.contains(tp)) {
                p.dropBelow.put(tp, target);
                p.strictFloors.add(tp);
                cutLog.put(tp, move);
            }
            if (fromOpen) {
                openMoves.put(tp, move);
                p.position.put(tp, move);
            } else {
                p.seeksToIssue.put(tp, move);
                p.held.remove(tp);
            }
        }
        if (!openMoves.isEmpty()) {
            applyMoves.accept(openMoves);
        }
        if (!cutLog.isEmpty()) {
            log.info("Control topic: {} partitions start at the add time of their topic: {}", cutLog.size(), cutLog);
        }
        if (!removeLog.isEmpty()) {
            log.info("Control topic: {} partitions of removed topics forward their records below the remove cut (the "
                + "first offset at or after the remove time) and are then paused: {}", removeLog.size(), removeLog);
        }
        if (!gapLog.isEmpty()) {
            log.info("Control topic: {} re-added partitions first forward their records before the remove cut, then "
                + "move to the add cut: {}", gapLog.size(), gapLog);
        }
        if (fromOpen && startTimestamp >= 0 && openMoves.size() > cutLog.size()) {
            log.info("Start timestamp {}: moved {} assigned partitions without a committed offset", startTimestamp,
                openMoves.size() - cutLog.size());
        }
        if (!failed.isEmpty()) {
            nextCutRetry = clock.getAsLong() + CONTROL_RETRY_MS;
            String message = "Control topic: could not find the start position or remove cut of {}; they stay paused, "
                + "their commits stay where they are, and the lookup is tried again in {} ms. Other partitions are "
                + "not affected. Errors: {}";
            if (fromOpen) {
                log.warn(message, failed.keySet(), CONTROL_RETRY_MS, failed);
            } else {
                log.debug(message, failed.keySet(), CONTROL_RETRY_MS, failed);
            }
        }
    }

    /**
     * Takes the remove cut of a partition from its committed ledger when it is opened, or notes that it must be
     * looked up by the remove time:
     *
     * <ul>
     *   <li>{@code removed:<t>:<cut>}: the cut of the ledger is used, so that every owner applies the same cut. For an
     *       allowed topic only while the committed offset is before the cut.</li>
     *   <li>{@code allowed:<t>} and the control topic shows a remove after that add: the task that applied the remove
     *       had not committed since. The cut is looked up at the remove time.</li>
     *   <li>{@code removed:<t>} without a cut, no ledger, or a remove that the control topic no longer shows: no cut.
     *       No record after the committed offset is forwarded before the next add.</li>
     * </ul>
     */
    private boolean removeCutFromLedger(TopicPartition tp, ControlLedger raw, Long committedOffset) {
        if (raw == null || p.removeCut.containsKey(tp) || p.removeCutNeeded.containsKey(tp)) {
            return false;
        }
        String topic = tp.topic();
        if (!raw.allowed()) {
            if (raw.hasCut()) {
                boolean reached = committedOffset != null && committedOffset >= raw.cut();
                if (!p.allowed.contains(topic) || !reached) {
                    p.removeCut.put(tp, raw);
                    if (reached) {
                        p.removeReached.add(tp);
                    }
                    return true;
                }
            }
            return false;
        }
        Long removedAt = p.removeTimes.get(topic);
        if (removedAt != null && (!p.allowed.contains(topic) || p.removedSinceLedger(topic, raw))) {
            p.removeCutNeeded.put(tp, new ControlPartitions.RemoveNeed(removedAt, raw));
        }
        return false;
    }

    /**
     * A remove cut from the ledger cannot lie after the end of the partition: the ledger was written by a build that
     * knew the cut as an offset of this log. A larger cut comes from a foreign or damaged ledger, or from a log that
     * was truncated (topic deleted and created again, unclean leader election). It is capped at the end offset, so
     * that records written from now on are not forwarded.
     */
    private void capLedgerCut(TopicPartition tp, long endOffset, Long committedOffset) {
        ControlLedger raw = p.removeCut.get(tp);
        if (raw == null || raw.cut() <= endOffset) {
            return;
        }
        log.warn("Control topic: the committed ledger of {} has the remove cut {}, after the end offset {} of the "
            + "partition (a foreign or damaged ledger, or a truncated log). The cut is capped at {}.", tp, raw.cut(),
            endOffset, endOffset);
        p.removeCut.remove(tp);
        p.removeReached.remove(tp);
        removeCutFromLedger(tp, ControlLedger.removedAt(raw.timeMs(), endOffset), committedOffset);
    }

    /**
     * Applies a remove cut that was just looked up. A partition whose position is at or after the cut is paused. If
     * records were held while the lookup was pending, the partition moves back to the first of them, so the records
     * before the cut are forwarded. While the reader is behind, the recovery of the reader does this move.
     */
    private void applyRemoveCut(TopicPartition tp, ControlLedger cut) {
        p.removeCut.put(tp, cut);
        Long pos = p.firstUnhandled(tp);
        if (pos != null && pos >= cut.cut()) {
            p.removeReached.add(tp);
        }
        if (!p.allowed.contains(tp.topic()) && !p.stalled) {
            // For an allowed topic (a re-add in the same interval) the add decision moves the partition.
            Long h = p.held.remove(tp);
            if (h != null) {
                p.seeksToIssue.merge(tp, h, Math::min);
            }
        }
    }

    /**
     * A partition was decided without a move: records dropped while it was not decided are read again, so that they
     * are filtered with the decision. While the reader is behind, the recovery of the reader does this move.
     */
    private void rereadHeld(TopicPartition tp, boolean fromOpen) {
        if (fromOpen || p.stalled) {
            return;
        }
        Long h = p.held.remove(tp);
        if (h != null) {
            p.seeksToIssue.merge(tp, h, Math::min);
        }
    }

    /** Marks every partition without a result as failed if the time budget is used up. */
    private void failAllIfOverBudget(long deadline, Collection<TopicPartition> partitions,
                                     Map<TopicPartition, Exception> failed) {
        if (clock.getAsLong() > deadline) {
            TimeoutException e = new TimeoutException("The lookups took longer than " + LOOKUP_BUDGET_MS + " ms");
            for (TopicPartition tp : partitions) {
                failed.putIfAbsent(tp, e);
            }
        }
    }

    private void checkBudget(long deadline) throws TimeoutException {
        if (clock.getAsLong() > deadline) {
            throw new TimeoutException("The lookups took longer than " + LOOKUP_BUDGET_MS + " ms");
        }
    }
}
