package com.example.fanin;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The control topic feature of one {@link FanInSinkTask}. The task creates it only when fanin.control.topic is set,
 * and calls it from its own methods: {@link #onOpen}, {@link #beforePut}, {@link #accept} and {@link #forwarded} for
 * each record, {@link #afterPut}, {@link #preCommit}, {@link #onClose} and {@link #stop}.
 *
 * <p>It owns the {@link ControlReader} and its {@link ControlState}, applies the changes of the control list, stops
 * all forwarding while the reader is behind, pauses and resumes partitions, decides for each record whether it is
 * forwarded, and warns about listed topics that do not exist. The start positions and remove cuts are decided by
 * {@link ControlCuts}, the offsets and ledgers to commit by {@link ControlCommits}. All three share one
 * {@link ControlPartitions}.
 *
 * <p>Everything here runs on the task thread, including every call of the {@link SinkTaskContext}. Only the reader's
 * background thread writes to the {@link ControlState}.
 */
final class ControlCoordinator {

    /** Logs under the task's logger, so log lines and log configuration stay as they were. */
    private static final Logger log = LoggerFactory.getLogger(FanInSinkTask.class);

    /** Interval of the missing-topic check, of the topic listing cache and of the drop summary. */
    static final long CONTROL_CHECK_MS = Duration.ofMinutes(1).toMillis();

    private final SinkTaskContext context;
    private final Supplier<StartOffsets.Lookup> lookups;
    private final Consumer<String> warn;
    private final LongSupplier clock;
    private final ControlState controlState;
    private ControlReader controlReader;
    private final ControlPartitions p;
    private final ControlCuts cuts;
    private final ControlCommits commits;

    private final long missingWarnMs;
    private final long maxLagMs;
    private final boolean warnTask;
    private final Pattern subscriptionRegex;
    private final Set<String> subscriptionTopics;

    /** The assignment at the start of the current put(). */
    private Set<TopicPartition> assigned;
    private final Set<String> warnedMissing = new HashSet<>();
    private boolean missingChecked;
    private long lastMissingCheck;
    private Set<String> cachedTopicNames;
    private long topicNamesFetchedAt;
    private long dropped;
    private long droppedSince;

    /**
     * Reads the control topic to its end, then keeps reading it on a background thread.
     *
     * @param readers creates the reader; the task passes its overridable factory, so tests can use a MockConsumer
     * @param warn logs a warning about the control list; the task passes its overridable method
     */
    static ControlCoordinator start(FanInSinkConfig config, Map<String, String> props, String topic,
                                    SinkTaskContext context, Supplier<StartOffsets.Lookup> lookups,
                                    BiFunction<String, ControlState, ControlReader> readers, Consumer<String> warn,
                                    LongSupplier clock, long startTimestamp, String consumerGroup) {
        ControlCoordinator coordinator = new ControlCoordinator(config, props, topic, context, lookups, readers, warn,
            clock, startTimestamp, consumerGroup);
        coordinator.controlReader.startBackground();
        return coordinator;
    }

    private ControlCoordinator(FanInSinkConfig config, Map<String, String> props, String topic,
                               SinkTaskContext context, Supplier<StartOffsets.Lookup> lookups,
                               BiFunction<String, ControlState, ControlReader> readers, Consumer<String> warn,
                               LongSupplier clock, long startTimestamp, String consumerGroup) {
        this.context = context;
        this.lookups = lookups;
        this.warn = warn;
        this.clock = clock;
        FanInSinkConfig.AddStart addStart = config.controlAddStart();
        missingWarnMs = config.controlMissingWarnMs();
        maxLagMs = config.controlMaxLagMs();
        warnTask = "0".equals(props.getOrDefault(FanInSinkConnector.TASK_INDEX, "0"));
        String regex = props.get(FanInSinkConfig.TOPICS_REGEX);
        subscriptionRegex = regex == null || regex.isBlank() ? null : Pattern.compile(regex);
        subscriptionTopics = new HashSet<>();
        String topics = props.get(FanInSinkConfig.TOPICS);
        if (topics != null) {
            for (String t : topics.split(",")) {
                if (!t.isBlank()) {
                    subscriptionTopics.add(t.trim());
                }
            }
        }
        p = new ControlPartitions(addStart);
        cuts = new ControlCuts(p, lookups, clock, consumerGroup, startTimestamp);
        commits = new ControlCommits(p, clock);
        ControlState state = new ControlState();
        ControlReader reader = readers.apply(topic, state);
        try {
            reader.loadInitial(Duration.ofMillis(config.controlLoadTimeoutMs()));
        } catch (RuntimeException e) {
            reader.close();
            throw e;
        }
        state.drainChanges(); // the start state is applied in open(), not as changes
        Map<String, Long> addedAt = state.addedAt();
        p.removeTimes.putAll(state.removedAt());
        for (String t : state.allowed()) {
            p.allowed.add(t);
            p.addTimes.put(t, addedAt.get(t));
            checkSubscription(t);
        }
        controlState = state;
        controlReader = reader;
        droppedSince = clock.getAsLong();
        log.info("Control topic {}: {} allowed topics, added topics start at {}", topic, p.allowed.size(), addStart);
    }

    /** The control state. For tests. */
    ControlState state() {
        return controlState;
    }

    /**
     * Pauses the partitions that must not run, then decides the start position of each partition (see
     * {@link ControlCuts#decideOnOpen}): the add-time cut where the ledger in the committed offset metadata asks for
     * it, the remove cut, and the start timestamp. Partitions that cannot be decided yet stay paused.
     */
    void onOpen(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            p.forget(tp);
            p.resumeNeeded.add(tp);
            if (!p.allowed.contains(tp.topic())) {
                p.notAllowedSeen.add(tp);
            }
        }
        pauseWhatMustNotRun(partitions);
        cuts.decideOnOpen(partitions, context::offset);
        pauseWhatMustNotRun(partitions);
    }

    /** Forgets the closed partitions. */
    void onClose(Collection<TopicPartition> partitions) {
        partitions.forEach(p::forget);
    }

    /**
     * Start of put(): applies the control changes since the last put(), checks the reader lag and pauses the
     * partitions that must not run.
     */
    void beforePut() {
        p.issuedSeeks.clear(); // the framework applied them before this call
        assigned = context.assignment();
        applyControlChanges(assigned);
        checkReaderLag();
        pauseWhatMustNotRun(assigned);
    }

    /** True if the record may be forwarded. A record that is not forwarded is counted for the drop summary. */
    boolean accept(SinkRecord record) {
        if (passesControl(record)) {
            return true;
        }
        dropped++;
        if (log.isDebugEnabled()) {
            log.debug("Dropping {}-{}@{} (control topic)", record.topic(), record.kafkaPartition(),
                record.kafkaOffset());
        }
        return false;
    }

    /** The task handed the record to the producer. A later seek never goes below it. */
    void forwarded(SinkRecord record) {
        p.forwardedNext.merge(new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset() + 1,
            Math::max);
    }

    /** End of a completed put(): issues the seeks, ends the strict floors, pauses and resumes partitions. */
    void afterPut(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            p.delivered.merge(tp, record.kafkaOffset(), Math::max);
            p.position.merge(tp, record.kafkaOffset() + 1, Math::max);
        }
        p.seeksToIssue.keySet().retainAll(assigned);
        if (!p.seeksToIssue.isEmpty()) {
            Map<TopicPartition, Long> seeks = new HashMap<>();
            p.seeksToIssue.forEach((tp, offset) -> seeks.put(tp,
                Math.max(offset, p.forwardedNext.getOrDefault(tp, 0L))));
            context.offset(seeks);
            p.issuedSeeks.putAll(seeks);
            seeks.forEach((tp, offset) -> {
                p.delivered.remove(tp);
                p.held.remove(tp);
                p.position.put(tp, offset);
                ControlLedger removed = p.removeCut.get(tp);
                if (removed != null && offset < removed.cut()) {
                    p.removeReached.remove(tp); // it reads records before its remove cut again
                }
            });
            log.info("Control topic: moving {} partitions: {}", seeks.size(), seeks);
            p.seeksToIssue.clear();
        }
        p.strictFloors.clear();
        pauseWhatMustNotRun(assigned); // partitions that reached their remove cut in this put()
        resumeWhatMayRun(assigned);
        housekeeping();
    }

    /** The offsets to commit, with the ledger of each partition; see {@link ControlCommits#preCommit}. */
    Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return commits.preCommit(currentOffsets, context.assignment());
    }

    /** Closes the reader. */
    void stop() {
        ControlReader reader = controlReader;
        controlReader = null;
        if (reader != null) {
            try {
                reader.close();
            } catch (RuntimeException e) {
                log.warn("Closing the control topic reader failed: {}", e.toString());
            }
        }
    }

    // ---- control list changes ----

    /** Applies the control changes since the last call. Runs on the task thread at the start of put(). */
    private void applyControlChanges(Set<TopicPartition> assigned) {
        Throwable failure = controlReader.failure();
        if (failure != null) {
            throw new ConnectException("Reading the control topic failed: " + failure, failure);
        }
        ControlState.Changes changes = controlState.drainChanges();
        if (!changes.isEmpty()) {
            for (String t : changes.removed()) {
                p.allowed.remove(t);
                Long addedAt = p.addTimes.remove(t);
                Long removedAt = changes.removeTimes().get(t);
                if (removedAt != null) {
                    p.removeTimes.put(t, removedAt);
                }
                warnedMissing.remove(t);
                List<TopicPartition> tps = partitionsOf(t, assigned);
                for (TopicPartition tp : tps) {
                    boolean running = p.isRunningForRemoveCut(tp);
                    p.notAllowedSeen.add(tp);
                    ControlLedger ledger = p.committedLedger.get(tp);
                    if (ledger != null && ledger.allowed()) {
                        p.committedLedger.put(tp, ControlLedger.removedAt(removedAt != null ? removedAt
                            : ledger.timeMs()));
                    }
                    if (p.committedLedger.containsKey(tp)) {
                        p.pending.remove(tp);
                        p.pendingReason.remove(tp);
                    } else if (p.pending.containsKey(tp)) {
                        p.pending.put(tp, false);
                    }
                    // A partition that has not reached the cut of an earlier remove keeps that cut, and an add cut
                    // that it was waiting for no longer applies.
                    p.gapEnd.remove(tp);
                    if (running && removedAt != null && addedAt != null) {
                        p.removeCutNeeded.put(tp, new ControlPartitions.RemoveNeed(removedAt,
                            ControlLedger.allowedAt(addedAt)));
                        cuts.retrySoon();
                    }
                }
                log.info("Control topic: removed {} at {}; its {} assigned partitions forward their records written "
                    + "before that time and are then paused", t, removedAt, tps.size());
            }
            for (String t : changes.added()) {
                boolean wasAllowed = p.allowed.contains(t);
                Long oldAddTime = p.addTimes.get(t);
                p.allowed.add(t);
                p.addTimes.put(t, changes.addTimes().get(t));
                boolean readd = changes.readded().contains(t);
                if (readd && changes.removeTimes().get(t) != null) {
                    p.removeTimes.put(t, changes.removeTimes().get(t));
                }
                checkSubscription(t);
                List<TopicPartition> tps = partitionsOf(t, assigned);
                List<TopicPartition> cut = new ArrayList<>();
                for (TopicPartition tp : tps) {
                    if (p.addStart == FanInSinkConfig.AddStart.LATEST || readd) {
                        cut.add(tp);
                    } else if (p.committedLedger.containsKey(tp) && p.needsCut(t, p.committedLedger.get(tp))) {
                        // EARLIEST, but the committed ledger shows a remove (also after its tombstone is gone): the
                        // same rule as in open().
                        cut.add(tp);
                    }
                    // A partition whose ledger is not known yet stays pending; the decision applies needsCut later.
                }
                Long readdRemoveTime = changes.removeTimes().get(t);
                for (TopicPartition tp : cut) {
                    if (readd && wasAllowed && readdRemoveTime != null && oldAddTime != null
                        && p.isRunningForRemoveCut(tp)) {
                        // Remove and add in one interval: the partition first needs the cut of the remove, so that
                        // its records written before the remove time are forwarded.
                        p.removeCutNeeded.put(tp, new ControlPartitions.RemoveNeed(readdRemoveTime,
                            ControlLedger.allowedAt(oldAddTime)));
                    }
                    p.pending.put(tp, true);
                }
                if (!cut.isEmpty()) {
                    cuts.retrySoon();
                }
                if (cut.size() == tps.size()) {
                    log.info("Control topic: added {}{}, it starts at its add time {}", t, readd ? " again" : "",
                        p.addTimes.get(t));
                } else {
                    log.info("Control topic: added {}, {} assigned partitions start at the add time {} (their "
                        + "committed ledger shows a remove), the others resume", t, cut.size(), p.addTimes.get(t));
                }
            }
        }
        p.pending.keySet().retainAll(assigned);
        p.removeCutNeeded.keySet().retainAll(assigned);
        p.pendingReason.keySet().removeIf(tp -> !p.pending.containsKey(tp) && !p.removeCutNeeded.containsKey(tp));
        cuts.retryPendingIfDue();
    }

    private static List<TopicPartition> partitionsOf(String topic, Collection<TopicPartition> assigned) {
        List<TopicPartition> result = new ArrayList<>();
        for (TopicPartition tp : assigned) {
            if (tp.topic().equals(topic)) {
                result.add(tp);
            }
        }
        return result;
    }

    /**
     * Stops all forwarding while the reader has not read the control topic to its end for more than
     * fanin.control.max.lag.ms, so that a remove that the reader cannot see yet takes effect within that time.
     */
    private void checkReaderLag() {
        long now = clock.getAsLong();
        long caughtUpAt = controlReader.caughtUpAt();
        boolean behind = now - caughtUpAt > maxLagMs;
        if (behind && !p.stalled) {
            p.stalled = true;
            log.warn("Control topic: the reader has not read the control topic to its end since {} ({} ms, more than "
                + "{}={}). The task pauses all partitions and forwards nothing until the reader has caught up.",
                caughtUpAt, now - caughtUpAt, FanInSinkConfig.CONTROL_MAX_LAG_MS, maxLagMs);
        } else if (!behind && p.stalled) {
            p.stalled = false;
            log.info("Control topic: the reader has caught up again; forwarding resumes");
            // Records dropped while the reader was behind are read again and filtered with the current list.
            p.held.forEach((tp, offset) -> {
                if (!p.pending.containsKey(tp) && !p.removeCutNeeded.containsKey(tp)) {
                    p.seeksToIssue.merge(tp, offset, Math::min);
                }
            });
        }
    }

    // ---- per record ----

    /** True if the record may be forwarded. */
    private boolean passesControl(SinkRecord record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        long offset = record.kafkaOffset();
        Long last = p.delivered.get(tp);
        if (last != null && offset <= last) {
            offsetWentBack(tp, offset, last);
        }
        if (p.stalled || p.pending.containsKey(tp) || p.removeCutNeeded.containsKey(tp)) {
            p.held.merge(tp, offset, Math::min);
            return false;
        }
        // The floor of an add cut comes first, for a removed topic too: after a RetriableException the framework
        // redelivers a batch whose records below the floor were written while the topic was removed, and a remove
        // in between must not let them through.
        Long floor = p.dropBelow.get(tp);
        if (floor != null && offset < floor) {
            if (p.strictFloors.contains(tp)) {
                return false;
            }
            offsetWentBack(tp, offset, floor);
        }
        ControlLedger removed = p.removeCut.get(tp);
        if (!p.allowed.contains(record.topic())) {
            if (removed != null) {
                if (offset < removed.cut()) {
                    return true; // written before the remove time
                }
                // The position reached the remove cut: the partition is paused at the end of this put(). The commit
                // stays at the first dropped record, so a re-add can still move back to records after its add time.
                p.removeReached.add(tp);
                p.removedHold.merge(tp, offset, Math::min);
                return false;
            }
            Long removedAt = p.removeTimes.get(record.topic());
            Long timestamp = record.timestamp();
            if (removedAt != null && (timestamp == null || timestamp < 0 || timestamp >= removedAt)) {
                // Written after the remove: if the topic is added again soon, records after the add time are in
                // this range, and a commit past them would make the re-add skip them.
                p.removedHold.merge(tp, offset, Math::min);
            }
            return false;
        }
        if (removed != null && offset >= removed.cut()) {
            // A re-added partition reached its remove cut: the records up to the add cut were written while the
            // topic was removed. Drop them and move to the add cut.
            Long end = p.gapEnd.remove(tp);
            p.removeCut.remove(tp);
            p.removeReached.remove(tp);
            if (end != null && offset < end) {
                p.dropBelow.put(tp, end);
                p.strictFloors.add(tp);
                p.seeksToIssue.merge(tp, end, Math::max);
                return false;
            }
        }
        return true;
    }

    /**
     * The consumer delivered an offset below one that this task has already passed, without a seek of this task: the
     * topic was deleted and created again, or its offsets were reset. The old floor and positions do not apply to the
     * new offsets.
     */
    private void offsetWentBack(TopicPartition tp, long offset, long before) {
        log.info("Control topic: offset of {} went back from {} to {} without a seek of this task (topic created "
            + "again or offsets reset); its old cut position is cleared", tp, before, offset);
        p.forgetPositions(tp);
    }

    // ---- pause and resume ----

    /** Pauses the partitions that must not run. Runs at once, so it also works when put() fails afterwards. */
    private void pauseWhatMustNotRun(Collection<TopicPartition> partitions) {
        List<TopicPartition> toPause = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            if (!p.shouldRun(tp) && !p.pausedByTask.contains(tp)) {
                toPause.add(tp);
            }
        }
        if (!toPause.isEmpty()) {
            context.pause(toPause.toArray(new TopicPartition[0]));
            p.pausedByTask.addAll(toPause);
            p.resumeNeeded.removeAll(toPause);
        }
    }

    /**
     * Resumes the partitions that may run again. Runs only at the end of a completed put(): the framework redelivers
     * a batch after a RetriableException with all partitions paused, and a resume during that time would let the
     * consumer fetch new records into the batch.
     */
    private void resumeWhatMayRun(Collection<TopicPartition> assigned) {
        List<TopicPartition> toResume = new ArrayList<>();
        for (TopicPartition tp : assigned) {
            if (p.shouldRun(tp) && (p.pausedByTask.contains(tp) || p.resumeNeeded.contains(tp))) {
                toResume.add(tp);
            }
        }
        if (!toResume.isEmpty()) {
            context.resume(toResume.toArray(new TopicPartition[0]));
            p.pausedByTask.removeAll(toResume);
            p.resumeNeeded.removeAll(toResume);
        }
    }

    // ---- warnings and summaries ----

    private void checkSubscription(String topic) {
        if (!warnTask) {
            return;
        }
        boolean matches = subscriptionRegex != null
            ? subscriptionRegex.matcher(topic).matches()
            : subscriptionTopics.contains(topic);
        if (!matches) {
            warn.accept("Control topic lists " + topic + " but it does not match the subscription of this connector");
        }
    }

    /** Drop summary and missing-topic check, at most once per {@link #CONTROL_CHECK_MS}. */
    private void housekeeping() {
        long now = clock.getAsLong();
        if (dropped > 0 && now - droppedSince >= CONTROL_CHECK_MS) {
            log.info("Control topic: dropped {} records in the last {} s (topics not allowed, start position not yet "
                + "applied, or the reader behind)", dropped, (now - droppedSince) / 1000);
            dropped = 0;
            droppedSince = now;
        }
        if (!warnTask || (missingChecked && now - lastMissingCheck < CONTROL_CHECK_MS)) {
            return;
        }
        missingChecked = true;
        lastMissingCheck = now;
        Set<String> assignedTopics = new HashSet<>();
        context.assignment().forEach(tp -> assignedTopics.add(tp.topic()));
        List<String> candidates = new ArrayList<>();
        for (String t : p.allowed) {
            Long at = p.addTimes.get(t);
            if (at != null && !warnedMissing.contains(t) && !assignedTopics.contains(t) && now - at >= missingWarnMs) {
                candidates.add(t);
            }
        }
        if (candidates.isEmpty()) {
            return;
        }
        Set<String> existing;
        try {
            if (cachedTopicNames == null || now - topicNamesFetchedAt >= CONTROL_CHECK_MS) {
                cachedTopicNames = lookups.get().topicNames();
                topicNamesFetchedAt = now;
            }
            existing = cachedTopicNames;
        } catch (Exception | LinkageError e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.debug("Listing topics for the missing-topic check failed: {}", e.toString());
            return;
        }
        for (String t : candidates) {
            if (!existing.contains(t)) {
                warnedMissing.add(t);
                warn.accept("Control topic lists " + t + " but no such topic exists for "
                    + (now - p.addTimes.get(t)) / 60_000 + " min");
            }
        }
    }
}
