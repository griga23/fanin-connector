package com.example.fanin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * The control list as this task applies it, and what the task knows about each assigned partition. This class only
 * holds state and answers small questions about it. It makes no broker or consumer calls. {@link ControlCoordinator},
 * {@link ControlCuts} and {@link ControlCommits} share one instance. Used on the task thread only.
 */
final class ControlPartitions {

    /**
     * A remove cut that a partition must still look up: the remove time, and the ledger to commit until the cut is
     * known (the ledger from before the remove, so that a restart looks the cut up again).
     */
    record RemoveNeed(long removeTime, ControlLedger before) {
    }

    final FanInSinkConfig.AddStart addStart;

    // ---- the control list ----
    /** The allowed topics as this task applies them; changes take effect when put() drains them. */
    final Set<String> allowed = new HashSet<>();
    /** Add time of each allowed topic, as this task applies it. */
    final Map<String, Long> addTimes = new HashMap<>();
    /** Time of the last remove of a topic, as far as this task knows it. */
    final Map<String, Long> removeTimes = new HashMap<>();
    /** True while the control reader is behind by more than fanin.control.max.lag.ms. */
    boolean stalled;

    // ---- per partition ----
    /**
     * Partitions whose start position is not decided yet. They stay paused and their records are dropped. Value true:
     * the topic was added while this task ran, so the partition is cut at the add time. Value false: open() could not
     * look up the committed offset, so the decision is made from the ledger when the lookup works.
     */
    final Map<TopicPartition, Boolean> pending = new HashMap<>();
    /** Why each pending partition is not decided yet; for the WARN about held commits. */
    final Map<TopicPartition, String> pendingReason = new HashMap<>();
    /** Committed offset that open() saw, for a partition whose open() decision is not made yet. */
    final Map<TopicPartition, Long> committedAtOpen = new HashMap<>();
    /** Assigned partitions whose topic this task saw as not allowed since the partition was opened. */
    final Set<TopicPartition> notAllowedSeen = new HashSet<>();
    /**
     * What preCommit returned last for each partition, kept across rebalances for the life of the task. Used in
     * place of the committed offsets when the consumer group is not found (wrong fanin.start.consumer.group), so a
     * rebalance does not apply a cut again that this task already applied.
     */
    final Map<TopicPartition, OffsetAndMetadata> ownCommits = new HashMap<>();
    /** The ledger of each assigned partition as committed before it was opened. A key exists once it is known. */
    final Map<TopicPartition, ControlLedger> committedLedger = new HashMap<>();
    /** Records below these offsets were read before a cut and are dropped. */
    final Map<TopicPartition, Long> dropBelow = new HashMap<>();
    /** Floors whose seek is not yet followed by a completed put(); records below them are expected there. */
    final Set<TopicPartition> strictFloors = new HashSet<>();
    /** Offset after the last record that this task forwarded, per partition. A seek never goes below it. */
    final Map<TopicPartition, Long> forwardedNext = new HashMap<>();
    /** Highest offset delivered by completed put() calls since the partition was opened or moved. */
    final Map<TopicPartition, Long> delivered = new HashMap<>();
    /** First offset dropped because the partition was pending or the reader was behind. Commits stay at or below it. */
    final Map<TopicPartition, Long> held = new HashMap<>();
    /**
     * First offset of a removed topic that was dropped although its record time is at or after the remove time.
     * Commits stay at or below it, so a re-add can still move back to records that were written after the add.
     */
    final Map<TopicPartition, Long> removedHold = new HashMap<>();
    /** Partitions whose remove cut is not looked up yet. They stay paused and their records are dropped and held. */
    final Map<TopicPartition, RemoveNeed> removeCutNeeded = new HashMap<>();
    /**
     * The remove cut of a partition, as a "removed" ledger with the cut offset. Records below the cut were written
     * before the remove time and are forwarded; records at or after it are dropped. For an allowed topic (a re-add
     * whose partition has not reached its remove cut yet) the entry lives until the partition reaches the cut.
     */
    final Map<TopicPartition, ControlLedger> removeCut = new HashMap<>();
    /** Partitions of a removed topic whose position reached the remove cut. They are paused. */
    final Set<TopicPartition> removeReached = new HashSet<>();
    /** Add cut of a re-added partition that has not reached its remove cut yet. It moves there at the remove cut. */
    final Map<TopicPartition, Long> gapEnd = new HashMap<>();
    /** The next offset that the consumer delivers, as far as this task knows it (committed offset, seeks, records). */
    final Map<TopicPartition, Long> position = new HashMap<>();
    /** Seeks decided in put(); issued at the end of a completed put(). */
    final Map<TopicPartition, Long> seeksToIssue = new HashMap<>();
    /** Seeks issued by the last completed put(); the framework applies them before the next put(). */
    final Map<TopicPartition, Long> issuedSeeks = new HashMap<>();
    /** Partitions that this task paused and did not resume since. */
    final Set<TopicPartition> pausedByTask = new HashSet<>();
    /** Opened partitions that may still be paused from an earlier assignment; resumed at the end of put(). */
    final Set<TopicPartition> resumeNeeded = new HashSet<>();

    ControlPartitions(FanInSinkConfig.AddStart addStart) {
        this.addStart = addStart;
    }

    /**
     * True if a partition of an allowed topic must start at the add time of its topic.
     *
     * <p>The ledger is the control state that a task committed with the offset of this partition. "allowed" means
     * the cut for that add was applied, so a repeated add, a compacted control topic or source records with old
     * timestamps cannot move it again; only a remove after that add (known from the control topic) asks for a new
     * cut. "removed" means the topic was added again since, so it is cut. Without a ledger (first assignment, an
     * offset written without the control topic, or an expired offset) the add-time rule of the configuration applies.
     *
     * <p>"A remove after that add" is found without comparing the remove time with the add time, because the two
     * records can come from writers with different clocks. See {@link #removedSinceLedger}.
     */
    boolean needsCut(String topic, ControlLedger ledger) {
        if (ledger != null) {
            return !ledger.allowed() || removedSinceLedger(topic, ledger);
        }
        return addStart == FanInSinkConfig.AddStart.LATEST || removeTimes.containsKey(topic);
    }

    /**
     * True if the control topic shows a remove of the topic after the add that an "allowed" ledger records.
     *
     * <p>The add time of an allowed topic is the time of the first add after its last remove; a repeated add does not
     * change it. If the control topic shows a remove and the current add time differs from the ledger's add time,
     * the ledger's add came before that remove. Compaction does not break this: it can drop the first add only
     * together with the older remove before it, because a record is only removed when a newer record of the same
     * key exists, so a remove that is still there is followed by all later records of that topic.
     */
    boolean removedSinceLedger(String topic, ControlLedger ledger) {
        Long addTime = addTimes.get(topic);
        return removeTimes.containsKey(topic) && addTime != null && addTime != ledger.timeMs();
    }

    /**
     * True if the committed offset and ledger of the partition must be read from the consumer group: in open(), when
     * open() could not read them, and for a topic that was added while this task ran.
     */
    boolean needsFreshLedger(TopicPartition tp) {
        return Boolean.TRUE.equals(pending.get(tp)) || !committedLedger.containsKey(tp);
    }

    /**
     * The committed ledger as it applies now. An "allowed" ledger of a partition whose topic this task saw as not
     * allowed means the topic was removed after that add, also if the remove record is gone (preCommit writes
     * "removed" for it).
     */
    ControlLedger observedLedger(TopicPartition tp, ControlLedger ledger) {
        if (ledger != null && ledger.allowed()
            && (notAllowedSeen.contains(tp) || !allowed.contains(tp.topic()))) {
            notAllowedSeen.add(tp);
            // The remove time if the control topic still shows it; else the add time is the only time known.
            Long removedAt = removeTimes.get(tp.topic());
            return ControlLedger.removedAt(removedAt != null ? removedAt : ledger.timeMs());
        }
        if (!allowed.contains(tp.topic())) {
            notAllowedSeen.add(tp);
        }
        return ledger;
    }

    /** The first offset of the partition that this task has not forwarded or dropped for good, if known. */
    Long firstUnhandled(TopicPartition tp) {
        Long h = held.get(tp);
        Long p = position.get(tp);
        if (h == null) {
            return p;
        }
        return p == null ? h : Math.min(h, p);
    }

    /**
     * A partition runs if the reader is not behind, its start position and remove cut are decided, and its topic is
     * allowed or the partition has not reached the remove cut of its topic yet.
     */
    boolean shouldRun(TopicPartition tp) {
        if (stalled || pending.containsKey(tp) || removeCutNeeded.containsKey(tp)) {
            return false;
        }
        return allowed.contains(tp.topic()) || (removeCut.containsKey(tp) && !removeReached.contains(tp));
    }

    /**
     * True if the partition of a topic that was allowed until this change forwards with its cut applied, so that a
     * remove gets a remove cut. A partition whose start position is not decided, or that still has an earlier remove
     * cut, does not get a new one.
     */
    boolean isRunningForRemoveCut(TopicPartition tp) {
        return !pending.containsKey(tp) && committedLedger.containsKey(tp) && !removeCut.containsKey(tp)
            && !removeCutNeeded.containsKey(tp);
    }

    /** Forgets everything this task knows about a partition; used when it is opened and closed. */
    void forget(TopicPartition tp) {
        pending.remove(tp);
        pendingReason.remove(tp);
        committedAtOpen.remove(tp);
        notAllowedSeen.remove(tp);
        committedLedger.remove(tp);
        dropBelow.remove(tp);
        strictFloors.remove(tp);
        forwardedNext.remove(tp);
        delivered.remove(tp);
        held.remove(tp);
        removedHold.remove(tp);
        removeCutNeeded.remove(tp);
        removeCut.remove(tp);
        removeReached.remove(tp);
        gapEnd.remove(tp);
        position.remove(tp);
        seeksToIssue.remove(tp);
        issuedSeeks.remove(tp);
        pausedByTask.remove(tp);
        resumeNeeded.remove(tp);
    }

    /** Clears the floor, the positions and the remove cut of a partition whose offsets went back. */
    void forgetPositions(TopicPartition tp) {
        dropBelow.remove(tp);
        strictFloors.remove(tp);
        forwardedNext.remove(tp);
        delivered.remove(tp);
        position.remove(tp);
        removeCut.remove(tp);
        removeReached.remove(tp);
        gapEnd.remove(tp);
    }
}
