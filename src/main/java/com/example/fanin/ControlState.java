package com.example.fanin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The list of allowed source topics, as read from the control topic, and its net changes since the last drain.
 *
 * <p>The control reader thread calls {@link #apply}. The task thread calls {@link #drainChanges} and acts on the
 * result. All methods are synchronized.
 */
final class ControlState {

    /**
     * Net changes since the last drain, taken under one lock. {@code readded} is the subset of {@code added} that was
     * removed at an earlier time, so it must start at its new add time and not at its old position.
     * {@code addTimes} has the add time of each topic in {@code added}. {@code removeTimes} has the time of the last
     * remove of each topic in {@code removed} and in {@code readded}.
     */
    record Changes(Set<String> added, Set<String> removed, Set<String> readded, Map<String, Long> addTimes,
                   Map<String, Long> removeTimes) {
        boolean isEmpty() {
            return added.isEmpty() && removed.isEmpty();
        }
    }

    private final Set<String> allowed = new HashSet<>();
    private final Map<String, Long> addedAt = new HashMap<>();
    private final Map<String, Long> removedAt = new HashMap<>();
    private Set<String> pendingAdded = new HashSet<>();
    private Set<String> pendingRemoved = new HashSet<>();

    /**
     * Applies one control record. An add of an allowed topic and a remove of a topic that is not allowed change
     * nothing, so a repeated add record never moves the add time.
     *
     * @param timeMs the time of the add or remove, the timestamp of the control record
     */
    synchronized void apply(String topic, boolean allow, long timeMs) {
        if (allow) {
            if (!allowed.add(topic)) {
                return;
            }
            addedAt.put(topic, timeMs);
            pendingRemoved.remove(topic);
            pendingAdded.add(topic);
        } else {
            if (!allowed.remove(topic)) {
                return;
            }
            addedAt.remove(topic);
            removedAt.put(topic, timeMs);
            pendingAdded.remove(topic);
            pendingRemoved.add(topic);
        }
    }

    synchronized boolean isAllowed(String topic) {
        return allowed.contains(topic);
    }

    synchronized Set<String> allowed() {
        return Set.copyOf(allowed);
    }

    /** Add time of each allowed topic. */
    synchronized Map<String, Long> addedAt() {
        return Map.copyOf(addedAt);
    }

    /** Time of the last remove of each topic that was removed since this state was created. */
    synchronized Map<String, Long> removedAt() {
        return Map.copyOf(removedAt);
    }

    /** True if the topic was removed at any time since this state was created. */
    synchronized boolean wasRemoved(String topic) {
        return removedAt.containsKey(topic);
    }

    /** Returns the net changes since the last call and starts a new interval. */
    synchronized Changes drainChanges() {
        Set<String> readded = new HashSet<>();
        Map<String, Long> addTimes = new HashMap<>();
        Map<String, Long> removeTimes = new HashMap<>();
        for (String topic : pendingAdded) {
            addTimes.put(topic, addedAt.get(topic));
            if (removedAt.containsKey(topic)) {
                readded.add(topic);
                removeTimes.put(topic, removedAt.get(topic));
            }
        }
        for (String topic : pendingRemoved) {
            removeTimes.put(topic, removedAt.get(topic));
        }
        Changes changes = new Changes(Set.copyOf(pendingAdded), Set.copyOf(pendingRemoved), Set.copyOf(readded),
            Map.copyOf(addTimes), Map.copyOf(removeTimes));
        pendingAdded = new HashSet<>();
        pendingRemoved = new HashSet<>();
        return changes;
    }
}
