package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Net changes of the allowed topic list between two drains. */
class ControlStateTest {

    @Test
    void apply_addThenRemove_netRemoved() {
        ControlState state = new ControlState();
        state.apply("a", true, 10);
        state.apply("a", false, 20);

        ControlState.Changes changes = state.drainChanges();
        assertEquals(Set.of(), changes.added());
        assertEquals(Set.of("a"), changes.removed());
        assertFalse(state.isAllowed("a"));
    }

    @Test
    void apply_removeThenAdd_markedReadd() {
        ControlState state = new ControlState();
        state.apply("a", true, 10);
        state.drainChanges();

        state.apply("a", false, 20);
        state.apply("a", true, 30);

        ControlState.Changes changes = state.drainChanges();
        assertEquals(Set.of("a"), changes.added());
        assertEquals(Set.of(), changes.removed());
        assertEquals(Set.of("a"), changes.readded());
        assertTrue(state.isAllowed("a"));
        assertEquals(Map.of("a", 30L), state.addedAt());
    }

    @Test
    void apply_removeAndAddInSeparateIntervals_stillReadd() {
        ControlState state = new ControlState();
        state.apply("a", true, 10);
        state.drainChanges();
        state.apply("a", false, 20);
        assertEquals(Set.of("a"), state.drainChanges().removed());

        state.apply("a", true, 30);
        ControlState.Changes changes = state.drainChanges();
        assertEquals(Set.of("a"), changes.added());
        assertEquals(Set.of("a"), changes.readded());
    }

    @Test
    void apply_firstAdd_isNotReadd() {
        ControlState state = new ControlState();
        state.apply("a", true, 10);

        ControlState.Changes changes = state.drainChanges();
        assertEquals(Set.of("a"), changes.added());
        assertEquals(Set.of(), changes.readded());
        assertEquals(Map.of("a", 10L), state.addedAt());
    }

    @Test
    void apply_duplicateAdd_keepsTheFirstAddTimeAndIsNoChange() {
        ControlState state = new ControlState();
        state.apply("a", true, 10);
        state.drainChanges();

        state.apply("a", true, 50);

        assertTrue(state.drainChanges().isEmpty());
        assertEquals(Map.of("a", 10L), state.addedAt());
    }

    @Test
    void apply_removeOfUnknownTopic_isNoChange() {
        ControlState state = new ControlState();
        state.apply("x", false, 10);
        assertTrue(state.drainChanges().isEmpty());
        assertFalse(state.wasRemoved("x"));
    }

    @Test
    void drain_isEmptyAfterDrain() {
        ControlState state = new ControlState();
        state.apply("a", true, 10);
        state.apply("b", true, 10);
        assertFalse(state.drainChanges().isEmpty());

        ControlState.Changes second = state.drainChanges();
        assertTrue(second.isEmpty());
        assertEquals(Set.of("a", "b"), state.allowed());
    }

    /** The add and remove times come with the changes, under the same lock. */
    @Test
    void drain_carriesAddAndRemoveTimes() {
        ControlState state = new ControlState();
        state.apply("a", true, 10);
        state.apply("b", true, 11);
        state.drainChanges();
        state.apply("a", false, 20);
        state.apply("a", true, 30);
        state.apply("b", false, 40);
        state.apply("c", true, 50);

        ControlState.Changes changes = state.drainChanges();
        assertEquals(Map.of("a", 30L, "c", 50L), changes.addTimes());
        assertEquals(Map.of("a", 20L, "b", 40L), changes.removeTimes());
        assertEquals(Set.of("a"), changes.readded());
    }
}
