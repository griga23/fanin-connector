package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class VersionTest {

    @Test
    void versionIsFilteredFromThePom() {
        assertFalse(Version.get().contains("${"), Version.get());
        assertFalse(Version.get().isBlank());
        assertEquals(Version.get(), new FanInSinkConnector().version());
        assertEquals(Version.get(), new FanInSinkTask().version());
    }
}
