package com.netflix.hollow.core.util;

import static com.netflix.hollow.core.HollowConstants.VERSION_LATEST;
import static com.netflix.hollow.core.HollowConstants.VERSION_NONE;

import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

public class VersionsTest {

    @Test
    public void testPrettyPrint() {
        assertEquals(Versions.PRETTY_VERSION_NONE, Versions.prettyVersion(VERSION_NONE));
        assertEquals(Versions.PRETTY_VERSION_LATEST, Versions.prettyVersion(VERSION_LATEST));
        assertEquals("123", Versions.prettyVersion(123l));
    }

}
