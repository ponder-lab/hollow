package com.netflix.hollow.core.util;

import static com.netflix.hollow.core.util.Threads.daemonThread;
import static com.netflix.hollow.test.AssertShim.assertEquals;
import static com.netflix.hollow.test.AssertShim.fail;

import org.junit.jupiter.api.Test;

// TODO(timt): tag as MEDIUM test
public class ThreadsTest {
    @Test
    public void described() {
        Thread thread = daemonThread(() -> {}, getClass(), "howdy");

        assertEquals("hollow | ThreadsTest | howdy", thread.getName());
    }

    @Test
    public void nullRunnable() {
        try {
            daemonThread(null, getClass(), "boom");
            fail("expected an exception");
        } catch (NullPointerException e) {
            assertEquals("runnable required", e.getMessage());
        }
    }

    @Test
    public void nullContext() {
        try {
            daemonThread(() -> {}, null, "boom");
            fail("expected an exception");
        } catch (NullPointerException e) {
            assertEquals("context required", e.getMessage());
        }
    }

    @Test
    public void nullDescription() {
        try {
            daemonThread(() -> {}, getClass(), null);
            fail("expected an exception");
        } catch (NullPointerException e) {
            assertEquals("description required", e.getMessage());
        }
    }
}
