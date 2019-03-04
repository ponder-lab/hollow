package com.netflix.hollow.api.producer.enforcer;

import java.util.concurrent.TimeUnit;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

public class BasicSingleProducerEnforcerTest {

    @Test
    public void testEnableDisable() {
        BasicSingleProducerEnforcer se = new BasicSingleProducerEnforcer();
        assertTrue(se.isPrimary());

        se.disable();
        assertFalse(se.isPrimary());

        se.enable();
        assertTrue(se.isPrimary());
    }

    @Test
    public void testEnabledDisabledCyle() {
        BasicSingleProducerEnforcer se = new BasicSingleProducerEnforcer();
        assertTrue(se.isPrimary());

        se.onCycleStart(1234L);
        se.onCycleComplete(null, 10L, TimeUnit.SECONDS);

        se.disable();
        assertFalse(se.isPrimary());
    }

    @Test
    public void testMultiCycle() {
        BasicSingleProducerEnforcer se = new BasicSingleProducerEnforcer();

        for (int i = 0; i < 10; i++) {
            se.enable();
            assertTrue(se.isPrimary());

            se.onCycleStart(1234L);

            se.disable();
            assertTrue(se.isPrimary());

            se.onCycleComplete(null, 10L, TimeUnit.SECONDS);
            assertFalse(se.isPrimary());
        }
    }

    @Test
    public void testTransitions() {
        BasicSingleProducerEnforcer se = new BasicSingleProducerEnforcer();
        assertTrue(se.isPrimary());

        se.onCycleStart(1234L);
        assertTrue(se.isPrimary());

        se.disable();
        assertTrue(se.isPrimary());

        se.onCycleComplete(null, 10L, TimeUnit.SECONDS);
        assertFalse(se.isPrimary());

        se.enable();
        assertTrue(se.isPrimary());

        se.onCycleStart(1235L);
        assertTrue(se.isPrimary());

        se.disable();
        assertTrue(se.isPrimary());

        se.onCycleComplete(null, 10L, TimeUnit.SECONDS);
        assertFalse(se.isPrimary());
    }

}
