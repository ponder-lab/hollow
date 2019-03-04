/*
 *  Copyright 2016-2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.api.consumer;

import com.netflix.hollow.api.client.HollowUpdatePlan;
import com.netflix.hollow.test.consumer.TestBlob;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

public class HollowUpdatePlanTest {

    @Test
    public void testIsSnapshot() {
        HollowUpdatePlan plan = new HollowUpdatePlan();
        plan.add(new TestBlob(1));

        assertTrue(plan.isSnapshotPlan());

        plan.add(new TestBlob(1, 2));

        assertTrue(plan.isSnapshotPlan());

        plan = new HollowUpdatePlan();

        assertFalse(plan.isSnapshotPlan());

        plan.add(new TestBlob(1, 2));

        assertFalse(plan.isSnapshotPlan());
    }

    @Test
    public void testGetSnapshotTransition() {
        TestBlob snapshotTransition = new TestBlob(1);

        HollowUpdatePlan plan = new HollowUpdatePlan();
        plan.add(snapshotTransition);

        assertSame(snapshotTransition, plan.getSnapshotTransition());

        plan.add(new TestBlob(1, 2));

        assertSame(snapshotTransition, plan.getSnapshotTransition());
    }

    @Test
    public void testGetDeltaTransitionsForSnapshotPlan() {
        TestBlob snapshotTransition = new TestBlob(1);

        HollowUpdatePlan plan = new HollowUpdatePlan();
        plan.add(snapshotTransition);

        assertTrue(plan.getDeltaTransitions().isEmpty());

        TestBlob delta1 = new TestBlob(1, 2);
        plan.add(delta1);

        assertEquals(1, plan.getDeltaTransitions().size());

        TestBlob delta2 = new TestBlob(2, 3);
        plan.add(delta2);

        assertEquals(2, plan.getDeltaTransitions().size());
        assertSame(snapshotTransition, plan.getSnapshotTransition());
        assertSame(delta1, plan.getDeltaTransitions().get(0));
        assertSame(delta2, plan.getDeltaTransitions().get(1));
    }

    @Test
    public void testGetDeltaTransitionsForDeltaPlan() {
        HollowUpdatePlan plan = new HollowUpdatePlan();

        assertTrue(plan.getDeltaTransitions().isEmpty());

        TestBlob delta1 = new TestBlob(1, 2);
        plan.add(delta1);

        assertEquals(1, plan.getDeltaTransitions().size());

        TestBlob delta2 = new TestBlob(2, 3);
        plan.add(delta2);

        assertEquals(2, plan.getDeltaTransitions().size());
        assertNull(plan.getSnapshotTransition());
        assertSame(delta1, plan.getDeltaTransitions().get(0));
        assertSame(delta2, plan.getDeltaTransitions().get(1));
    }

}
