/*
*
*  Copyright 2017 Netflix, Inc.
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
package com.netflix.hollow.core.index;

import com.netflix.hollow.core.util.SimultaneousExecutor;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for SparseBitSet implementation defined in HollowSparseIntegerSet.
 */
public class SparseBitSetTest {

    HollowSparseIntegerSet.SparseBitSet sparseBitSet;

    @Test
    public void testRandom() {

        int maxValue = 5000000;
        sparseBitSet = new HollowSparseIntegerSet.SparseBitSet(maxValue);

        Random random = new Random();
        Set<Integer> intIndexed = new HashSet<>();
        for (int i = 0; i < 100000; i++) {
            int r = random.nextInt(maxValue);
            while (intIndexed.contains(r)) r = random.nextInt(maxValue);
            sparseBitSet.set(r);
            intIndexed.add(r);
        }

        assertEquals(100000, sparseBitSet.cardinality());
        assertTrue(sparseBitSet.estimateBitsUsed() > 0);

        for (int i = 0; i < maxValue; i++) {
            if (intIndexed.contains(i))
                assertTrue("Expected in set, but not found the int " + i, sparseBitSet.get(i));
            else assertFalse("Not expected in set, but found the int " + i, sparseBitSet.get(i));
        }
    }

    @Test
    public void testEvenNumbersMultipleThread() {
        for (int j = 0; j < 10; j++) {
            int maxValue = 500000;
            sparseBitSet = new HollowSparseIntegerSet.SparseBitSet(maxValue);
            SimultaneousExecutor executor = new SimultaneousExecutor(getClass(), "test");
            int parallelism = executor.getMaximumPoolSize();
            int taskSize = maxValue / parallelism;
            for (int i = 0; i < parallelism; i++) {
                int from = i * taskSize;
                int to = (from + taskSize) - 1;
                if (i == (parallelism - 1)) to = maxValue;
                executor.submit(new Task(sparseBitSet, from, to));
            }
            executor.awaitUninterruptibly();
            HollowSparseIntegerSet.SparseBitSet.compact(sparseBitSet);
            assertTrue(sparseBitSet.cardinality() == 250001);
        }

    }

    private static class Task implements Runnable {
        HollowSparseIntegerSet.SparseBitSet set;
        int from;
        int to;

        public Task(HollowSparseIntegerSet.SparseBitSet set, int from, int to) {
            this.set = set;
            this.from = from;
            this.to = to;
        }

        @Override
        public void run() {
            for (int i = from; i <= to; i++) {
                if ((i % 2) == 0)
                    set.set(i);
            }
        }
    }

    @Test
    public void testInsertLongInBuckets() {

        int maxValue = 8192;
        sparseBitSet = new HollowSparseIntegerSet.SparseBitSet(maxValue);

        sparseBitSet.set(8000);// 1st bit in long for bucket 1
        sparseBitSet.set(8001);// 1st bit in long for bucket 1
        sparseBitSet.set(8002);// 1st bit in long for bucket 1
        sparseBitSet.set(8167);// 3rd bit in long for bucket 1
        sparseBitSet.set(8067);// 2nd bit in long for bucket 1

        sparseBitSet.set(0);// 1st bit in long for bucket 0
        sparseBitSet.set(512);// 8th bit in long for bucket 0
        sparseBitSet.set(69);// 2nd bit in long for bucket 0
        sparseBitSet.set(2047); // bit in long for bucket 0
        sparseBitSet.set(4095);// bit in long for bucket 0

        assertTrue(sparseBitSet.get(8000));
        assertTrue(sparseBitSet.get(8001));
        assertTrue(sparseBitSet.get(8002));
        assertTrue(sparseBitSet.get(8167));
        assertTrue(sparseBitSet.get(8067));

        assertTrue(sparseBitSet.get(0));
        assertTrue(sparseBitSet.get(4095));
        assertTrue(sparseBitSet.get(512));

        assertFalse(sparseBitSet.get(8003));
        assertFalse(sparseBitSet.get(8063));
        assertFalse(sparseBitSet.get(7999));
        assertFalse(sparseBitSet.get(8168));
        assertFalse(sparseBitSet.get(8191));
    }

    @Test
    public void testRemoveLongInBuckets() {
        int maxValue = 8192;
        sparseBitSet = new HollowSparseIntegerSet.SparseBitSet(maxValue);

        sparseBitSet.set(8000);// 1st bit in long for bucket 1
        sparseBitSet.set(8001);// 1st bit in long for bucket 1
        sparseBitSet.set(8002);// 1st bit in long for bucket 1
        sparseBitSet.set(8167);// 3rd bit in long for bucket 1
        sparseBitSet.set(8067);// 2nd bit in long for bucket 1

        sparseBitSet.clear(8000); // not long removal, plain clear test
        assertFalse(sparseBitSet.get(8000));
        assertTrue(sparseBitSet.get(8001));
        assertTrue(sparseBitSet.get(8002));
        assertTrue(sparseBitSet.get(8067));
        assertTrue(sparseBitSet.get(8167));

        sparseBitSet.clear(8001);
        sparseBitSet.clear(8002);// now first long should be removed from buckets
        assertFalse(sparseBitSet.get(8001));
        assertFalse(sparseBitSet.get(8002));
        assertTrue(sparseBitSet.get(8067));
        assertTrue(sparseBitSet.get(8167));

        // add them back
        sparseBitSet.set(8000);
        sparseBitSet.set(8001);
        sparseBitSet.set(8002);

        // removing long from between other longs in bucket
        sparseBitSet.clear(8067);
        assertTrue(sparseBitSet.get(8000));
        assertTrue(sparseBitSet.get(8001));
        assertTrue(sparseBitSet.get(8002));
        assertTrue(sparseBitSet.get(8167));
        assertFalse(sparseBitSet.get(8067));

        // removing the last long
        sparseBitSet.set(8067);
        sparseBitSet.clear(8167);
        assertTrue(sparseBitSet.get(8000));
        assertTrue(sparseBitSet.get(8001));
        assertTrue(sparseBitSet.get(8002));
        assertTrue(sparseBitSet.get(8067));
        assertFalse(sparseBitSet.get(8167));

        // removing all longs
        sparseBitSet.clear(8000);
        sparseBitSet.clear(8001);
        sparseBitSet.clear(8002);
        sparseBitSet.clear(8167);
        sparseBitSet.clear(8067);

        assertFalse(sparseBitSet.get(8000));
        assertFalse(sparseBitSet.get(8001));
        assertFalse(sparseBitSet.get(8002));
        assertFalse(sparseBitSet.get(8067));
        assertFalse(sparseBitSet.get(8167));

        sparseBitSet.set(8000);
        sparseBitSet.set(8001);
        sparseBitSet.set(8002);
        sparseBitSet.set(8067);
        sparseBitSet.set(8167);

        assertTrue(sparseBitSet.get(8000));
        assertTrue(sparseBitSet.get(8001));
        assertTrue(sparseBitSet.get(8002));
        assertTrue(sparseBitSet.get(8167));
        assertTrue(sparseBitSet.get(8067));
    }

    @Test
    public void testFindMaxValue() {
        int maxValue = 8192;
        sparseBitSet = new HollowSparseIntegerSet.SparseBitSet(maxValue);
        sparseBitSet.set(8000);
        sparseBitSet.set(8001);
        sparseBitSet.set(8002);
        assertEquals(8002, sparseBitSet.findMaxValue());
        sparseBitSet.set(8067);
        assertEquals(8067, sparseBitSet.findMaxValue());
        sparseBitSet.set(8167);
        assertEquals(8167, sparseBitSet.findMaxValue());
        sparseBitSet.clear(8167);
        assertEquals(8067, sparseBitSet.findMaxValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
        int maxValue = 8192;
        sparseBitSet = new HollowSparseIntegerSet.SparseBitSet(maxValue);
        sparseBitSet.set(-1);
    }

    @Test
    public void testCompaction() {
        int maxValue = 8192;
        sparseBitSet = new HollowSparseIntegerSet.SparseBitSet(maxValue);

        sparseBitSet.set(0);// 1st bit in long for bucket 0
        sparseBitSet.set(512);// 8th bit in long for bucket 0
        sparseBitSet.set(69);// 2nd bit in long for bucket 0
        sparseBitSet.set(2047); // bit in long for bucket 0
        sparseBitSet.set(4095);// bit in long for bucket 0

        assertTrue(sparseBitSet.get(0));
        assertTrue(sparseBitSet.get(4095));
        assertTrue(sparseBitSet.get(512));

        HollowSparseIntegerSet.SparseBitSet compactSparseBitSet = HollowSparseIntegerSet.SparseBitSet.compact(sparseBitSet);

        assertTrue(compactSparseBitSet.get(0));
        assertTrue(compactSparseBitSet.get(4095));
        assertTrue(compactSparseBitSet.get(512));

        boolean addedValueGreaterThanMax = true;
        try {
            compactSparseBitSet.set(4096);
        } catch (Exception e) {
            addedValueGreaterThanMax = false;
        }

        if (addedValueGreaterThanMax)
            fail("Should not be ale to set a value greater than max value in compacted bit set.");

    }

    @Test
    public void testResize() {
        int maxValue = 8192;
        sparseBitSet = new HollowSparseIntegerSet.SparseBitSet(maxValue);

        sparseBitSet.set(0);// 1st bit in long for bucket 0
        sparseBitSet.set(512);// 8th bit in long for bucket 0
        sparseBitSet.set(69);// 2nd bit in long for bucket 0
        sparseBitSet.set(2047); // bit in long for bucket 0
        sparseBitSet.set(4095);// bit in long for bucket 0

        assertTrue(sparseBitSet.get(0));
        assertTrue(sparseBitSet.get(4095));
        assertTrue(sparseBitSet.get(512));

        HollowSparseIntegerSet.SparseBitSet compactSparseBitSet = HollowSparseIntegerSet.SparseBitSet.compact(sparseBitSet);

        boolean addedValueGreaterThanMax = true;
        try {
            compactSparseBitSet.set(4096);
        } catch (Exception e) {
            addedValueGreaterThanMax = false;
        }

        if (addedValueGreaterThanMax)
            fail("Should not be ale to set a value greater than max value in compacted bit set.");

        HollowSparseIntegerSet.SparseBitSet resizedSparsedBitSet = HollowSparseIntegerSet.SparseBitSet.resize(compactSparseBitSet, 8192);
        resizedSparsedBitSet.set(4096);
        assertTrue(resizedSparsedBitSet.get(0));
        assertTrue(resizedSparsedBitSet.get(0));
        assertTrue(resizedSparsedBitSet.get(4095));
        assertTrue(resizedSparsedBitSet.get(512));
    }
}
