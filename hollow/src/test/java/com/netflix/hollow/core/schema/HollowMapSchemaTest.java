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
package com.netflix.hollow.core.schema;

import com.netflix.hollow.core.index.key.PrimaryKey;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

public class HollowMapSchemaTest {

    @Test
    public void testEquals() {
        {
            HollowMapSchema s1 = new HollowMapSchema("Test", "TypeA", "TypeB");
            HollowMapSchema s2 = new HollowMapSchema("Test", "TypeA", "TypeB");

            assertEquals(s1, s2);
        }

        {
            HollowMapSchema s1 = new HollowMapSchema("Test", "TypeA", "TypeB");
            HollowMapSchema s2 = new HollowMapSchema("Test2", "TypeA", "TypeB");

            assertNotEquals(s1, s2);
        }

        {
            HollowMapSchema s1 = new HollowMapSchema("Test", "TypeA", "TypeB");
            HollowMapSchema s2 = new HollowMapSchema("Test", "TypeB", "TypeB");

            assertNotEquals(s1, s2);
        }

        {
            HollowMapSchema s1 = new HollowMapSchema("Test", "TypeA", "TypeB");
            HollowMapSchema s2 = new HollowMapSchema("Test", "TypeA", "TypeC");

            assertNotEquals(s1, s2);
        }
    }

    @Test
    public void testEqualsWithKeys() {
        {
            HollowMapSchema s1 = new HollowMapSchema("Test", "TypeA", "TypeB", "f1");
            HollowMapSchema s2 = new HollowMapSchema("Test", "TypeA", "TypeB", "f1");

            assertEquals(s1, s2);
            assertEquals(s1.getHashKey(), s2.getHashKey());
            assertEquals(new PrimaryKey("TypeA", "f1"), s2.getHashKey());
        }

        {
            HollowMapSchema s1 = new HollowMapSchema("Test", "TypeA", "TypeB", "f1", "f2");
            HollowMapSchema s2 = new HollowMapSchema("Test", "TypeA", "TypeB", "f1", "f2");

            assertEquals(s1, s2);
            assertEquals(s1.getHashKey(), s2.getHashKey());
            assertEquals(new PrimaryKey("TypeA", "f1", "f2"), s2.getHashKey());
        }

        {
            HollowMapSchema s1 = new HollowMapSchema("Test", "TypeA", "TypeB");
            HollowMapSchema s2 = new HollowMapSchema("Test", "TypeA", "TypeB", "f1");

            assertNotEquals(s1, s2);
            assertNotEquals(s1.getHashKey(), s2.getHashKey());
        }

        {
            HollowMapSchema s1 = new HollowMapSchema("Test", "TypeA", "TypeB", "f1");
            HollowMapSchema s2 = new HollowMapSchema("Test", "TypeA", "TypeB", "f1", "f2");

            assertNotEquals(s1, s2);
            assertNotEquals(s1.getHashKey(), s2.getHashKey());
        }
    }
}