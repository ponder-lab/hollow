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

import com.netflix.hollow.core.schema.HollowSchema.SchemaType;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

public class HollowSchemaTest {

    @Test
    public void isNullableObjectEquals() {
        assertTrue(HollowSchema.isNullableObjectEquals(null, null));
        assertTrue(HollowSchema.isNullableObjectEquals("S1", "S1"));
        assertTrue(HollowSchema.isNullableObjectEquals(1, 1));

        assertFalse(HollowSchema.isNullableObjectEquals(null, 1));
        assertFalse(HollowSchema.isNullableObjectEquals(null, "S1"));
        assertFalse(HollowSchema.isNullableObjectEquals("S1", null));
        assertFalse(HollowSchema.isNullableObjectEquals("S1", ""));
        assertFalse(HollowSchema.isNullableObjectEquals("S1", "S2"));
        assertFalse(HollowSchema.isNullableObjectEquals("S1", 1));
    }

    @Test
    public void fromTypeId() {
        assertEquals(SchemaType.OBJECT, SchemaType.fromTypeId(0));
        assertEquals(SchemaType.OBJECT, SchemaType.fromTypeId(6));

        assertNotEquals(SchemaType.OBJECT, SchemaType.fromTypeId(1));
        assertEquals(SchemaType.LIST, SchemaType.fromTypeId(2));

        assertEquals(SchemaType.SET, SchemaType.fromTypeId(1));
        assertEquals(SchemaType.SET, SchemaType.fromTypeId(4));

        assertEquals(SchemaType.MAP, SchemaType.fromTypeId(3));
        assertEquals(SchemaType.MAP, SchemaType.fromTypeId(5));
    }

    @Test
    public void hasKey() {
        assertTrue(SchemaType.hasKey(4));
        assertTrue(SchemaType.hasKey(5));
        assertTrue(SchemaType.hasKey(6));
    }
}