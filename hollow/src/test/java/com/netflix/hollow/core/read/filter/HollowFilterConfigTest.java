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
package com.netflix.hollow.core.read.filter;

import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

public class HollowFilterConfigTest {

    @Test
    public void includeFilterSpecifiesTypesAndFields() {
        HollowFilterConfig conf = new HollowFilterConfig();

        conf.addType("TypeA");
        conf.addField("TypeB", "b1");
        conf.addField("TypeB", "b2");

        assertTrue(conf.doesIncludeType("TypeA"));
        assertTrue(conf.getObjectTypeConfig("TypeA").includesField("anyGivenField"));
        assertTrue(conf.doesIncludeType("TypeB"));
        assertTrue(conf.getObjectTypeConfig("TypeB").includesField("b1"));
        assertFalse(conf.getObjectTypeConfig("TypeB").includesField("b3"));
        assertFalse(conf.doesIncludeType("TypeC"));
        assertFalse(conf.getObjectTypeConfig("TypeC").includesField("asdf"));
    }

    @Test
    public void excludeFilterSpecifiesTypesAndFields() {
        HollowFilterConfig conf = new HollowFilterConfig(true);

        conf.addType("TypeA");
        conf.addField("TypeB", "b1");
        conf.addField("TypeB", "b2");

        assertFalse(conf.doesIncludeType("TypeA"));
        assertFalse(conf.getObjectTypeConfig("TypeA").includesField("anyGivenField"));
        assertTrue(conf.doesIncludeType("TypeB"));
        assertFalse(conf.getObjectTypeConfig("TypeB").includesField("b1"));
        assertTrue(conf.getObjectTypeConfig("TypeB").includesField("b3"));
        assertTrue(conf.doesIncludeType("TypeC"));
        assertTrue(conf.getObjectTypeConfig("TypeC").includesField("anyGivenField"));
    }

    @Test
    public void serializesAndDeserializes() {
        HollowFilterConfig conf = new HollowFilterConfig(true);

        conf.addType("TypeA");
        conf.addField("TypeB", "b1");
        conf.addField("TypeB", "b2");

        String configStr = conf.toString();
        conf = HollowFilterConfig.fromString(configStr);

        System.out.println(configStr);

        assertFalse(conf.doesIncludeType("TypeA"));
        assertFalse(conf.getObjectTypeConfig("TypeA").includesField("anyGivenField"));
        assertTrue(conf.doesIncludeType("TypeB"));
        assertFalse(conf.getObjectTypeConfig("TypeB").includesField("b1"));
        assertTrue(conf.getObjectTypeConfig("TypeB").includesField("b3"));
        assertTrue(conf.doesIncludeType("TypeC"));
        assertTrue(conf.getObjectTypeConfig("TypeC").includesField("anyGivenField"));
    }
}
