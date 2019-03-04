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
package com.netflix.hollow.core.read.object;

import com.netflix.hollow.api.objects.generic.GenericHollowObject;
import com.netflix.hollow.core.AbstractStateEngineTest;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowObjectSchema.FieldType;
import com.netflix.hollow.core.write.HollowObjectTypeWriteState;
import com.netflix.hollow.core.write.HollowObjectWriteRecord;
import java.io.IOException;
import java.util.BitSet;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HollowObjectShardedTest extends AbstractStateEngineTest {

    HollowObjectSchema schema;

    @BeforeEach
    public void setUp() {
        schema = new HollowObjectSchema("TestObject", 3);
        schema.addField("longField", FieldType.LONG);
        schema.addField("intField", FieldType.INT);
        schema.addField("doubleField", FieldType.DOUBLE);

        super.setUp();
    }
    
    @Test
    public void testShardedData() throws IOException {
    
        HollowObjectWriteRecord rec = new HollowObjectWriteRecord(schema);
        
        for(int i=0;i<1000;i++) {
            rec.reset();
            rec.setLong("longField", i);
            rec.setInt("intField", i);
            rec.setDouble("doubleField", i);
            
            writeStateEngine.add("TestObject", rec);
        }
        
        roundTripSnapshot();
        
        assertEquals(4, readStateEngine.getTypeState("TestObject").numShards());
        
        for(int i=0;i<1000;i++) {
            GenericHollowObject obj = new GenericHollowObject(readStateEngine, "TestObject", i); 
            
            assertEquals(i, obj.getLong("longField"));
            assertEquals(i, obj.getInt("intField"));
            assertEquals((double)i, obj.getDouble("doubleField"), 0);
        }

        for(int i=0;i<1000;i++) {
            rec.reset();
            rec.setLong("longField", i*2);
            rec.setInt("intField", i*2);
            rec.setDouble("doubleField", i*2);
            
            writeStateEngine.add("TestObject", rec);
        }
        
        roundTripDelta();
        
        int expectedValue = 0;
        
        BitSet populatedOrdinals = readStateEngine.getTypeState("TestObject").getPopulatedOrdinals();
        
        int ordinal = populatedOrdinals.nextSetBit(0);
        while(ordinal != -1) {
            GenericHollowObject obj = new GenericHollowObject(readStateEngine, "TestObject", ordinal);
            
            assertEquals(expectedValue, obj.getLong("longField"));
            assertEquals(expectedValue, obj.getInt("intField"));
            assertEquals(expectedValue, obj.getDouble("doubleField"), 0);
            
            expectedValue += 2;
            ordinal = populatedOrdinals.nextSetBit(ordinal+1);
        }
    }
    
    @Override
    protected void initializeTypeStates() {
        writeStateEngine.setTargetMaxTypeShardSize(4096);
        writeStateEngine.addTypeState(new HollowObjectTypeWriteState(schema));
    }


}
