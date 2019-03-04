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
package com.netflix.hollow.tools.combine;

import com.netflix.hollow.api.objects.HollowObject;
import com.netflix.hollow.api.objects.HollowRecord;
import com.netflix.hollow.api.objects.generic.GenericHollowSet;
import com.netflix.hollow.core.HollowDataset;
import com.netflix.hollow.core.index.key.PrimaryKey;
import com.netflix.hollow.core.memory.pool.RecyclingRecycler;
import com.netflix.hollow.core.read.engine.HollowBlobReader;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.engine.set.HollowSetTypeReadState;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowObjectSchema.FieldType;
import com.netflix.hollow.core.schema.HollowSchema;
import com.netflix.hollow.core.schema.HollowSchema.SchemaType;
import com.netflix.hollow.core.schema.HollowSetSchema;
import com.netflix.hollow.core.util.DefaultHashCodeFinder;
import com.netflix.hollow.core.util.HollowObjectHashCodeFinder;
import com.netflix.hollow.core.write.HollowBlobWriter;
import com.netflix.hollow.core.write.HollowHashableWriteRecord.HashBehavior;
import com.netflix.hollow.core.write.HollowObjectTypeWriteState;
import com.netflix.hollow.core.write.HollowObjectWriteRecord;
import com.netflix.hollow.core.write.HollowSetTypeWriteState;
import com.netflix.hollow.core.write.HollowSetWriteRecord;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HollowCombinerTest {

    HollowObjectSchema aSchema;
    HollowSetSchema bSchema;
    HollowObjectSchema cSchema;

    HollowObjectHashCodeFinder hashCodeFinder;

    HollowWriteStateEngine shard1;
    HollowWriteStateEngine shard2;
    HollowWriteStateEngine shard3;

    @BeforeEach
    public void setUp() {
        aSchema = new HollowObjectSchema("A", 2, new PrimaryKey("A", "a1"));
        aSchema.addField("a1", FieldType.INT);
        aSchema.addField("a2", FieldType.REFERENCE, "B");

        bSchema = new HollowSetSchema("B", "C");

        cSchema = new HollowObjectSchema("C", 1);
        cSchema.addField("c1", FieldType.STRING);

        hashCodeFinder = new DefaultHashCodeFinder("C");

        shard1 = createStateEngine();
        shard2 = createStateEngine();
        shard3 = createStateEngine();
    }


    @Test
    public void testCombiner() throws IOException {
        addRecord(shard1, 1, "C1", "C2", "C3");
        addRecord(shard1, 2, "C2", "C3", "C4");
        addRecord(shard1, 3, "C1", "C2", "C3");

        addRecord(shard2, 4, "C2", "C3", "C4");
        addRecord(shard2, 5, "C1", "C4", "C5");
        addRecord(shard2, 6, "C1", "C2", "C3");

        addRecord(shard3, 7, "C4", "C5", "C6");

        HollowCombiner combiner = new HollowCombiner(roundTrip(shard1), roundTrip(shard2), roundTrip(shard3));
        combiner.combine();

        HollowReadStateEngine combinedResult = roundTrip(combiner.getCombinedStateEngine());

        assertEquals(6, combinedResult.getTypeState("A").maxOrdinal());
        assertEquals(3, combinedResult.getTypeState("B").maxOrdinal());
        assertEquals(5, combinedResult.getTypeState("C").maxOrdinal());

        HollowSetTypeReadState bTypeState = (HollowSetTypeReadState)combinedResult.getTypeState("B");

        assertTrue(setOrderingExists(bTypeState, "C1", "C2", "C3"));
        assertTrue(setOrderingExists(bTypeState, "C2", "C3", "C4"));
        assertTrue(setOrderingExists(bTypeState, "C1", "C4", "C5"));
        assertTrue(setOrderingExists(bTypeState, "C4", "C5", "C6"));
    }

    @Test
    public void testCombinerPrimaryKey() throws IOException {
        HollowWriteStateEngine sEngine = createStateEngine();
        List<PrimaryKey> pKeys = extractPrimaryKeys(sEngine);
        assertEquals(1, pKeys.size());

        // Make sure combinedResult has PrimaryKey transferred
        {
            HollowCombiner combiner = new HollowCombiner(roundTrip(sEngine), roundTrip(shard1));
            HollowReadStateEngine combinedResult = roundTrip(combiner.getCombinedStateEngine());
            assertEquals(1, extractPrimaryKeys(combinedResult).size());
            assertEquals(pKeys, extractPrimaryKeys(combinedResult));
        }

        // validate deduping
        {
            PrimaryKey oldPK = pKeys.get(0);
            PrimaryKey newPK = new PrimaryKey(oldPK.getType(), oldPK.getFieldPaths());
            assertEquals(oldPK.getType(), newPK.getType());
            assertEquals(oldPK, newPK);

            HollowCombiner combiner = new HollowCombiner(roundTrip(sEngine), roundTrip(shard1));
            combiner.setPrimaryKeys(newPK);
            assertEquals(1, combiner.getPrimaryKeys().size());
            assertEquals(oldPK, combiner.getPrimaryKeys().get(0));
            assertEquals(newPK, combiner.getPrimaryKeys().get(0));
        }

        // validate new PK of same type replaces old one
        {
            PrimaryKey oldPK = pKeys.get(0);
            PrimaryKey newPK = new PrimaryKey(oldPK.getType(), "xyz");
            assertEquals(oldPK.getType(), newPK.getType());
            assertNotEquals(oldPK, newPK);

            HollowCombiner combiner = new HollowCombiner(roundTrip(sEngine), roundTrip(shard1));
            combiner.setPrimaryKeys(newPK);
            assertEquals(1, combiner.getPrimaryKeys().size());
            assertNotEquals(oldPK, combiner.getPrimaryKeys().get(0));
            assertEquals(newPK, combiner.getPrimaryKeys().get(0));
        }

        // validate new PK gets added
        {
            PrimaryKey oldPK = pKeys.get(0);
            PrimaryKey newPK = new PrimaryKey("C", "c1");
            assertNotEquals(oldPK.getType(), newPK.getType());
            assertNotEquals(oldPK, newPK);

            HollowCombiner combiner = new HollowCombiner(roundTrip(sEngine), roundTrip(shard1));
            combiner.setPrimaryKeys(newPK);
            assertEquals(2, combiner.getPrimaryKeys().size());
            assertTrue(combiner.getPrimaryKeys().contains(oldPK));
            assertTrue(combiner.getPrimaryKeys().contains(newPK));
        }
    }

    private boolean setOrderingExists(HollowSetTypeReadState bTypeState, String... orderedCValues) {
        for(int i=0;i<=bTypeState.maxOrdinal();i++) {
            GenericHollowSet set = new GenericHollowSet(bTypeState, i);

            Iterator<HollowRecord> iter = set.iterator();

            for(int j=0;j<orderedCValues.length;j++) {
                if(!iter.hasNext())
                    break;
                HollowObject obj = (HollowObject)iter.next();
                String cValue = obj.getString("c1");
                if(!cValue.equals(orderedCValues[j]))
                    break;

                if(j == (orderedCValues.length - 1))
                    return true;
            }
        }

        return false;
    }


    private int addRecord(HollowWriteStateEngine shardEngine, int aVal, String... cVals) {
        int cOrdinals[] = new int[cVals.length];

        HollowObjectWriteRecord cRec = new HollowObjectWriteRecord(cSchema);

        for(int i=0;i<cVals.length;i++) {
            cRec.reset();
            cRec.setString("c1", cVals[i]);
            cOrdinals[i] = shardEngine.add("C", cRec);
        }

        HollowSetWriteRecord bRec = new HollowSetWriteRecord(HashBehavior.UNMIXED_HASHES);

        for(int i=0;i<cOrdinals.length;i++) {
            bRec.addElement(cOrdinals[i], i);   /// hash code is ordering here
        }

        int bOrdinal = shardEngine.add("B", bRec);

        HollowObjectWriteRecord aRec = new HollowObjectWriteRecord(aSchema);
        aRec.setInt("a1", aVal);
        aRec.setReference("a2", bOrdinal);
        return shardEngine.add("A", aRec);
    }

    private HollowReadStateEngine roundTrip(HollowWriteStateEngine writeEngine) throws IOException {
        writeEngine.prepareForWrite();
        HollowBlobWriter writer = new HollowBlobWriter(writeEngine);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writer.writeSnapshot(baos);

        HollowReadStateEngine readEngine = new HollowReadStateEngine(writeEngine.getHashCodeFinder(), true, new RecyclingRecycler());
        HollowBlobReader reader = new HollowBlobReader(readEngine);
        reader.readSnapshot(new ByteArrayInputStream(baos.toByteArray()));

        return readEngine;
    }

    private HollowWriteStateEngine createStateEngine() {
        HollowWriteStateEngine stateEngine = new HollowWriteStateEngine(hashCodeFinder);
        stateEngine.addTypeState(new HollowObjectTypeWriteState(aSchema));
        stateEngine.addTypeState(new HollowSetTypeWriteState(bSchema));
        stateEngine.addTypeState(new HollowObjectTypeWriteState(cSchema));
        return stateEngine;
    }
    
    private List<PrimaryKey> extractPrimaryKeys(HollowDataset dataset) {
        List<PrimaryKey> keys = new ArrayList<PrimaryKey>();
        for (HollowSchema schema : dataset.getSchemas()) {
            if (schema.getSchemaType() == SchemaType.OBJECT) {
                PrimaryKey pk = ((HollowObjectSchema) schema).getPrimaryKey();
                if (pk != null)
                    keys.add(pk);
            }
        }
        
        return keys;
    }

}
