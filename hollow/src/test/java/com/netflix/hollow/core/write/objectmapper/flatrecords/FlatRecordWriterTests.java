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
package com.netflix.hollow.core.write.objectmapper.flatrecords;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.InMemoryBlobStore;
import com.netflix.hollow.api.objects.generic.GenericHollowObject;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowInMemoryBlobStager;
import com.netflix.hollow.core.index.key.PrimaryKey;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowObjectSchema.FieldType;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import com.netflix.hollow.core.write.objectmapper.HollowHashKey;
import com.netflix.hollow.core.write.objectmapper.HollowInline;
import com.netflix.hollow.core.write.objectmapper.HollowObjectMapper;
import com.netflix.hollow.core.write.objectmapper.HollowPrimaryKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FlatRecordWriterTests {

    
    private HollowObjectMapper mapper;
    private HollowSchemaIdentifierMapper schemaIdMapper;

    private FlatRecordWriter flatRecordWriter;

    private HollowProducer producer;
    private InMemoryBlobStore blobStore;

    @BeforeEach
    public void setUp() {
        mapper = new HollowObjectMapper(new HollowWriteStateEngine());
        mapper.initializeTypeState(TypeA.class);
        mapper.initializeTypeState(TypeC.class);
        schemaIdMapper = new FakeHollowSchemaIdentifierMapper(mapper.getStateEngine());
        blobStore = new InMemoryBlobStore();

        flatRecordWriter = new FlatRecordWriter(mapper.getStateEngine(), schemaIdMapper);

        producer = HollowProducer.withPublisher(blobStore).withBlobStager(new HollowInMemoryBlobStager()).build();
    }


    @Test
    public void flatRecordsDedupRedundantDataWithinRecords() {
        int recSize1 = flatRecordSize(new TypeA(1, "two", "three", "four"));
        int recSize2 = flatRecordSize(new TypeA(1, "two", "three", "four", "three"));
        
        assertTrue(recSize2 - recSize1 == 1);
    }
    
    @Test
    public void flatRecordsCanBeDumpedToStateEngineWithIdenticalSchemas() throws IOException {
        producer.initializeDataModel(TypeA.class, TypeC.class);

        producer.runCycle(state -> {
            FlatRecordDumper dumper = new FlatRecordDumper(state.getStateEngine());
            dumper.dump(flatten(new TypeA(1, "two", "three", "four", "five")));
            dumper.dump(flatten(new TypeA(2, "two", "four", "six", "eight", "é with acute accent", "ten")));
            dumper.dump(flatten(new TypeC("one", "four")));
        });

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore).build();
        consumer.triggerRefresh();
        
        GenericHollowObject typeA0 = new GenericHollowObject(consumer.getStateEngine(), "TypeA", 0);
        GenericHollowObject typeA1 = new GenericHollowObject(consumer.getStateEngine(), "TypeA", 1);
        
        assertEquals(1, typeA0.getInt("a1"));
        assertEquals("two", typeA0.getString("a2"));
        assertEquals("three", typeA0.getObject("a3").getString("b1"));
        assertEquals(2, typeA0.getSet("a4").size());
        assertTrue(typeA0.getSet("a4").findElement("four") != null);
        assertTrue(typeA0.getSet("a4").findElement("five") != null);

        assertEquals(2, typeA1.getInt("a1"));
        assertEquals("two", typeA1.getString("a2"));
        assertEquals("four", typeA1.getObject("a3").getString("b1"));
        assertEquals(4, typeA1.getSet("a4").size());
        assertTrue(typeA1.getSet("a4").findElement("six") != null);
        assertTrue(typeA1.getSet("a4").findElement("eight") != null);
        assertTrue(typeA1.getSet("a4").findElement("é with acute accent") != null);
        assertTrue(typeA1.getSet("a4").findElement("ten") != null);

        GenericHollowObject typeC0 = new GenericHollowObject(consumer.getStateEngine(), "TypeC", 0);

        assertEquals("one", typeC0.getString("c1"));
        assertEquals("four", typeC0.getObject("c2").getString("b1"));
        
        assertEquals(typeC0.getObject("c2").getOrdinal(), typeA0.getSet("a4").findElement("four").getOrdinal());
    }
    
    @Test
    public void flatRecordsCanBeDumpedToStateEnginesWithDifferentButCompatibleSchemas() {
        HollowObjectSchema typeASchema = new HollowObjectSchema("TypeA", 4, new PrimaryKey("TypeA", "a1", "a3.b1"));
        typeASchema.addField("a1", FieldType.INT);
        typeASchema.addField("a2", FieldType.STRING);
        typeASchema.addField("a3", FieldType.REFERENCE, "TypeB");
        typeASchema.addField("a5", FieldType.BYTES);
        
        HollowObjectSchema typeBSchema = new HollowObjectSchema("TypeB", 2);
        typeBSchema.addField("b1", FieldType.STRING);
        typeBSchema.addField("b2", FieldType.INT);

        HollowObjectSchema typeCSchema = new HollowObjectSchema("TypeC", 2);
        typeCSchema.addField("c2", FieldType.REFERENCE, "TypeB");
        typeCSchema.addField("c3", FieldType.FLOAT);
        
        producer.initializeDataModel(typeASchema, typeBSchema, typeCSchema);
        
        producer.runCycle(state -> {
            FlatRecordDumper dumper = new FlatRecordDumper(state.getStateEngine());
            dumper.dump(flatten(new TypeA(1, "two", "three", "four", "five")));
            dumper.dump(flatten(new TypeA(2, "two", "four", "six", "eight", "ten")));
            dumper.dump(flatten(new TypeC("one", "three")));
        });

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore).build();
        consumer.triggerRefresh();

        GenericHollowObject typeA0 = new GenericHollowObject(consumer.getStateEngine(), "TypeA", 0);
        GenericHollowObject typeA1 = new GenericHollowObject(consumer.getStateEngine(), "TypeA", 1);
        
        assertEquals(1, typeA0.getInt("a1"));
        assertEquals("two", typeA0.getString("a2"));
        assertEquals("three", typeA0.getObject("a3").getString("b1"));
        assertNull(typeA0.getSet("a4"));
        assertNull(typeA0.getBytes("a5"));

        assertEquals(2, typeA1.getInt("a1"));
        assertEquals("two", typeA1.getString("a2"));
        assertEquals("four", typeA1.getObject("a3").getString("b1"));
        assertNull(typeA1.getSet("a4"));
        assertNull(typeA1.getBytes("a4"));

        GenericHollowObject typeC0 = new GenericHollowObject(consumer.getStateEngine(), "TypeC", 0);

        assertTrue(typeC0.isNull("c1"));
        assertEquals("three", typeC0.getObject("c2").getString("b1"));
        assertTrue(typeC0.isNull("c3"));
        
        assertEquals(typeC0.getObject("c2").getOrdinal(), typeA0.getObject("a3").getOrdinal());
    }
    
    private FlatRecord flatten(Object obj) {
        flatRecordWriter.reset();
        mapper.writeFlat(obj, flatRecordWriter);
        return flatRecordWriter.generateFlatRecord();
    }
    
    private int flatRecordSize(Object obj) {
        flatRecordWriter.reset();
        mapper.writeFlat(obj, flatRecordWriter);
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            flatRecordWriter.writeTo(baos);
            return baos.size();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @HollowPrimaryKey(fields = { "a1", "a3" })
    public static class TypeA {
        int a1;
        @HollowInline
        String a2;
        TypeB a3;
        @HollowHashKey(fields="b1")
        Set<TypeB> a4;
        @HollowInline
        Integer nullablePrimitiveInt;

        public TypeA(int a1, String a2, String a3, String... b1s) {
            this.a1 = a1;
            this.a2 = a2;
            this.a3 = new TypeB(a3);
            this.a4 = new HashSet<>(b1s.length);
            for (int i = 0; i < b1s.length; i++) {
                this.a4.add(new TypeB(b1s[i]));
            }
            this.nullablePrimitiveInt = null;
        }
    }

    public static class TypeB {
        @HollowInline
        String b1;

        public TypeB(String b1) {
            this.b1 = b1;
        }
    }

    public static class TypeC {
        @HollowInline
        String c1;
        TypeB c2;

        public TypeC(String c1, String b1) {
            this.c1 = c1;
            this.c2 = new TypeB(b1);
        }
    }

}
