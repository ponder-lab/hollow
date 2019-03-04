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
 */
package com.netflix.hollow.core.index;

import com.netflix.hollow.core.index.key.PrimaryKey;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowObjectSchema.FieldType;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import com.netflix.hollow.core.write.objectmapper.HollowObjectMapper;
import com.netflix.hollow.core.write.objectmapper.HollowPrimaryKey;
import java.util.List;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unused")
public class PrimaryKeyTest {
    
    HollowWriteStateEngine writeEngine;
    
    @BeforeEach
    public void setUp() {
        writeEngine = new HollowWriteStateEngine();
        HollowObjectMapper mapper = new HollowObjectMapper(writeEngine);

        mapper.initializeTypeState(TypeWithTraversablePrimaryKey.class);
    }

    @Test
    public void automaticallyTraversesSomeIncompletelyDefinedFieldPaths() {
        HollowObjectSchema schema = (HollowObjectSchema) writeEngine.getTypeState("TypeWithTraversablePrimaryKey").getSchema();
        PrimaryKey traversablePrimaryKey = schema.getPrimaryKey();
        
        assertEquals(2, traversablePrimaryKey.getFieldPathIndex(writeEngine, 0).length);
        assertEquals(3, traversablePrimaryKey.getFieldPathIndex(writeEngine, 1).length);
        assertEquals(1, traversablePrimaryKey.getFieldPathIndex(writeEngine, 2).length);
        
        PrimaryKey anotherTraversablePrimaryKey = new PrimaryKey("TypeWithTraversablePrimaryKey", "subType.id");
        assertEquals(3, anotherTraversablePrimaryKey.getFieldPathIndex(writeEngine, 0).length);
        
        PrimaryKey hardStopPrimaryKey = new PrimaryKey("TypeWithTraversablePrimaryKey", "subType.id!");
        assertEquals(2, hardStopPrimaryKey.getFieldPathIndex(writeEngine, 0).length);
        
        PrimaryKey hardStopPrimaryKey2 = new PrimaryKey("TypeWithTraversablePrimaryKey", "subType2!");
        assertEquals(1, hardStopPrimaryKey2.getFieldPathIndex(writeEngine, 0).length);
        
        PrimaryKey hardStopPrimaryKey3 = new PrimaryKey("TypeWithTraversablePrimaryKey", "strList!");
        assertEquals(1, hardStopPrimaryKey3.getFieldPathIndex(writeEngine, 0).length);
    }
    
    @Test
    public void throwsMeaningfulExceptions() {
        try {
            PrimaryKey invalidFieldDefinition = new PrimaryKey("TypeWithTraversablePrimaryKey", "subType.nofield");
            invalidFieldDefinition.getFieldPathIndex(writeEngine, 0);
            fail("IllegalArgumentException expected");
        } catch (FieldPaths.FieldPathException expected) {
            assertEquals(FieldPaths.FieldPathException.ErrorKind.NOT_FOUND, expected.error);
            assertEquals(1, expected.fieldSegments.size());
            assertEquals(1, expected.segmentIndex);
            assertEquals("SubTypeWithTraversablePrimaryKey", expected.enclosingSchema.getName());
        }
        
        try {
            PrimaryKey invalidFieldDefinition = new PrimaryKey("TypeWithTraversablePrimaryKey", "subType.id.value.alldone");
            invalidFieldDefinition.getFieldPathIndex(writeEngine, 0);
            fail("IllegalArgumentException expected");
        } catch (FieldPaths.FieldPathException expected) {
            assertEquals(FieldPaths.FieldPathException.ErrorKind.NOT_TRAVERSABLE, expected.error);
            assertEquals(3, expected.fieldSegments.size());
            assertEquals(2, expected.segmentIndex);
            assertEquals("value", expected.fieldSegments.get(2).getName());
            assertEquals("String", expected.enclosingSchema.getName());
        }
        
        try {
            PrimaryKey invalidFieldDefinition = new PrimaryKey("TypeWithTraversablePrimaryKey", "subType2");
            invalidFieldDefinition.getFieldPathIndex(writeEngine, 0);
            fail("IllegalArgumentException expected");
        } catch (FieldPaths.FieldPathException expected) {
            assertEquals(FieldPaths.FieldPathException.ErrorKind.NOT_EXPANDABLE, expected.error);
            assertEquals(1, expected.fieldSegments.size());
            assertEquals("subType2", expected.fieldSegments.get(0).getName());
            assertEquals("SubTypeWithNonTraversablePrimaryKey", expected.enclosingSchema.getName());
        }
        
        try {
            PrimaryKey invalidFieldDefinition = new PrimaryKey("TypeWithTraversablePrimaryKey", "strList.element.value");
            invalidFieldDefinition.getFieldPathIndex(writeEngine, 0);
            fail("IllegalArgumentException expected");
        } catch (FieldPaths.FieldPathException expected) {
            assertEquals(FieldPaths.FieldPathException.ErrorKind.NOT_TRAVERSABLE, expected.error);
            assertEquals(1, expected.fieldSegments.size());
            assertEquals(1, expected.segmentIndex);
            assertEquals("element", expected.segments[expected.segmentIndex]);
            assertEquals("ListOfString", expected.enclosingSchema.getName());
        }
        
        try {
            PrimaryKey invalidFieldDefinition = new PrimaryKey("UnknownType", "id");
            invalidFieldDefinition.getFieldPathIndex(writeEngine, 0);
            fail("IllegalArgumentException expected");
        } catch (FieldPaths.FieldPathException expected) {
            assertEquals(FieldPaths.FieldPathException.ErrorKind.NOT_BINDABLE, expected.error);
            assertEquals(0, expected.fieldSegments.size());
            assertEquals(0, expected.segmentIndex);
            assertEquals("UnknownType", expected.rootType);
        }
    }
    
    
    @Test
    public void testAutoExpand() {
        { // verify fieldPath auto expand
            PrimaryKey autoExpandPK = new PrimaryKey("TypeWithTraversablePrimaryKey", "subType");
            assertEquals(FieldType.STRING, autoExpandPK.getFieldType(writeEngine, 0));
            assertEquals(null, autoExpandPK.getFieldSchema(writeEngine, 0));
        }

        { // verify disabled fieldPath auto expand with ending "!" 
            PrimaryKey autoExpandPK = new PrimaryKey("TypeWithTraversablePrimaryKey", "subType!");
            assertNotEquals(FieldType.STRING, autoExpandPK.getFieldType(writeEngine, 0));
            assertEquals(FieldType.REFERENCE, autoExpandPK.getFieldType(writeEngine, 0));
            assertEquals("SubTypeWithTraversablePrimaryKey", autoExpandPK.getFieldSchema(writeEngine, 0).getName());
        }
    }
    
    
    @HollowPrimaryKey(fields={"pk1", "subType", "intId"})
    private static class TypeWithTraversablePrimaryKey {
        String pk1;
        SubTypeWithTraversablePrimaryKey subType;
        SubTypeWithNonTraversablePrimaryKey subType2;
        int intId;
        List<String> strList;
    }
    
    @HollowPrimaryKey(fields="id")
    private static class SubTypeWithTraversablePrimaryKey {
        String id;
        int anotherField;
    }
    
    @HollowPrimaryKey(fields={"id1", "id2"})
    private static class SubTypeWithNonTraversablePrimaryKey {
        long id1;
        float id2;
    }

}
