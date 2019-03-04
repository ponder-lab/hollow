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
import com.netflix.hollow.core.schema.HollowObjectSchema.FieldType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

public class HollowSchemaParserTest {

    @Test
    public void parsesObjectSchema() throws IOException {
        String objectSchema =
                "/* This is a comment\n" +
                        "   consisting of multiple lines */\n" +
                        " TypeA {\n" +
                        "    int a1;\n" +
                        "    \tstring a2; //This is a comment\n" +
                        "    String a3;\n" +
                        "}\n";

        HollowObjectSchema schema = (HollowObjectSchema) HollowSchemaParser.parseSchema(objectSchema);

        assertEquals("TypeA", schema.getName());
        assertEquals(3, schema.numFields());
        assertEquals(FieldType.INT, schema.getFieldType(0));
        assertEquals("a1", schema.getFieldName(0));
        assertEquals(FieldType.STRING, schema.getFieldType(1));
        assertEquals("a2", schema.getFieldName(1));
        assertEquals(FieldType.REFERENCE, schema.getFieldType(2));
        assertEquals("String", schema.getReferencedType(2));
        assertEquals("a3", schema.getFieldName(2));

        // HollowObjectSchame.toString is parsed properly
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }

    @Test
    public void parsesObjectSchemaWithKey() throws IOException {
        String objectSchema = " TypeA @PrimaryKey(a1) {\n" +
                "    int a1;\n" +
                "    string a2;\n" +
                "    String a3;\n" +
                "}\n";

        HollowObjectSchema schema = (HollowObjectSchema) HollowSchemaParser.parseSchema(objectSchema);

        assertEquals("TypeA", schema.getName());
        assertEquals(3, schema.numFields());
        assertEquals(FieldType.INT, schema.getFieldType(0));
        assertEquals("a1", schema.getFieldName(0));
        assertEquals(FieldType.STRING, schema.getFieldType(1));
        assertEquals("a2", schema.getFieldName(1));
        assertEquals(FieldType.REFERENCE, schema.getFieldType(2));
        assertEquals("String", schema.getReferencedType(2));
        assertEquals("a3", schema.getFieldName(2));

        // Make sure primary key and HollowObjectSchame.toString is parsed properly
        assertEquals(new PrimaryKey("TypeA", "a1"), schema.getPrimaryKey());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }

    @Test
    public void parsesObjectSchemaMultipleWithKey() throws IOException {
        String objectSchema = " TypeA @PrimaryKey(a1, a3.value) {\n" +
                "    int a1;\n" +
                "    string a2;\n" +
                "    String a3;\n" +
                "}\n";

        HollowObjectSchema schema = (HollowObjectSchema) HollowSchemaParser.parseSchema(objectSchema);

        assertEquals("TypeA", schema.getName());
        assertEquals(3, schema.numFields());
        assertEquals(FieldType.INT, schema.getFieldType(0));
        assertEquals("a1", schema.getFieldName(0));
        assertEquals(FieldType.STRING, schema.getFieldType(1));
        assertEquals("a2", schema.getFieldName(1));
        assertEquals(FieldType.REFERENCE, schema.getFieldType(2));
        assertEquals("String", schema.getReferencedType(2));
        assertEquals("a3", schema.getFieldName(2));

        // Make sure primary key and HollowObjectSchame.toString is parsed properly
        assertEquals(new PrimaryKey("TypeA", "a1", "a3.value"), schema.getPrimaryKey());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }

    @Test
    public void parsesListSchema() throws IOException {
        String listSchema = "ListOfTypeA List<TypeA>;\n";

        HollowListSchema schema = (HollowListSchema) HollowSchemaParser.parseSchema(listSchema);

        assertEquals("ListOfTypeA", schema.getName());
        assertEquals("TypeA", schema.getElementType());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }


    @Test
    public void parsesSetSchema() throws IOException {
        String listSchema = "SetOfTypeA Set<TypeA>;\n";

        HollowSetSchema schema = (HollowSetSchema) HollowSchemaParser.parseSchema(listSchema);

        assertEquals("SetOfTypeA", schema.getName());
        assertEquals("TypeA", schema.getElementType());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }

    @Test
    public void parsesSetSchemaWithKey() throws IOException {
        String listSchema = "SetOfTypeA Set<TypeA> @HashKey(id.value);\n";

        HollowSetSchema schema = (HollowSetSchema) HollowSchemaParser.parseSchema(listSchema);

        assertEquals("SetOfTypeA", schema.getName());
        assertEquals("TypeA", schema.getElementType());
        assertEquals(new PrimaryKey("TypeA", "id.value"), schema.getHashKey());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }

    @Test
    public void parsesSetSchemaWithMultiFieldKey() throws IOException {
        String listSchema = "SetOfTypeA Set<TypeA> @HashKey(id.value, region.country.id, key);\n";

        HollowSetSchema schema = (HollowSetSchema) HollowSchemaParser.parseSchema(listSchema);

        assertEquals("SetOfTypeA", schema.getName());
        assertEquals("TypeA", schema.getElementType());
        assertEquals(new PrimaryKey("TypeA", "id.value", "region.country.id", "key"), schema.getHashKey());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }


    @Test
    public void parsesMapSchema() throws IOException {
        String listSchema = "MapOfStringToTypeA Map<String, TypeA>;\n";

        HollowMapSchema schema = (HollowMapSchema) HollowSchemaParser.parseSchema(listSchema);

        assertEquals("MapOfStringToTypeA", schema.getName());
        assertEquals("String", schema.getKeyType());
        assertEquals("TypeA", schema.getValueType());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }


    @Test
    public void parsesMapSchemaWithPrimaryKey() throws IOException {
        String listSchema = "MapOfStringToTypeA Map<String, TypeA> @HashKey(value);\n";

        HollowMapSchema schema = (HollowMapSchema) HollowSchemaParser.parseSchema(listSchema);

        assertEquals("MapOfStringToTypeA", schema.getName());
        assertEquals("String", schema.getKeyType());
        assertEquals("TypeA", schema.getValueType());
        assertEquals(new PrimaryKey("String", "value"), schema.getHashKey());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }


    @Test
    public void parsesMapSchemaWithMultiFieldPrimaryKey() throws IOException {
        String listSchema = "MapOfStringToTypeA Map<String, TypeA> @HashKey(id.value, region.country.id, key);\n";

        HollowMapSchema schema = (HollowMapSchema) HollowSchemaParser.parseSchema(listSchema);

        assertEquals("MapOfStringToTypeA", schema.getName());
        assertEquals("String", schema.getKeyType());
        assertEquals("TypeA", schema.getValueType());
        assertEquals(new PrimaryKey("String", "id.value", "region.country.id", "key"), schema.getHashKey());
        assertEquals(schema, HollowSchemaParser.parseSchema(schema.toString()));
    }

    @Test
    public void parsesManySchemas() throws IOException {
        String manySchemas =
                "/* This is a comment\n" +
                        "   consisting of multiple lines */\n" +
                        " TypeA {\n" +
                        "    int a1;\n" +
                        "    \tstring a2; //This is a comment\n" +
                        "    String a3;\n" +
                        "}\n\n"+
                        "MapOfStringToTypeA Map<String, TypeA>;\n"+
                        "ListOfTypeA List<TypeA>;\n"+
                        "TypeB { float b1; double b2; boolean b3; }";


        List<HollowSchema> schemas = HollowSchemaParser.parseCollectionOfSchemas(manySchemas);

        assertEquals(4, schemas.size());
    }

    @Test
    public void testParseCollectionOfSchemas_reader() throws Exception {
        InputStream input = null;
        try {
            input = getClass().getResourceAsStream("/schema1.txt");
            List<HollowSchema> schemas =
                    HollowSchemaParser.parseCollectionOfSchemas(new BufferedReader(new InputStreamReader(input)));
            assertEquals("Should have two schemas", 2, schemas.size());
            assertEquals("Should have Minion schema", "Minion", schemas.get(0).getName());
            assertEquals("Should have String schema", "String", schemas.get(1).getName());
        } finally {
            if (input != null) {
                input.close();
            }
        }
    }
}
