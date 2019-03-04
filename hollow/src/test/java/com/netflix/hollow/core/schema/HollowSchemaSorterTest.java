package com.netflix.hollow.core.schema;

import com.netflix.hollow.core.util.HollowWriteStateCreator;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import java.io.IOException;
import java.util.List;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.Test;

public class HollowSchemaSorterTest {
    
    @Test
    public void schemasAreSortedBasedOnDependencies() throws IOException {
        String schemasText = "TypeB {"
                           + "    ListOfString str;"
                           + "}"
                           + ""
                           + "String {"
                           + "    string value;"
                           + "}"
                           + ""
                           + "ListOfString List<String>;"
                           + ""
                           + "TypeA {"
                           + "    TypeB b;"
                           + "    String str;"
                           + "}";
        
        List<HollowSchema> schemas = HollowSchemaParser.parseCollectionOfSchemas(schemasText);
        
        List<HollowSchema> sortedSchemas = HollowSchemaSorter.dependencyOrderedSchemaList(schemas);
        
        assertEquals(4, sortedSchemas.size());
        assertEquals("String", sortedSchemas.get(0).getName());
        assertEquals("ListOfString", sortedSchemas.get(1).getName());
        assertEquals("TypeB", sortedSchemas.get(2).getName());
        assertEquals("TypeA", sortedSchemas.get(3).getName());
    }
    
    @Test
    public void sortsSchemasEvenIfDependencyTypesNotPresent() throws IOException {
        String schemasText = "TypeA { TypeB b; }"
                           + "TypeB { TypeC c; }";
        
        
        List<HollowSchema> schemas = HollowSchemaParser.parseCollectionOfSchemas(schemasText);
        
        List<HollowSchema> sortedSchemas = HollowSchemaSorter.dependencyOrderedSchemaList(schemas);

        assertEquals(2, sortedSchemas.size());
        assertEquals("TypeB", sortedSchemas.get(0).getName());
        assertEquals("TypeA", sortedSchemas.get(1).getName());
    }
    
    @Test
    public void determinesIfSchemasAreTransitivelyDependent() throws IOException {
        String schemasText = "TypeA { TypeB b; }"
                           + "TypeB { TypeC c; }"
                           + "TypeC { TypeD d; }";
        
        List<HollowSchema> schemas = HollowSchemaParser.parseCollectionOfSchemas(schemasText);
        
        HollowWriteStateEngine stateEngine = new HollowWriteStateEngine();
        HollowWriteStateCreator.populateStateEngineWithTypeWriteStates(stateEngine, schemas);
        
        assertTrue(HollowSchemaSorter.typeIsTransitivelyDependent(stateEngine, "TypeA", "TypeB"));
        assertTrue(HollowSchemaSorter.typeIsTransitivelyDependent(stateEngine, "TypeA", "TypeC"));
        assertTrue(HollowSchemaSorter.typeIsTransitivelyDependent(stateEngine, "TypeB", "TypeC"));
        assertFalse(HollowSchemaSorter.typeIsTransitivelyDependent(stateEngine, "TypeC", "TypeB"));
        assertFalse(HollowSchemaSorter.typeIsTransitivelyDependent(stateEngine, "TypeB", "TypeA"));
        assertFalse(HollowSchemaSorter.typeIsTransitivelyDependent(stateEngine, "TypeC", "TypeA"));
    }

}
