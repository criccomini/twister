package dev.twister.avro;

import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AvroSchemaInferrerTest extends TestCase {
    public void testSchemaInferrer() {
        Map<String, Object> testMap = new HashMap<>();
        testMap.put("stringField", "Hello, World!");
        testMap.put("intField", 42);
        testMap.put("booleanField", true);
        testMap.put("longField", 123456789L);
        testMap.put("floatField", 3.14f);
        testMap.put("doubleField", 2.718281828);
        testMap.put("nullField", null);

        Schema inferredSchema = new AvroSchemaInferrer().infer(testMap, "TestSchema");

        // Define expected schema with fields added in alphabetical order
        Schema expectedSchema = SchemaBuilder.record("TestSchema").fields()
                .name("booleanField").type(SchemaBuilder.unionOf().nullType().and().booleanType().endUnion()).noDefault()
                .name("doubleField").type(SchemaBuilder.unionOf().nullType().and().doubleType().endUnion()).noDefault()
                .name("floatField").type(SchemaBuilder.unionOf().nullType().and().floatType().endUnion()).noDefault()
                .name("intField").type(SchemaBuilder.unionOf().nullType().and().intType().endUnion()).noDefault()
                .name("longField").type(SchemaBuilder.unionOf().nullType().and().longType().endUnion()).noDefault()
                .name("nullField").type().nullType().noDefault()
                .name("stringField").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault()
                .endRecord();

        // Compare inferred schema with expected schema
        assertEquals(expectedSchema, inferredSchema);
    }

    public void testArray() {
        AvroSchemaInferrer inferrer = new AvroSchemaInferrer();
        Map<String, Object> map = new HashMap<>();
        map.put("array", Arrays.asList(1, 2, 3));
        Schema schema = inferrer.infer(map, "TestArray");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestArray\",\"fields\":[{\"name\":\"array\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"int\"]}]}]}";
        assertEquals(expectedSchema, schema.toString());
    }

    public void testNestedRecord() {
        AvroSchemaInferrer inferrer = new AvroSchemaInferrer();
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> subMap = new HashMap<>();
        subMap.put("subField", "subValue");
        map.put("field", subMap);
        Schema schema = inferrer.infer(map, "TestComplexMap");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestComplexMap\",\"fields\":[{\"name\":\"field\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"TestComplexMap_field\",\"fields\":[{\"name\":\"subField\",\"type\":[\"null\",\"string\"]}]}]}]}";
        assertEquals(expectedSchema, schema.toString());
    }

    public void testSchemaWithMapAsMap() {
        // Create a test map with multiple different types of values
        Map<String, Object> map = new HashMap<>();
        map.put("field1", "string");
        map.put("field2", 123);
        map.put("field3", 45.67);

        // Create an AvroSchemaInferrer with mapAsRecord = false
        AvroSchemaInferrer inferrer = new AvroSchemaInferrer(false);

        // Infer the Avro schema for the map
        Schema schema = inferrer.infer(map, "TestRecord");

        // Check that the resulting schema is a map schema with a union value type
        assertEquals(Schema.Type.MAP, schema.getType());
        assertEquals(Schema.Type.UNION, schema.getValueType().getType());
    }
}
