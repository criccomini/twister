package dev.twister.avro;

import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
        AvroSchemaInferrer inferrer = new AvroSchemaInferrer(AvroSchemaInferrer.mapOfDefaultInferrers(TimeUnit.MILLISECONDS), false);

        // Infer the Avro schema for the map
        Schema schema = inferrer.infer(map, "TestRecord");

        // Check that the resulting schema is a map schema with a union value type
        assertEquals(Schema.Type.MAP, schema.getType());
        assertEquals(Schema.Type.UNION, schema.getValueType().getType());
    }

    public void testBigDecimal() {
        Map<String, Object> map = new HashMap<>();
        BigDecimal decimalValue = new BigDecimal("123.456");
        map.put("decimalField", decimalValue);

        Schema schema = new AvroSchemaInferrer().infer(map, "TestDecimal");
        String expectedSchema = String.format("{\"type\":\"record\",\"name\":\"TestDecimal\",\"fields\":[{\"name\":\"decimalField\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":%d,\"scale\":%d}]}]}", decimalValue.precision(), decimalValue.scale());

        assertEquals(expectedSchema, schema.toString());
    }

    public void testUUID() {
        Map<String, Object> map = new HashMap<>();
        UUID uuid = UUID.randomUUID();
        map.put("uuidField", uuid);

        Schema schema = new AvroSchemaInferrer().infer(map, "TestUUID");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestUUID\",\"fields\":[{\"name\":\"uuidField\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"uuid\"}]}]}";

        assertEquals(expectedSchema, schema.toString());
    }

    public void testLocalDate() {
        Map<String, Object> map = new HashMap<>();
        LocalDate date = LocalDate.now();
        map.put("dateField", date);

        Schema schema = new AvroSchemaInferrer().infer(map, "TestDate");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestDate\",\"fields\":[{\"name\":\"dateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}";

        assertEquals(expectedSchema, schema.toString());
    }

    public void testLocalTimeMillis() {
        Map<String, Object> map = new HashMap<>();
        LocalTime time = LocalTime.now();
        map.put("timeField", time);

        Schema schema = new AvroSchemaInferrer().infer(map, "TestTime");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestTime\",\"fields\":[{\"name\":\"timeField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"time-millis\"}]}]}";

        assertEquals(expectedSchema, schema.toString());
    }

    public void testLocalTimeMicros() {
        Map<String, Object> map = new HashMap<>();
        LocalTime time = LocalTime.now();
        map.put("timeField", time);

        AvroSchemaInferrer inferrer = new AvroSchemaInferrer(AvroSchemaInferrer.mapOfDefaultInferrers(TimeUnit.MICROSECONDS), true);
        Schema schema = inferrer.infer(map, "TestTimeMicros");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestTimeMicros\",\"fields\":[{\"name\":\"timeField\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"time-micros\"}]}]}";

        assertEquals(expectedSchema, schema.toString());
    }

    public void testInstantMillis() {
        Map<String, Object> map = new HashMap<>();
        Instant instant = Instant.now();
        map.put("timestampField", instant);

        Schema schema = new AvroSchemaInferrer().infer(map, "TestTimestamp");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestTimestamp\",\"fields\":[{\"name\":\"timestampField\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}]}]}";

        assertEquals(expectedSchema, schema.toString());
    }

    public void testInstantMicros() {
        Map<String, Object> map = new HashMap<>();
        Instant instant = Instant.now();
        map.put("timestampField", instant);

        AvroSchemaInferrer inferrer = new AvroSchemaInferrer(AvroSchemaInferrer.mapOfDefaultInferrers(TimeUnit.MICROSECONDS), true);
        Schema schema = inferrer.infer(map, "TestTimestampMicros");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestTimestampMicros\",\"fields\":[{\"name\":\"timestampField\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}]}";

        assertEquals(expectedSchema, schema.toString());
    }

    public void testLocalDateTimeMillis() {
        Map<String, Object> map = new HashMap<>();
        LocalDateTime dateTime = LocalDateTime.now();
        map.put("dateTimeField", dateTime);

        Schema schema = new AvroSchemaInferrer().infer(map, "TestDateTime");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestDateTime\",\"fields\":[{\"name\":\"dateTimeField\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}]}]}";

        assertEquals(expectedSchema, schema.toString());
    }

    public void testLocalDateTimeMicros() {
        Map<String, Object> map = new HashMap<>();
        LocalDateTime dateTime = LocalDateTime.now();
        map.put("dateTimeField", dateTime);

        AvroSchemaInferrer inferrer = new AvroSchemaInferrer(AvroSchemaInferrer.mapOfDefaultInferrers(TimeUnit.MICROSECONDS), true);
        Schema schema = inferrer.infer(map, "TestDateTimeMicros");
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestDateTimeMicros\",\"fields\":[{\"name\":\"dateTimeField\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}]}";

        assertEquals(expectedSchema, schema.toString());
    }
}
