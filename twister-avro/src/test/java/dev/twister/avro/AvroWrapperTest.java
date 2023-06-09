package dev.twister.avro;

import junit.framework.TestCase;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AvroWrapperTest extends TestCase {
    public void testWrapPrimitives() {
        Schema schema = Schema.createRecord("Test", "", "", false);
        schema.setFields(Arrays.asList(
                new Schema.Field("testString", Schema.create(Schema.Type.STRING), "", null),
                new Schema.Field("testInt", Schema.create(Schema.Type.INT), "", null),
                new Schema.Field("testBoolean", Schema.create(Schema.Type.BOOLEAN), "", null),
                new Schema.Field("testLong", Schema.create(Schema.Type.LONG), "", null),
                new Schema.Field("testFloat", Schema.create(Schema.Type.FLOAT), "", null),
                new Schema.Field("testDouble", Schema.create(Schema.Type.DOUBLE), "", null),
                new Schema.Field("testBytesArray", Schema.create(Schema.Type.BYTES), "", null),
                new Schema.Field("testBytesBuffer", Schema.create(Schema.Type.BYTES), "", null),
                new Schema.Field("testNull", Schema.create(Schema.Type.NULL), "", null)
        ));

        GenericRecord record = new GenericData.Record(schema);
        record.put("testString", "string");
        record.put("testInt", 123);
        record.put("testBoolean", true);
        record.put("testLong", 123L);
        record.put("testFloat", 123.45f);
        record.put("testDouble", 123.45);
        record.put("testBytesArray", new byte[] {1, 2, 3});
        record.put("testBytesBuffer", ByteBuffer.wrap(new byte[] {4, 5, 6}));
        record.put("testNull", null);

        AvroWrapper wrapper = new AvroWrapper();

        Map<String, Object> result = wrapper.wrap(record);

        assertEquals("string", result.get("testString"));
        assertEquals(123, result.get("testInt"));
        assertEquals(true, result.get("testBoolean"));
        assertEquals(123L, result.get("testLong"));
        assertEquals(123.45f, result.get("testFloat"));
        assertEquals(123.45, result.get("testDouble"));
        assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), result.get("testBytesArray"));
        assertEquals(ByteBuffer.wrap(new byte[] {4, 5, 6}), result.get("testBytesBuffer"));
        assertNull(result.get("testNull"));
    }

    public void testWrapNestedRecord() {
        Schema nestedSchema = Schema.createRecord("Nested", "", "", false);
        nestedSchema.setFields(List.of(new Schema.Field("nestedField", Schema.create(Schema.Type.STRING), "", null)));
        GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
        nestedRecord.put("nestedField", "nestedValue");

        Schema schema = Schema.createRecord("Test", "", "", false);
        schema.setFields(List.of(new Schema.Field("nested", nestedSchema, "", null)));
        GenericRecord record = new GenericData.Record(schema);
        record.put("nested", nestedRecord);
        AvroWrapper wrapper = new AvroWrapper();

        Map<String, Object> result = wrapper.wrap(record);

        assertTrue(result.get("nested") instanceof Map);
        assertEquals("nestedValue", ((Map) result.get("nested")).get("nestedField"));
    }

    public void testWrapArray() {
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        GenericData.Array<String> array = new GenericData.Array<>(arraySchema, Arrays.asList("element1", "element2"));

        Schema schema = Schema.createRecord("Test", "", "", false);
        schema.setFields(List.of(new Schema.Field("array", arraySchema, "", null)));
        GenericRecord record = new GenericData.Record(schema);
        record.put("array", array);
        AvroWrapper wrapper = new AvroWrapper();

        Map<String, Object> result = wrapper.wrap(record);

        assertTrue(result.get("array") instanceof List);
        assertEquals("element1", ((List) result.get("array")).get(0));
        assertEquals("element2", ((List) result.get("array")).get(1));
    }

    public void testWrapUnion() {
        Schema unionSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
        Schema schema = Schema.createRecord("Test", "", "", false);
        schema.setFields(List.of(new Schema.Field("union", unionSchema, "", null)));
        GenericRecord record = new GenericData.Record(schema);
        record.put("union", "string");
        AvroWrapper wrapper = new AvroWrapper();

        Map<String, Object> result = wrapper.wrap(record);

        assertEquals("string", result.get("union"));
    }

    public void testWrapUnionWithString() {
        List<Schema> unionTypes = Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));
        Schema unionSchema = Schema.createUnion(unionTypes);
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testUnion", unionSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        record.put("testUnion", "a string");

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertTrue(result.get("testUnion") instanceof String);
        assertEquals("a string", result.get("testUnion"));
    }

    public void testWrapEnum() {
        Schema enumSchema = Schema.createEnum("TestEnum", "", "", Arrays.asList("A", "B", "C"));
        GenericData.EnumSymbol enumSymbol = new GenericData.EnumSymbol(enumSchema, "B");
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testEnum", enumSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        record.put("testEnum", enumSymbol);

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals("B", result.get("testEnum"));
    }

    public void testWrapFixed() {
        Schema fixedSchema = Schema.createFixed("TestFixed", "", "", 4);
        GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema, new byte[] {1, 2, 3, 4});
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testFixed", fixedSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        record.put("testFixed", fixed);

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}), result.get("testFixed"));
    }

    public void testWrapMap() {
        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testMap", mapSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        record.put("testMap", new HashMap<String, Object>() {{
            put("key1", "value1");
            put("key2", "value2");
        }});

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertTrue(result.get("testMap") instanceof Map);
        Map<String, Object> resultMap = (Map<String, Object>) result.get("testMap");
        assertEquals("value1", resultMap.get("key1"));
        assertEquals("value2", resultMap.get("key2"));
    }

    public void testListOfRecords() {
        Schema subRecordSchema = Schema.createRecord("SubRecord", "", "test", false);
        subRecordSchema.setFields(List.of(new Schema.Field("subField", Schema.create(Schema.Type.INT), "", null)));

        Schema arraySchema = Schema.createArray(subRecordSchema);
        Schema mainRecordSchema = Schema.createRecord("MainRecord", "", "test", false);
        mainRecordSchema.setFields(List.of(new Schema.Field("mainField", arraySchema, "", null)));

        GenericRecord subRecord1 = new GenericRecordBuilder(subRecordSchema).set("subField", 1).build();
        GenericRecord subRecord2 = new GenericRecordBuilder(subRecordSchema).set("subField", 2).build();
        GenericData.Array<GenericRecord> array = new GenericData.Array<>(mainRecordSchema.getField("mainField").schema(), Arrays.asList(subRecord1, subRecord2));

        GenericRecord mainRecord = new GenericRecordBuilder(mainRecordSchema).set("mainField", array).build();

        AvroWrapper avroWrapper = new AvroWrapper();
        Map<String, Object> wrapped = avroWrapper.wrap(mainRecord);

        assertTrue(wrapped.get("mainField") instanceof List);
        List<?> list = (List<?>) wrapped.get("mainField");

        assertEquals(2, list.size());

        assertTrue(list.get(0) instanceof Map);
        Map<String, Object> subRecord1Map = (Map<String, Object>) list.get(0);
        assertEquals(1, subRecord1Map.get("subField"));

        assertTrue(list.get(1) instanceof Map);
        Map<String, Object> subRecord2Map = (Map<String, Object>) list.get(1);
        assertEquals(2, subRecord2Map.get("subField"));
    }

    public void testWrapLogicalDate() {
        Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testDate", dateSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        LocalDate testDate = LocalDate.now();
        record.put("testDate", (int) testDate.toEpochDay());

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(testDate, result.get("testDate"));
    }

    public void testWrapLogicalTimestampMillis() {
        Schema timestampMillisSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testTimestampMillis", timestampMillisSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        Instant now = Instant.now();
        record.put("testTimestampMillis", now.toEpochMilli());

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(now.truncatedTo(ChronoUnit.MILLIS), result.get("testTimestampMillis"));
    }

    public void testWrapLogicalTimestampMicros() {
        Schema timestampMicrosSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testTimestampMicros", timestampMicrosSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        Instant now = Instant.now();
        long epochMicros = TimeUnit.SECONDS.toMicros(now.getEpochSecond()) + now.getNano() / 1000;
        record.put("testTimestampMicros", epochMicros);

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(now.truncatedTo(ChronoUnit.MICROS), result.get("testTimestampMicros"));
    }

    public void testWrapLogicalLocalTimestampMillis() {
        Schema localTimestampMillisSchema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testLocalTimestampMillis", localTimestampMillisSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        LocalDateTime now = LocalDateTime.now();
        long epochMillis = now.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        record.put("testLocalTimestampMillis", epochMillis);

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(now.truncatedTo(ChronoUnit.MILLIS), result.get("testLocalTimestampMillis"));
    }

    public void testWrapLogicalLocalTimestampMicros() {
        Schema localTimestampMicrosSchema = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testLocalTimestampMicros", localTimestampMicrosSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        LocalDateTime now = LocalDateTime.now();
        long epochMicros = TimeUnit.SECONDS.toMicros(now.atZone(ZoneOffset.UTC).toEpochSecond()) + now.getNano() / 1000;
        record.put("testLocalTimestampMicros", epochMicros);

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(now.truncatedTo(ChronoUnit.MICROS), result.get("testLocalTimestampMicros"));
    }

    public void testWrapLogicalTimeMillis() {
        Schema timeMillisSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testTimeMillis", timeMillisSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        LocalTime now = LocalTime.now();
        int millisOfDay = (int) (TimeUnit.SECONDS.toMillis(now.toSecondOfDay()) + (now.getNano() / 1_000_000));
        record.put("testTimeMillis", millisOfDay);

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(now.truncatedTo(ChronoUnit.MILLIS), result.get("testTimeMillis"));
    }

    public void testWrapLogicalTimeMicros() {
        Schema timeMicrosSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testTimeMicros", timeMicrosSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        LocalTime now = LocalTime.now();
        long microsOfDay = TimeUnit.SECONDS.toMicros(now.toSecondOfDay()) + now.getNano() / 1000;
        record.put("testTimeMicros", microsOfDay);

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(now.truncatedTo(ChronoUnit.MICROS), result.get("testTimeMicros"));
    }

    public void testWrapLogicalDecimal() {
        LogicalTypes.Decimal decimalLogicalType = LogicalTypes.decimal(9, 2);
        Schema decimalSchema = decimalLogicalType.addToSchema(Schema.createFixed("TestDecimal", "", "", 5));
        Schema recordSchema = Schema.createRecord("TestRecord", "", "", false);
        recordSchema.setFields(Collections.singletonList(new Schema.Field("testDecimal", decimalSchema, "", null)));

        GenericRecord record = new GenericData.Record(recordSchema);
        BigDecimal decimalValue = new BigDecimal("12345.67");
        record.put("testDecimal", new Conversions.DecimalConversion().toFixed(decimalValue, decimalSchema, decimalLogicalType));

        AvroWrapper wrapper = new AvroWrapper();
        Map<String, Object> result = wrapper.wrap(record);

        assertEquals(decimalValue, result.get("testDecimal"));
    }
}
