package dev.twister.avro;

import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AvroReaderTest extends TestCase {
    public void testPrimitives() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"nullField\", \"type\": \"null\"},\n" +
                "    {\"name\": \"booleanField\", \"type\": \"boolean\"},\n" +
                "    {\"name\": \"intField\", \"type\": \"int\"},\n" +
                "    {\"name\": \"longField\", \"type\": \"long\"},\n" +
                "    {\"name\": \"floatField\", \"type\": \"float\"},\n" +
                "    {\"name\": \"doubleField\", \"type\": \"double\"},\n" +
                "    {\"name\": \"stringField\", \"type\": \"string\"},\n" +
                "    {\"name\": \"bytesField\", \"type\": \"bytes\"}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericData.Record record = new GenericData.Record(schema);
        record.put("nullField", null);
        record.put("booleanField", true);
        record.put("intField", 42);
        record.put("longField", 123456789L);
        record.put("floatField", 3.14f);
        record.put("doubleField", 2.718281828);
        record.put("stringField", "Hello, World!");
        record.put("bytesField", ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        assertNull(resultMap.get("nullField"));
        assertTrue((Boolean) resultMap.get("booleanField"));
        assertEquals(42, resultMap.get("intField"));
        assertEquals(123456789L, resultMap.get("longField"));
        assertEquals(3.14f, (Float) resultMap.get("floatField"), 0.001);
        assertEquals(2.718281828, (Double) resultMap.get("doubleField"), 0.000000001);
        assertEquals("Hello, World!", resultMap.get("stringField"));

        ByteBuffer decodedBytes = (ByteBuffer) resultMap.get("bytesField");
        assertEquals(5, decodedBytes.remaining());
        for (int i = 1; i <= 5; i++) {
            assertEquals(i, decodedBytes.get());
        }
    }

    public void testComplexTypes() throws Exception {
        Schema schema = Schema.parse("{\"type\":\"record\",\"name\":\"TestComplexTypes\",\"fields\":[{\"name\":\"enumField\",\"type\":{\"type\":\"enum\",\"name\":\"TestEnum\",\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}},{\"name\":\"arrayField\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"mapField\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},{\"name\":\"unionField\",\"type\":[\"null\",\"string\"]},{\"name\":\"fixedField\",\"type\":{\"type\":\"fixed\",\"name\":\"TestFixed\",\"size\":4}}]}");

        GenericData.Record record = new GenericData.Record(schema);
        record.put("enumField", new GenericData.EnumSymbol(schema.getField("enumField").schema(), "GREEN"));
        record.put("arrayField", new GenericData.Array<>(schema.getField("arrayField").schema(), Arrays.asList("Aa", "Bb", "Cc")));
        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        record.put("mapField", map);
        record.put("unionField", "example");
        record.put("fixedField", new GenericData.Fixed(schema.getField("fixedField").schema(), new byte[]{1, 2, 3, 4}));

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> result = new AvroReader().read(byteBuffer, schema);

        assertEquals("GREEN", result.get("enumField"));
        assertEquals(Arrays.asList("Aa", "Bb", "Cc"), result.get("arrayField"));
        assertEquals(map, result.get("mapField"));
        assertEquals("example", result.get("unionField"));
        assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), result.get("fixedField"));
    }

    public void testNullStringUnionType() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"unionField\", \"type\": [\"null\", \"string\"]}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);

        // Test with string value
        GenericData.Record recordWithString = new GenericData.Record(schema);
        recordWithString.put("unionField", "Hello, World!");

        ByteBuffer byteBufferWithString = encodeRecordToByteBuffer(recordWithString, schema);
        Map<String, Object> resultMapWithString = new AvroReader().read(byteBufferWithString, schema);

        assertEquals("Hello, World!", resultMapWithString.get("unionField"));

        // Test with null value
        GenericData.Record recordWithNull = new GenericData.Record(schema);
        recordWithNull.put("unionField", null);

        ByteBuffer byteBufferWithNull = encodeRecordToByteBuffer(recordWithNull, schema);
        Map<String, Object> resultMapWithNull = new AvroReader().read(byteBufferWithNull, schema);

        assertNull(resultMapWithNull.get("unionField"));
    }

    public void testDecimalLogicalType() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"decimalField\", \"type\": {\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        BigDecimal decimalValue = new BigDecimal("12.34");
        BigInteger unscaledValue = decimalValue.unscaledValue();
        byte[] bytesValue = unscaledValue.toByteArray();

        GenericData.Record record = new GenericData.Record(schema);
        record.put("decimalField", ByteBuffer.wrap(bytesValue));

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        BigDecimal resultDecimal = (BigDecimal) resultMap.get("decimalField");

        assertEquals(decimalValue, resultDecimal);
    }

    public void testUuidLogicalType() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"uuidField\", \"type\": {\"type\":\"string\",\"logicalType\":\"uuid\"}}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        UUID uuidValue = UUID.randomUUID();
        GenericData.Record record = new GenericData.Record(schema);
        record.put("uuidField", uuidValue.toString());

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        assertEquals(uuidValue, resultMap.get("uuidField"));
    }

    public void testDateLogicalType() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"dateField\", \"type\": {\"type\":\"int\",\"logicalType\":\"date\"}}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        LocalDate dateValue = LocalDate.of(2023, 5, 22);
        int daysSinceEpoch = (int) dateValue.toEpochDay();
        GenericData.Record record = new GenericData.Record(schema);
        record.put("dateField", daysSinceEpoch);

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        assertEquals(dateValue, resultMap.get("dateField"));
    }

    public void testTimeMillisLogicalType() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"timeMillisField\", \"type\": {\"type\":\"int\",\"logicalType\":\"time-millis\"}}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        int millisInDay = 12345678; // Represents the number of milliseconds past midnight.
        GenericData.Record record = new GenericData.Record(schema);
        record.put("timeMillisField", millisInDay);

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        assertEquals(LocalTime.ofNanoOfDay(millisInDay * 1000000L), resultMap.get("timeMillisField"));
    }

    public void testTimeMicrosLogicalType() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"timeMicrosField\", \"type\": {\"type\":\"long\",\"logicalType\":\"time-micros\"}}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        long microsInDay = 12345678910L; // Represents the number of microseconds past midnight.
        GenericData.Record record = new GenericData.Record(schema);
        record.put("timeMicrosField", microsInDay);

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        assertEquals(LocalTime.ofNanoOfDay(microsInDay * 1000L), resultMap.get("timeMicrosField"));
    }

    public void testTimestampMillisLogicalType() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"timestampMillisField\", \"type\": {\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        long timestampMillis = 1234567891011L; // Represents a timestamp in milliseconds since the Unix epoch.
        GenericData.Record record = new GenericData.Record(schema);
        record.put("timestampMillisField", timestampMillis);

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        assertEquals(Instant.ofEpochMilli(timestampMillis), resultMap.get("timestampMillisField"));
    }

    public void testTimestampMicrosLogicalType() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"timestampMicrosField\", \"type\": {\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        long timestampMicros = 12345678910111213L; // Represents a timestamp in microseconds since the Unix epoch.
        GenericData.Record record = new GenericData.Record(schema);
        record.put("timestampMicrosField", timestampMicros);

        ByteBuffer byteBuffer = encodeRecordToByteBuffer(record, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        assertEquals(Instant.ofEpochSecond(timestampMicros / 1_000_000, (timestampMicros % 1_000_000) * 1_000), resultMap.get("timestampMicrosField"));
    }

    private ByteBuffer encodeRecordToByteBuffer(GenericData.Record record, Schema schema) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.write(record, binaryEncoder);
        binaryEncoder.flush();
        return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    }
}
