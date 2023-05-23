package dev.twister.avro;

import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.stream.Collectors;

public class AvroWriterTest extends TestCase {
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
        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("nullField", null);
        recordMap.put("booleanField", true);
        recordMap.put("intField", 42);
        recordMap.put("longField", 123456789L);
        recordMap.put("floatField", 3.14f);
        recordMap.put("doubleField", 2.718281828);
        recordMap.put("stringField", "Hello, World!");
        recordMap.put("bytesField", ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        // Read data back using a GenericDatumReader and Decoder
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        assertNull(genericRecord.get("nullField"));
        assertTrue((Boolean) genericRecord.get("booleanField"));
        assertEquals(42, genericRecord.get("intField"));
        assertEquals(123456789L, genericRecord.get("longField"));
        assertEquals(3.14f, (Float) genericRecord.get("floatField"), 0.001);
        assertEquals(2.718281828, (Double) genericRecord.get("doubleField"), 0.000000001);
        assertEquals("Hello, World!", genericRecord.get("stringField").toString());

        ByteBuffer decodedBytes = (ByteBuffer) genericRecord.get("bytesField");
        ByteBuffer expectedBytes = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});
        assertEquals(expectedBytes, decodedBytes);
    }

    public void testBidirectionalPrimitives() throws Exception {
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
        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("nullField", null);
        recordMap.put("booleanField", true);
        recordMap.put("intField", 42);
        recordMap.put("longField", 123456789L);
        recordMap.put("floatField", 3.14f);
        recordMap.put("doubleField", 2.718281828);
        recordMap.put("stringField", "Hello, World!");
        recordMap.put("bytesField", ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);
        Map<String, Object> resultMap = new AvroReader().read(byteBuffer, schema);

        assertEquals(recordMap, resultMap);
    }

    public void testEnumField() throws Exception {
        String schemaJson = "{\"type\":\"record\",\"name\":\"TestEnumRecord\",\"fields\":[{\"name\":\"enumField\",\"type\":{\"type\":\"enum\",\"name\":\"TestEnum\",\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}}]}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("enumField", "GREEN");

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        assertEquals("GREEN", genericRecord.get("enumField").toString());
    }

    public void testArrayField() throws Exception {
        String schemaJson = "{\"type\":\"record\",\"name\":\"TestArrayRecord\",\"fields\":[{\"name\":\"arrayField\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("arrayField", Arrays.asList("Aa", "Bb", "Cc"));

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        List<CharSequence> utf8List = (List<CharSequence>) genericRecord.get("arrayField");
        List<String> stringList = utf8List.stream().map(CharSequence::toString).collect(Collectors.toList());
        assertEquals(Arrays.asList("Aa", "Bb", "Cc"), stringList);
    }

    public void testMapField() throws Exception {
        String schemaJson = "{\"type\":\"record\",\"name\":\"TestMapRecord\",\"fields\":[{\"name\":\"mapField\",\"type\":{\"type\":\"map\",\"values\":\"int\"}}]}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        Map<String, Object> recordMap = new HashMap<>();
        Map<String, Integer> map = new TreeMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        recordMap.put("mapField", map);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        Map<Object, Object> actualMap = (Map<Object, Object>) genericRecord.get("mapField");
        assertEquals(1, actualMap.get(new Utf8("one")));
        assertEquals(2, actualMap.get(new Utf8("two")));
        assertEquals(3, actualMap.get(new Utf8("three")));
    }

    public void testUnionField() throws Exception {
        String schemaJson = "{\"type\":\"record\",\"name\":\"TestUnionRecord\",\"fields\":[{\"name\":\"unionField\",\"type\":[\"null\",\"string\"]}]}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("unionField", "example");

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        assertEquals("example", genericRecord.get("unionField").toString());
    }

    public void testBidirectionalComplexTypes() throws Exception {
        String schemaJson = "{\"type\":\"record\",\"name\":\"TestComplexTypes\",\"fields\":[{\"name\":\"enumField\",\"type\":{\"type\":\"enum\",\"name\":\"TestEnum\",\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}},{\"name\":\"arrayField\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"mapField\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},{\"name\":\"unionField\",\"type\":[\"null\",\"string\"]},{\"name\":\"fixedField\",\"type\":{\"type\":\"fixed\",\"name\":\"TestFixed\",\"size\":4}}]}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("enumField", "GREEN");
        recordMap.put("arrayField", Arrays.asList("Aa", "Bb", "Cc"));
        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        recordMap.put("mapField", map);
        recordMap.put("unionField", "example");
        recordMap.put("fixedField", ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);
        Map<String, Object> result = new AvroReader().read(byteBuffer, schema);

        assertEquals("GREEN", result.get("enumField"));
        assertEquals(Arrays.asList("Aa", "Bb", "Cc"), result.get("arrayField"));
        assertEquals(map, result.get("mapField"));
        assertEquals("example", result.get("unionField"));
        ByteBuffer decodedBytes = (ByteBuffer) result.get("fixedField");
        assertEquals(4, decodedBytes.remaining());
        for (int i = 1; i <= 4; i++) {
            assertEquals(i, decodedBytes.get());
        }
    }

    public void testWriteWithInferredSchema() throws Exception {
        // Define a map of values
        Map<String, Object> testMap = new HashMap<>();
        testMap.put("stringField", "Hello, World!");
        testMap.put("intField", 42);
        testMap.put("booleanField", true);
        testMap.put("longField", 123456789L);
        testMap.put("floatField", 3.14f);
        testMap.put("doubleField", 2.718281828);
        testMap.put("nullField", null);

        // Write the map to Avro
        AvroWriter avroWriter = new AvroWriter();
        ByteBuffer avroData = avroWriter.write(testMap, "TestSchema");

        // Manually create expected schema
        Schema expectedSchema = SchemaBuilder.record("TestSchema").fields()
                .name("booleanField").type().unionOf().nullType().and().booleanType().endUnion().noDefault()
                .name("doubleField").type().unionOf().nullType().and().doubleType().endUnion().noDefault()
                .name("floatField").type().unionOf().nullType().and().floatType().endUnion().noDefault()
                .name("intField").type().unionOf().nullType().and().intType().endUnion().noDefault()
                .name("longField").type().unionOf().nullType().and().longType().endUnion().noDefault()
                .name("nullField").type().nullType().noDefault()
                .name("stringField").type().unionOf().nullType().and().stringType().endUnion().noDefault()
                .endRecord();

        DatumReader<GenericRecord> reader = new SpecificDatumReader<>(expectedSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(avroData.array()), null);
        GenericRecord record = reader.read(null, decoder);

        // Validate the read back data
        assertEquals("Hello, World!", record.get("stringField").toString());
        assertEquals(42, record.get("intField"));
        assertEquals(true, record.get("booleanField"));
        assertEquals(123456789L, record.get("longField"));
        assertEquals(3.14f, (float)record.get("floatField"), 0.001);
        assertEquals(2.718281828, (double)record.get("doubleField"), 0.001);
        assertNull(record.get("nullField"));
    }

    public void testDecimal() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"decimalField\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        recordMap.put("decimalField", new BigDecimal("12.34"));

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        BigDecimal decodedDecimal = new BigDecimal(new BigInteger(((ByteBuffer) genericRecord.get("decimalField")).array()), 2);
        assertEquals(new BigDecimal("12.34"), decodedDecimal);
    }

    public void testUUID() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"uuidField\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        UUID uuid = UUID.randomUUID();
        recordMap.put("uuidField", uuid);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        assertEquals(uuid.toString(), genericRecord.get("uuidField").toString());
    }

    public void testDate() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"dateField\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        LocalDate date = LocalDate.now();
        recordMap.put("dateField", date);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        int readDays = (Integer) genericRecord.get("dateField");
        assertEquals(date, LocalDate.ofEpochDay(readDays));
    }

    public void testTimeMillis() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"timeMillisField\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        LocalTime time = LocalTime.now();
        recordMap.put("timeMillisField", time);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        int readMillis = (Integer) genericRecord.get("timeMillisField");
        assertEquals(time.truncatedTo(ChronoUnit.MILLIS), LocalTime.ofNanoOfDay((long) readMillis * 1_000_000));
    }

    public void testTimeMicros() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"timeMicrosField\", \"type\": {\"type\": \"long\", \"logicalType\": \"time-micros\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        LocalTime time = LocalTime.now();
        recordMap.put("timeMicrosField", time);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        long readMicros = (Long) genericRecord.get("timeMicrosField");
        assertEquals(time.truncatedTo(ChronoUnit.MICROS), LocalTime.ofNanoOfDay(readMicros * 1_000));
    }

    public void testTimestampMillis() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"timestampMillisField\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        Instant timestamp = Instant.now();
        recordMap.put("timestampMillisField", timestamp);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        long readMillis = (Long) genericRecord.get("timestampMillisField");
        assertEquals(timestamp.truncatedTo(ChronoUnit.MILLIS), Instant.ofEpochMilli(readMillis));
    }

    public void testTimestampMicros() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"timestampMicrosField\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        Instant timestamp = Instant.now();
        recordMap.put("timestampMicrosField", timestamp);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        long readMicros = (Long) genericRecord.get("timestampMicrosField");
        assertEquals(timestamp.truncatedTo(ChronoUnit.MICROS), Instant.ofEpochSecond(0, readMicros * 1_000));
    }

    public void testLocalTimestampMillis() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"localTimestampMillisField\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        LocalDateTime localTimestamp = LocalDateTime.now();
        recordMap.put("localTimestampMillisField", localTimestamp);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        long readMillis = (Long) genericRecord.get("localTimestampMillisField");
        assertEquals(localTimestamp.truncatedTo(ChronoUnit.MILLIS), LocalDateTime.ofInstant(Instant.ofEpochMilli(readMillis), ZoneOffset.UTC));
    }

    public void testLocalTimestampMicros() throws Exception {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"localTimestampMicrosField\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);
        Map<String, Object> recordMap = new HashMap<>();
        LocalDateTime localTimestamp = LocalDateTime.now();
        recordMap.put("localTimestampMicrosField", localTimestamp);

        ByteBuffer byteBuffer = new AvroWriter().write(recordMap, schema);

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteBuffer.array(), null);
        GenericRecord genericRecord = datumReader.read(null, decoder);

        long readMicros = (Long) genericRecord.get("localTimestampMicrosField");
        assertEquals(localTimestamp.truncatedTo(ChronoUnit.MICROS), LocalDateTime.ofInstant(Instant.ofEpochSecond(0, readMicros * 1_000), ZoneOffset.UTC));
    }
}