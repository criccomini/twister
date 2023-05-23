package dev.twister.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This class provides functionality to write Avro data based on the provided Avro schema and data.
 */
public class AvroWriter {

    /**
     * A map of default logical type writers provided out-of-the-box by AvroWriter. Each entry maps a logical type name
     * to a LogicalTypeWriter capable of writing data of that logical type.
     */
    public static final Map<String, LogicalTypeWriter> DEFAULT_LOGICAL_TYPE_WRITERS;

    /**
     * A map of logical type writers that will be used by this AvroWriter instance. Each entry maps a logical type name
     * to a LogicalTypeWriter capable of writing data of that logical type.
     * Can be replaced with a custom map to override the default logical type writers.
     */
    private final Map<String, LogicalTypeWriter> logicalTypeWriters;

    /**
     * Default constructor that uses the default logical type writers.
     */
    public AvroWriter() {
        this(DEFAULT_LOGICAL_TYPE_WRITERS);
    }

    /**
     * Constructor that accepts a custom map of logical type writers.
     *
     * @param logicalTypeWriters A map of logical type writers. Each entry maps a logical type name
     *                           to a LogicalTypeWriter capable of writing data of that logical type.
     */
    public AvroWriter(Map<String, LogicalTypeWriter> logicalTypeWriters) {
        this.logicalTypeWriters = logicalTypeWriters;
    }

    /**
     * This class provides functionality to write Avro data based on a provided Avro schema and map data.
     */
    public class MapDatumWriter implements DatumWriter<Map<String, Object>> {
        private Schema schema;

        public MapDatumWriter(Schema schema) {
            this.schema = schema;
        }

        @Override
        public void setSchema(Schema schema) {
            this.schema = schema;
        }

        /**
         * Writes an object to the encoder output based on the provided Avro schema.
         *
         * @param value The object to be written.
         * @param schema The Avro schema to use for writing.
         * @param out The encoder output to write to.
         * @throws IOException If an error occurs during writing.
         */
        private void writeObject(Object value, Schema schema, Encoder out) throws IOException {
            LogicalType logicalType = schema.getLogicalType();
            if (logicalType != null) {
                LogicalTypeWriter logicalTypeWriter = logicalTypeWriters.get(logicalType.getName());
                if (logicalTypeWriter != null) {
                    logicalTypeWriter.write(value, schema, out);
                    return;
                }
            }

            switch (schema.getType()) {
                case BOOLEAN:
                    out.writeBoolean((Boolean) value);
                    break;
                case INT:
                    out.writeInt((Integer) value);
                    break;
                case LONG:
                    out.writeLong((Long) value);
                    break;
                case FLOAT:
                    out.writeFloat((Float) value);
                    break;
                case DOUBLE:
                    out.writeDouble((Double) value);
                    break;
                case STRING:
                    out.writeString((String) value);
                    break;
                case BYTES:
                    out.writeBytes((ByteBuffer) value);
                    break;
                case RECORD:
                    Map<String, Object> recordValue = (Map<String, Object>) value;
                    MapDatumWriter recordWriter = new MapDatumWriter(schema);
                    recordWriter.write(recordValue, out);
                    break;
                case ENUM:
                    String enumValue = (String) value;
                    int index = schema.getEnumSymbols().indexOf(enumValue);
                    if (index < 0) {
                        throw new IOException("Invalid enum value: " + enumValue + " for schema: " + schema);
                    }
                    out.writeEnum(index);
                    break;
                case ARRAY:
                    List<Object> arrayValue = (List<Object>) value;
                    out.writeArrayStart();
                    out.setItemCount(arrayValue.size());
                    Schema arraySchema = schema.getElementType();
                    for (Object item : arrayValue) {
                        out.startItem();
                        writeObject(item, arraySchema, out);
                    }
                    out.writeArrayEnd();
                    break;
                case MAP:
                    Map<String, Object> mapValue = (Map<String, Object>) value;
                    out.writeMapStart();
                    out.setItemCount(mapValue.size());
                    Schema mapValueSchema = schema.getValueType();
                    for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
                        out.startItem();
                        out.writeString(entry.getKey());
                        writeObject(entry.getValue(), mapValueSchema, out);
                    }
                    out.writeMapEnd();
                    break;
                case UNION:
                    List<Schema> unionSchemas = schema.getTypes();
                    int matchingSchemaIndex = getMatchingSchemaIndex(value, unionSchemas);
                    out.writeIndex(matchingSchemaIndex);
                    writeObject(value, unionSchemas.get(matchingSchemaIndex), out);
                    break;
                case FIXED:
                    ByteBuffer fixedValueBuffer = (ByteBuffer) value;
                    if (fixedValueBuffer.remaining() != schema.getFixedSize()) {
                        throw new IOException("Invalid fixed value size: " + fixedValueBuffer.remaining()
                                + " for schema: " + schema);
                    }
                    out.writeFixed(fixedValueBuffer);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + schema.getType());
            }
        }

        @Override
        public void write(Map<String, Object> datum, Encoder out) throws IOException {
            for (Schema.Field field : schema.getFields()) {
                Object value = datum.get(field.name());
                if (value == null) {
                    out.writeNull();
                } else {
                    writeObject(value, field.schema(), out);
                }
            }
        }

        /**
         * Returns the expected Java class for the provided Avro schema.
         *
         * @param schema The Avro schema to get the expected class for.
         * @return The expected Java class for the provided Avro schema.
         * @throws UnsupportedOperationException If the schema type is unsupported.
         */
        private Class<?> getExpectedClass(Schema schema) {
            LogicalType logicalType = schema.getLogicalType();
            if (logicalType != null) {
                LogicalTypeWriter logicalTypeWriter = logicalTypeWriters.get(logicalType.getName());
                if (logicalTypeWriter != null) {
                    return logicalTypeWriter.getExpectedClass();
                }
            }

            switch (schema.getType()) {
                case BOOLEAN: return Boolean.class;
                case INT:     return Integer.class;
                case LONG:    return Long.class;
                case FLOAT:   return Float.class;
                case DOUBLE:  return Double.class;
                case ENUM:
                case STRING:  return String.class;
                case FIXED:
                case BYTES:   return ByteBuffer.class;
                case ARRAY:   return List.class;
                case RECORD:
                case MAP:     return Map.class;
                case NULL:    return null;
                default:      throw new UnsupportedOperationException("Unsupported type: " + schema.getType());
            }
        }

        /**
         * Returns the index of the matching schema in the list of union schemas.
         *
         * @param value The value to match the schema with.
         * @param unionSchemas The list of union schemas.
         * @return The index of the matching schema in the list of union schemas.
         * @throws IOException If no matching schema is found.
         */
        private int getMatchingSchemaIndex(Object value, List<Schema> unionSchemas) throws IOException {
            for (int i = 0; i < unionSchemas.size(); i++) {
                Schema unionSchema = unionSchemas.get(i);
                Class<?> expectedClass = getExpectedClass(unionSchema);
                if (value == null && expectedClass == null) {
                    return i;
                }
                if (value != null && expectedClass != null && expectedClass.isInstance(value)) {
                    return i;
                }
            }
            throw new IOException("Invalid union value: " + value + " for schema: " + unionSchemas);
        }
    }

    /**
     * Writes the given object to a ByteBuffer based on the inferred Avro schema.
     *
     * @param object The object to be written.
     * @param recordName The name of the Avro record.
     * @return A ByteBuffer containing the written Avro data.
     * @throws IOException If an error occurs during writing.
     */
    public ByteBuffer write(Map<String, Object> object, String recordName) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        MapDatumWriter writer = new MapDatumWriter(new AvroSchemaInferrer().infer(object, recordName));
        writer.write(object, encoder);
        encoder.flush();
        return ByteBuffer.wrap(outputStream.toByteArray());
    }

    /**
     * Writes the given object to a ByteBuffer based on the provided Avro schema.
     *
     * @param object The object to be written.
     * @param schema The Avro schema to use for writing.
     * @return A ByteBuffer containing the written Avro data.
     * @throws IOException If an error occurs during writing.
     */
    public ByteBuffer write(Map<String, Object> object, Schema schema) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        MapDatumWriter writer = new MapDatumWriter(schema);
        writer.write(object, encoder);
        encoder.flush();
        return ByteBuffer.wrap(outputStream.toByteArray());
    }

    /**
     * This interface provides a contract for classes that write logical types based on a provided Avro schema and data.
     */
    public interface LogicalTypeWriter {
        /**
         * Writes a logical type value to the output Encoder.
         *
         * @param value  The value to write, expected to be of the class returned by getExpectedClass().
         * @param schema The Avro Schema for the data being written.
         * @param out    The Encoder to write to.
         * @throws IOException If there's an error writing the data.
         */
        void write(Object value, Schema schema, Encoder out) throws IOException;

        /**
         * Returns the Java class that this LogicalTypeWriter expects to write.
         *
         * @return The Java class that this LogicalTypeWriter expects to write.
         */
        Class<?> getExpectedClass();
    }

    static {
        DEFAULT_LOGICAL_TYPE_WRITERS = Map.of(
                "decimal", new DecimalWriter(),
                "uuid", new UuidWriter(),
                "date", new DateWriter(),
                "time-millis", new TimeMillisWriter(),
                "time-micros", new TimeMicrosWriter(),
                "timestamp-millis", new TimestampMillisWriter(),
                "timestamp-micros", new TimestampMicrosWriter(),
                "local-timestamp-millis", new LocalTimestampMillisWriter(),
                "local-timestamp-micros", new LocalTimestampMicrosWriter()
        );
    }

    /**
     * A LogicalTypeWriter implementation for writing "decimal" Avro logical type data.
     * The expected Java class is java.math.BigDecimal.
     */
    public static class DecimalWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            out.writeBytes(((BigDecimal) value).unscaledValue().toByteArray());
        }

        @Override
        public Class<?> getExpectedClass() {
            return BigDecimal.class;
        }
    }

    /**
     * A LogicalTypeWriter implementation for writing "uuid" Avro logical type data.
     * The expected Java class is java.util.UUID.
     */
    public static class UuidWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            out.writeString(value.toString());
        }

        @Override
        public Class<?> getExpectedClass() {
            return UUID.class;
        }
    }

    /**
     * A LogicalTypeWriter implementation for writing "date" Avro logical type data.
     * The expected Java class is java.time.LocalDate.
     */
    public static class DateWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            out.writeInt((int) ((LocalDate) value).toEpochDay());
        }

        @Override
        public Class<?> getExpectedClass() {
            return LocalDate.class;
        }
    }

    /**
     * A LogicalTypeWriter implementation for writing "time-millis" Avro logical type data.
     * The expected Java class is java.time.LocalTime.
     */
    public static class TimeMillisWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            out.writeInt((int) (((LocalTime) value).toNanoOfDay() / 1_000_000));
        }

        @Override
        public Class<?> getExpectedClass() {
            return LocalTime.class;
        }
    }

    /**
     * A LogicalTypeWriter implementation for writing "time-micros" Avro logical type data.
     * The expected Java class is java.time.LocalTime.
     */
    public static class TimeMicrosWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            out.writeLong(((LocalTime) value).toNanoOfDay() / 1_000);
        }

        @Override
        public Class<?> getExpectedClass() {
            return LocalTime.class;
        }
    }

    /**
     * A LogicalTypeWriter implementation for writing "timestamp-millis" Avro logical type data.
     * The expected Java class is java.time.Instant.
     */
    public static class TimestampMillisWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            out.writeLong(((Instant) value).toEpochMilli());
        }

        @Override
        public Class<?> getExpectedClass() {
            return Instant.class;
        }
    }

    /**
     * A LogicalTypeWriter implementation for writing "timestamp-micros" Avro logical type data.
     * The expected Java class is java.time.Instant.
     */
    public static class TimestampMicrosWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            out.writeLong(((Instant) value).getEpochSecond() * 1_000_000 + ((Instant) value).getNano() / 1_000);
        }

        @Override
        public Class<?> getExpectedClass() {
            return Instant.class;
        }
    }

    /**
     * A LogicalTypeWriter implementation for writing "local-timestamp-millis" Avro logical type data.
     * The expected Java class is java.time.LocalDateTime.
     */
    public static class LocalTimestampMillisWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            out.writeLong(((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli());
        }

        @Override
        public Class<?> getExpectedClass() {
            return LocalDateTime.class;
        }
    }

    /**
     * A LogicalTypeWriter implementation for writing "local-timestamp-micros" Avro logical type data.
     * The expected Java class is java.time.LocalDateTime.
     */
    public static class LocalTimestampMicrosWriter implements AvroWriter.LogicalTypeWriter {
        @Override
        public void write(Object value, Schema schema, Encoder out) throws IOException {
            LocalDateTime dateTime = (LocalDateTime) value;
            long micros = dateTime.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + dateTime.getNano() / 1_000;
            out.writeLong(micros);
        }

        @Override
        public Class<?> getExpectedClass() {
            return LocalDateTime.class;
        }
    }
}
