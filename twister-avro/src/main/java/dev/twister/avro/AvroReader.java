package dev.twister.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A utility class to read Avro-encoded data into Java Map objects.
 */
public class AvroReader {

    /**
     * A {@link Map} of default {@link LogicalTypeReader}s used to interpret Avro logical types.
     * <p>
     * The map's keys are the names of Avro logical types, and the values are the {@link LogicalTypeReader}s
     * that are used to read and interpret these types. By default, this includes readers for the following Avro logical
     * types:
     * <ul>
     *   <li>decimal</li>
     *   <li>uuid</li>
     *   <li>date</li>
     *   <li>time-millis</li>
     *   <li>time-micros</li>
     *   <li>timestamp-millis</li>
     *   <li>timestamp-micros</li>
     * </ul>
     */
    public static final Map<String, LogicalTypeReader> DEFAULT_LOGICAL_TYPE_READERS;

    /**
     * A {@link Map} of {@link LogicalTypeReader}s used to interpret Avro logical types.
     *
     * The map's keys are the names of Avro logical types, and the values are the {@link LogicalTypeReader}s
     * that are used to read and interpret these types. This map is initialized with either the default logical type
     * readers or a custom set provided via the constructor.
     */
    private final Map<String, LogicalTypeReader> logicalTypeReaders;

    /**
     * Constructs a new {@link AvroReader} with the default logical type readers.
     * <p>
     * The default logical type readers are capable of handling the following Avro logical types:
     * <ul>
     *   <li>decimal</li>
     *   <li>uuid</li>
     *   <li>date</li>
     *   <li>time-millis</li>
     *   <li>time-micros</li>
     *   <li>timestamp-millis</li>
     *   <li>timestamp-micros</li>
     * </ul>
     */
    public AvroReader() {
        this(DEFAULT_LOGICAL_TYPE_READERS);
    }

    /**
     * Constructs a new {@link AvroReader} with the provided logical type readers.
     *
     * @param logicalTypeReaders a {@link Map} of {@link String} keys and {@link LogicalTypeReader} values, where each
     * key is the name of an Avro logical type and each value is a {@link LogicalTypeReader} capable of reading that
     * type.
     */
    public AvroReader(Map<String, LogicalTypeReader> logicalTypeReaders) {
        this.logicalTypeReaders = logicalTypeReaders;
    }

    /**
     * Reads Avro-encoded data from a ByteBuffer using a provided schema.
     *
     * @param inputBuffer The ByteBuffer containing the Avro-encoded data.
     * @param schema The Avro schema that describes the data structure.
     * @return A Map representing the Avro data.
     * @throws IOException If there is a problem reading from the ByteBuffer.
     */
    public Map<String, Object> read(ByteBuffer inputBuffer, Schema schema) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputBuffer.array(), null);
        return (Map<String, Object>) readBasedOnSchema(decoder, schema);
    }

    /**
     * Reads Avro-encoded data from a BinaryDecoder based on a provided schema.
     *
     * @param decoder The BinaryDecoder to read data from.
     * @param schema The Avro schema that describes the data structure.
     * @return An Object representing the Avro data.
     * @throws IOException If there is a problem reading from the BinaryDecoder.
     */
    private Object readBasedOnSchema(BinaryDecoder decoder, Schema schema) throws IOException {
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            LogicalTypeReader reader = logicalTypeReaders.get(logicalType.getName());
            if (reader != null) {
                return reader.read(decoder, schema);
            }
        }

        switch (schema.getType()) {
            case RECORD:
                Map<String, Object> resultMap = new HashMap<>();
                for (Schema.Field field : schema.getFields()) {
                    resultMap.put(field.name(), readBasedOnSchema(decoder, field.schema()));
                }
                return resultMap;
            case ENUM:
                return schema.getEnumSymbols().get(decoder.readEnum());
            case UNION:
                int unionIndex = decoder.readIndex();
                return readBasedOnSchema(decoder, schema.getTypes().get(unionIndex));
            case FIXED:
                byte[] fixedBytes = new byte[schema.getFixedSize()];
                decoder.readFixed(fixedBytes);
                return ByteBuffer.wrap(fixedBytes);
            case ARRAY:
                Schema elementSchema = schema.getElementType();
                long arraySize = decoder.readArrayStart();
                List<Object> array = new ArrayList<>();
                while (arraySize > 0) {
                    for (long i = 0; i < arraySize; i++) {
                        array.add(readBasedOnSchema(decoder, elementSchema));
                    }
                    arraySize = decoder.arrayNext();
                }
                return array;
            case MAP:
                Schema valueSchema = schema.getValueType();
                long mapSize = decoder.readMapStart();
                Map<String, Object> map = new HashMap<>();
                while (mapSize > 0) {
                    for (long i = 0; i < mapSize; i++) {
                        String key = decoder.readString();
                        map.put(key, readBasedOnSchema(decoder, valueSchema));
                    }
                    mapSize = decoder.mapNext();
                }
                return map;
            default:
                return readPrimitive(decoder, schema.getType());
        }
    }

    /**
     * Reads a primitive value from a BinaryDecoder based on a provided schema type.
     *
     * @param decoder The BinaryDecoder to read data from.
     * @param type The Avro schema type of the primitive value.
     * @return An Object representing the Avro primitive value.
     * @throws IOException If there is a problem reading from the BinaryDecoder.
     * @throws IllegalArgumentException If the schema type is unsupported.
     */
    private Object readPrimitive(BinaryDecoder decoder, Schema.Type type) throws IOException {
        switch (type) {
            case BOOLEAN:
                return decoder.readBoolean();
            case INT:
                return decoder.readInt();
            case LONG:
                return decoder.readLong();
            case FLOAT:
                return decoder.readFloat();
            case DOUBLE:
                return decoder.readDouble();
            case STRING:
                return decoder.readString();
            case BYTES:
                return decoder.readBytes(null);
            case NULL:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + type);
        }
    }

    /**
     * The {@link LogicalTypeReader} interface defines a contract for reading Avro logical types.
     * <p>
     * A logical type in Avro is a way of specifying a predefined type, which informs how the data should be
     * interpreted.
     * <p>
     * For example, the 'date' logical type represents the number of days since the Unix epoch, and can be interpreted
     * as a date.
     * <p>
     * A {@link LogicalTypeReader} is capable of reading a logical type from an Avro {@link BinaryDecoder} based on the
     * logical type's {@link Schema}.
     */
    public interface LogicalTypeReader {

        /**
         * Reads an Avro logical type from a {@link BinaryDecoder} based on the logical type's {@link Schema}.
         *
         * @param decoder the {@link BinaryDecoder} to read the data from
         * @param schema the {@link Schema} of the logical type to read
         * @return an {@link Object} representing the data of the logical type
         * @throws IOException if an error occurs while reading the data
         */
        Object read(BinaryDecoder decoder, Schema schema) throws IOException;
    }

    static {
        DEFAULT_LOGICAL_TYPE_READERS = Map.of("decimal", (decoder, schema) -> {
            BigInteger unscaledValue = new BigInteger(decoder.readBytes(null).array());
            return new BigDecimal(unscaledValue, ((LogicalTypes.Decimal) schema.getLogicalType()).getScale());
        }, "uuid", (decoder, schema) -> {
            return UUID.fromString(decoder.readString().toString());
        }, "date", (decoder, schema) -> {
            int daysSinceEpoch = decoder.readInt();
            return LocalDate.ofEpochDay(daysSinceEpoch);
        }, "time-millis", (decoder, schema) -> {
            int millisOfDay = decoder.readInt();
            return LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L);
        }, "time-micros", (decoder, schema) -> {
            long microsOfDay = decoder.readLong();
            return LocalTime.ofNanoOfDay(microsOfDay * 1_000L);
        }, "timestamp-millis", (decoder, schema) -> {
            long millisSinceEpoch = decoder.readLong();
            return Instant.ofEpochMilli(millisSinceEpoch);
        }, "timestamp-micros", (decoder, schema) -> {
            long microsSinceEpoch = decoder.readLong();
            return Instant.ofEpochSecond(microsSinceEpoch / 1_000_000, (microsSinceEpoch % 1_000_000) * 1_000);
        }, "local-timestamp-millis", (decoder, schema) -> {
            long millisSinceEpoch = decoder.readLong();
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(millisSinceEpoch), ZoneOffset.UTC);
        }, "local-timestamp-micros", (decoder, schema) -> {
            long microsSinceEpoch = decoder.readLong();
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(microsSinceEpoch / 1_000_000,
                    (microsSinceEpoch % 1_000_000) * 1_000), ZoneOffset.UTC);
        });
    }
}
