package dev.twister.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This class provides functionality to wrap Avro IndexedRecord objects
 * into a Map representation for easier data manipulation.
 */
public class AvroWrapper {

    private static final Map<String, LogicalTypeConverter> DEFAULT_LOGICAL_TYPE_CONVERTERS;

    private final Map<String, LogicalTypeConverter> logicalTypeConverters;

    public AvroWrapper() {
        this(DEFAULT_LOGICAL_TYPE_CONVERTERS);
    }

    public AvroWrapper(Map<String, LogicalTypeConverter> logicalTypeConverters) {
        this.logicalTypeConverters = logicalTypeConverters;
    }

    /**
     * Wraps the given IndexedRecord into a Map.
     *
     * @param record the IndexedRecord to be wrapped
     * @return a Map representing the IndexedRecord
     */
    public Map<String, Object> wrap(IndexedRecord record) {
        return new Facade(record);
    }

    /**
     * This method coerces the value into the correct Java type based on the Avro schema.
     * It supports Avro primitives, as well as complex types like records (which it wraps
     * with Facade), arrays (which it wraps with FacadeList), and maps (which it wraps with FacadeMap).
     *
     * @param schema the Avro schema of the value
     * @param value the value to be coerced
     * @return the coerced value, or throws IllegalArgumentException if the Avro type is unsupported
     */
    private Object coerceType(Schema schema, Object value) {
        if (schema.getLogicalType() != null) {
            LogicalTypeConverter converter = logicalTypeConverters.get(schema.getLogicalType().getName());
            if (converter != null) {
                return converter.convert(value);
            }
        }

        switch (schema.getType()) {
            case RECORD:
                return new Facade((IndexedRecord) value);
            case ENUM:
                return value.toString();
            case UNION:
                return coerceType(schema.getTypes().get(0), value);
            case FIXED:
                return ByteBuffer.wrap(((GenericFixed) value).bytes());
            case ARRAY:
                return new FacadeList(schema.getElementType(), (List<?>) value);
            case MAP:
                return new FacadeMap(schema.getValueType(), (Map<String, Object>) value);
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
                return value;
            case BYTES:
                if (value instanceof ByteBuffer) {
                    return value;
                } else if (value instanceof byte[]) {
                    return ByteBuffer.wrap((byte[]) value);
                } else {
                    throw new IllegalArgumentException("Unsupported type for BYTES: " + value.getClass());
                }
            case NULL:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported type: " + schema.getType());
        }
    }

    /**
     * Facade is a private class that provides a Map view of a given IndexedRecord.
     * This facilitates easier manipulation of the IndexedRecord's data.
     * The map's keys are the field names in the IndexedRecord, and the values are the field values,
     * which are coerced to appropriate Java types using the coerceType method.
     */
    private class Facade extends AbstractMap<String, Object> {

        private final IndexedRecord record;

        Facade(IndexedRecord record) {
            this.record = record;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {

                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        private final Iterator<Schema.Field> iterator = record.getSchema().getFields().iterator();

                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Schema.Field field = iterator.next();
                            return new SimpleImmutableEntry<>(field.name(), coerceType(field.schema(),
                                    record.get(field.pos())));
                        }
                    };
                }

                @Override
                public int size() {
                    return record.getSchema().getFields().size();
                }
            };
        }
    }

    /**
     * FacadeList is a private class that provides a List view of a given Avro array.
     * This facilitates easier manipulation of the array's data.
     * The list's elements are the array items, which are coerced to appropriate Java types
     * using the coerceType method.
     */
    private class FacadeList extends AbstractList<Object> {

        private final Schema elementSchema;
        private final List<?> list;

        /**
         * Creates a new FacadeList object that provides a List view of the given Avro array.
         * The List's elements will be the array items, coerced to appropriate Java types
         * using the coerceType method.
         *
         * @param elementSchema the Avro schema of the elements in the array
         * @param list the actual list of elements, which will be coerced to appropriate Java types
         */
        FacadeList(Schema elementSchema, List<?> list) {
            this.elementSchema = elementSchema;
            this.list = list;
        }

        @Override
        public Object get(int index) {
            return coerceType(elementSchema, list.get(index));
        }

        @Override
        public int size() {
            return list.size();
        }
    }

    /**
     * FacadeMap is a private class that provides a Map view of a given Avro map.
     * This facilitates easier manipulation of the map's data.
     * The map's keys are the Avro map's keys, and the values are the map values,
     * which are coerced to appropriate Java types using the coerceType method.
     */
    private class FacadeMap extends AbstractMap<String, Object> {

        private final Schema valueSchema;
        private final Map<String, Object> map;

        /**
         * Creates a new FacadeMap object that provides a Map view of the given Avro map.
         * The Map's values will be the map values, coerced to appropriate Java types
         * using the coerceType method.
         *
         * @param valueSchema the Avro schema of the values in the map
         * @param map the actual map of keys to values, which will be coerced to appropriate Java types
         */
        FacadeMap(Schema valueSchema, Map<String, Object> map) {
            this.valueSchema = valueSchema;
            this.map = map;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        private final Iterator<Entry<String, Object>> iterator = map.entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            Entry<String, Object> entry = iterator.next();
                            return new SimpleImmutableEntry<>(entry.getKey(), coerceType(valueSchema,
                                    entry.getValue()));
                        }
                    };
                }

                @Override
                public int size() {
                    return map.size();
                }
            };
        }
    }

    public interface LogicalTypeConverter {
        Object convert(Object value);
    }

    static {
        DEFAULT_LOGICAL_TYPE_CONVERTERS = Map.of(
                "decimal", value -> {
                    GenericData.Fixed fixed = (GenericData.Fixed) value;
                    byte[] bytes = fixed.bytes();
                    int scale = ((LogicalTypes.Decimal) fixed.getSchema().getLogicalType()).getScale();
                    byte[] valueBytes = Arrays.copyOfRange(bytes, 0, bytes.length);
                    return new BigDecimal(new BigInteger(valueBytes), scale);
                },
                "uuid", value -> UUID.fromString((String) value),
                "date", value -> LocalDate.ofEpochDay((int) value),
                "time-millis", value -> LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos((int) value)),
                "time-micros", value -> LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((long) value)),
                "timestamp-millis", value -> Instant.ofEpochMilli((long) value),
                "timestamp-micros", value -> Instant.ofEpochSecond(0, (long) value * 1000),
                "local-timestamp-millis", value -> {
                    long longValue = (long) value;
                    long seconds = longValue / 1000;
                    int nanos = (int) (longValue % 1000) * 1000000;
                    return LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC);
                },
                "local-timestamp-micros", value -> {
                    long longValue = (long) value;
                    long seconds = longValue / 1000000;
                    int nanos = (int) (longValue % 1000000) * 1000;
                    return LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC);
                }
        );
    }

}
