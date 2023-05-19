package dev.twister.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.math.BigInteger;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Wraps a Protobuf message into a Java map, providing a convenient way to access the fields of
 * the message using standard Java methods. This can be useful when you need to work with Protobuf
 * messages but don't have access to the generated Java classes for them.
 * <p>
 * The keys of the map are the names of the Protobuf fields. The values are the field values,
 * converted to more Java-friendly types when necessary (for example, byte fields are converted to
 * ByteBuffer instances, and enum fields are converted to the names of the enum values).
 * <p>
 * Nested messages and repeated fields are also supported. Nested messages are themselves wrapped
 * into Facade instances, and repeated fields are wrapped into FacadeList instances. This means that
 * you can navigate the entire structure of a Protobuf message using just the standard Map and List
 * interfaces.
 */
public class ProtoWrapper {
    public Map<String, Object> wrap(Message message) {
        return new Facade(message);
    }

    /**
     * Converts a Protobuf field value to a more Java-friendly form.
     *
     * <p>The method supports various types of fields, including repeated fields and nested messages.
     * Scalar types (like integers, strings, booleans, etc.) are converted as-is. Some other types
     * (like enum values or bytes) are converted to more convenient or idiomatic Java types. Repeated
     * fields are wrapped into a {@link FacadeList} instance, and nested messages are wrapped into a
     * {@link Facade} instance.
     *
     * <p>The treatment of repeated fields is special. When a repeated field is first encountered,
     * it's wrapped into a {@link FacadeList}. The {@code isRepeated} argument should be true in this
     * case. The {@link FacadeList} will later call this method for individual elements of the list,
     * and in this case {@code isRepeated} should be false, because those individual elements are not
     * repeated fields themselves.
     *
     * @param field the field descriptor
     * @param value the field value
     * @param isRepeated whether the field is a repeated field
     * @return the converted value
     * @throws IllegalArgumentException if the field type is unsupported
     */
    private Object convertValue(Descriptors.FieldDescriptor field, Object value, boolean isRepeated) {
        if (isRepeated) {
            return new FacadeList(field, (List<?>) value);
        } else if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
            return new Facade((Message) value);
        } else {
            switch (field.getType()) {
                case INT32:
                case SINT32:
                case SFIXED32:
                case INT64:
                case SINT64:
                case SFIXED64:
                case BOOL:
                case STRING:
                case DOUBLE:
                case FLOAT:
                    return value;
                case UINT32:
                case FIXED32:
                    return ((Integer) value).longValue() & 0xFFFFFFFFL;
                case UINT64:
                case FIXED64:
                    return BigInteger.valueOf((Long) value).and(BigInteger.valueOf(Long.MAX_VALUE)).setBit(63);
                case ENUM:
                    return ((Descriptors.EnumValueDescriptor) value).getName();
                case BYTES:
                    return ((ByteString) value).asReadOnlyByteBuffer();
                default:
                    throw new IllegalArgumentException("Unsupported type: " + field.getType());
            }
        }
    }

    /**
     * Provides a view of a Protobuf message as a Java Map. The map keys are the names of the fields
     * in the Protobuf message, and the map values are the corresponding field values, converted to
     * Java-friendly types where necessary.
     * <p>
     * This class supports all Protobuf field types, including nested messages and repeated fields.
     * Nested messages are wrapped into Facade instances, and repeated fields are wrapped into
     * FacadeList instances. This means that you can navigate the entire structure of a Protobuf
     * message using just the standard Map and List interfaces.
     * <p>
     * This class also takes into account 'oneof' fields. Only the field that is currently set in
     * the 'oneof' is included in the map.
     */
    private class Facade extends AbstractMap<String, Object> {

        private final Message message;
        private final List<Descriptors.FieldDescriptor> allFields;
        private int size;

        /**
         * Constructor to create Facade wrapping the protobuf message.
         *
         * @param message the protobuf message
         */
        Facade(Message message) {
            this.message = message;
            this.allFields = message.getDescriptorForType().getFields();
            this.size = calculateSize();
        }

        /**
         * Calculates the size (the number of set fields), taking 'oneof' fields into account.
         *
         * @return the size
         */
        private int calculateSize() {
            int size = 0;
            for (Descriptors.FieldDescriptor field : allFields) {
                if (field.getContainingOneof() != null) {
                    if (message.getOneofFieldDescriptor(field.getContainingOneof()) == field) {
                        size++;  // Count the field if it's the one currently set in the 'oneof'.
                    }
                } else {
                    size++;  // Regular field, always count it.
                }
            }
            return size;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {
                /**
                 * This iterator iterates over both regular and 'oneof' fields of the Protobuf message, maintaining the
                 * order of fields as they are defined in the proto file. If a 'oneof' field is currently set, it is
                 * returned when its turn comes according to its order in the proto file.
                 */
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        private int index = 0;  // Current field index.

                        /**
                         * Checks if there is a next field. This method also prepares the next field to be returned,
                         * taking 'oneof' fields into account.
                         */
                        @Override
                        public boolean hasNext() {
                            return index < allFields.size();
                        }

                        /**
                         * Returns the next field. If a 'oneof' field is set and its turn comes, it is returned.
                         */
                        @Override
                        public Entry<String, Object> next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }

                            Descriptors.FieldDescriptor field = allFields.get(index);

                            // If the field is part of a `oneof`, and it is not the one currently set in the `oneof`,
                            // continue to the next field.
                            while (field.getContainingOneof() != null
                                    && field != message.getOneofFieldDescriptor(field.getContainingOneof())) {
                                if (++index >= allFields.size()) {
                                    throw new NoSuchElementException();
                                }
                                field = allFields.get(index);
                            }

                            Object value = message.getField(field);
                            index++;
                            return new SimpleImmutableEntry<>(field.getName(),
                                    convertValue(field, value, field.isRepeated()));
                        }
                    };
                }

                /**
                 * Returns the number of set fields in the Protobuf message. This includes the set field of any 'oneof'
                 * field group, if any.
                 */
                @Override
                public int size() {
                    return size;
                }
            };
        }
    }

    /**
     * Provides a view of a Protobuf repeated field as a Java List. The list elements are the
     * values of the repeated field, converted to Java-friendly types where necessary.
     * <p>
     * This class supports all Protobuf field types, including nested messages. Nested messages are
     * wrapped into Facade instances. This means that you can navigate the entire structure of a
     * Protobuf message using just the standard Map and List interfaces.
     */
    private class FacadeList extends AbstractList<Object> {

        private final Descriptors.FieldDescriptor field;
        private final List<?> list;

        FacadeList(Descriptors.FieldDescriptor field, List<?> list) {
            this.field = field;
            this.list = list;
        }

        @Override
        public Object get(int index) {
            // Pass `false` for the `isRepeated` parameter to handle individual elements correctly
            return convertValue(field, list.get(index), false);
        }

        @Override
        public int size() {
            return list.size();
        }
    }
}
