package dev.twister.proto;

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A utility class for converting a Map into a Protocol Buffers message and writing it to a ByteBuffer.
 */
public class ProtoWriter {

    /**
     * Writes a given object map into a ByteBuffer using the specified message name.
     * It uses a new instance of ProtoDescriptorInferrer to generate the descriptor.
     *
     * @param object The Map representing the Protocol Buffers message.
     * @param messageName The name of the Protocol Buffers message.
     * @return A ByteBuffer containing the Protocol Buffers message.
     */
    public ByteBuffer write(Map<String, Object> object, String messageName) {
        return write(object, new ProtoDescriptorInferrer().infer(object, messageName));
    }

    /**
     * Converts a Map into a Protocol Buffers message and writes it to a ByteBuffer.
     *
     * @param object The Map representing the Protocol Buffers message.
     * @param descriptor The Descriptor of the Protocol Buffers message.
     * @return A ByteBuffer containing the Protocol Buffers message.
     * @throws RuntimeException If an unsupported field type is encountered.
     */
    public ByteBuffer write(Map<String, Object> object, Descriptor descriptor) {
        DynamicMessage message = toMessage(object, descriptor);
        return ByteBuffer.wrap(message.toByteArray());
    }

    /**
     * Converts a Map into a DynamicMessage representing a Protocol Buffers message.
     *
     * @param object The Map representing the Protocol Buffers message.
     * @param descriptor The Descriptor of the Protocol Buffers message.
     * @return A DynamicMessage representing the Protocol Buffers message.
     * @throws RuntimeException If an unsupported field type is encountered.
     */
    public DynamicMessage toMessage(Map<String, Object> object, Descriptor descriptor) {
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);

        for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            String fieldName = fieldDescriptor.getName();
            Object value = object.get(fieldName);

            if (value != null) {
                if (fieldDescriptor.isRepeated()) {
                    for (Object element : (Iterable<?>) value) {
                        messageBuilder.addRepeatedField(fieldDescriptor, toProtobufValue(fieldDescriptor, element));
                    }
                } else {
                    messageBuilder.setField(fieldDescriptor, toProtobufValue(fieldDescriptor, value));
                }
            }
        }

        return messageBuilder.build();
    }

    /**
     * Converts a Java object into a Protocol Buffers value according to the provided field descriptor.
     *
     * @param fieldDescriptor The descriptor of the field.
     * @param value The Java object to convert.
     * @return The converted Protocol Buffers value.
     * @throws RuntimeException If an unsupported field type is encountered.
     */
    private Object toProtobufValue(FieldDescriptor fieldDescriptor, Object value) {
        switch (fieldDescriptor.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
            case INT64:
            case SINT64:
            case SFIXED64:
            case BOOL:
            case FLOAT:
            case STRING:
            case DOUBLE:
                return value;
            case ENUM:
                EnumDescriptor enumDescriptor = fieldDescriptor.getEnumType();
                return enumDescriptor.findValueByName((String) value);
            case FIXED64:
            case UINT64:
                BigInteger bigInt = (BigInteger) value;
                return bigInt.longValue();
            case BYTES:
                ByteBuffer byteBuffer = (ByteBuffer) value;
                return ByteString.copyFrom(byteBuffer);
            case FIXED32:
            case UINT32:
                return ((Long) value).intValue();
            case MESSAGE:
                Descriptor messageDescriptor = fieldDescriptor.getMessageType();
                return toMessage((Map<String, Object>) value, messageDescriptor);
            default:
                throw new RuntimeException("Unsupported field type: " + fieldDescriptor.getType());
        }
    }
}
