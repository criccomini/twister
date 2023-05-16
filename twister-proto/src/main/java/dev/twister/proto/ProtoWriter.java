package dev.twister.proto;

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;

public class ProtoWriter {
    public ByteBuffer write(Map<String, Object> object, Descriptor descriptor) {
        DynamicMessage message = toMessage(object, descriptor);
        return ByteBuffer.wrap(message.toByteArray());
    }

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
