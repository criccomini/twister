package dev.twister.proto;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * A utility class to infer a Protocol Buffers message descriptor from a Map of objects.
 */
public class ProtoDescriptorInferrer {

    /**
     * Infers a Protocol Buffers message descriptor from a Map of objects and assigns it a message name.
     *
     * @param object The Map of objects to infer the descriptor from.
     * @param messageName The name to assign to the message in the descriptor.
     * @return The inferred Protocol Buffers message descriptor.
     * @throws RuntimeException If there is an error validating the inferred descriptor.
     */
    public Descriptors.Descriptor infer(Map<String, Object> object, String messageName) {
        DescriptorProtos.FileDescriptorProto.Builder fileDescriptorBuilder = DescriptorProtos.FileDescriptorProto.newBuilder();
        DescriptorProtos.DescriptorProto.Builder messageBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        int fieldNumber = 1;

        // Set message name
        messageBuilder.setName(messageName);

        for (Map.Entry<String, Object> entry : object.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder();

            // Set field name and number
            fieldBuilder.setName(fieldName);
            fieldBuilder.setNumber(fieldNumber++);

            if (fieldValue instanceof List) {
                fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
                // assuming all elements in the list are of the same type
                Descriptors.FieldDescriptor.Type fieldType = inferFieldType(((List<?>) fieldValue).get(0));
                fieldBuilder.setType(fieldType.toProto());
            } else if (fieldValue instanceof Map) {
                DescriptorProtos.DescriptorProto nestedMessage = infer((Map<String, Object>) fieldValue, messageName + "_" + fieldName).toProto();
                messageBuilder.addNestedType(nestedMessage);
                fieldBuilder.setTypeName(nestedMessage.getName());
                fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
            } else {
                fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
                Descriptors.FieldDescriptor.Type fieldType = inferFieldType(fieldValue);
                fieldBuilder.setType(fieldType.toProto());
            }

            // Add field to the message
            messageBuilder.addField(fieldBuilder.build());
        }

        // Add message to the file descriptor
        fileDescriptorBuilder.addMessageType(messageBuilder.build());

        try {
            Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorBuilder.build(), new Descriptors.FileDescriptor[0]);
            return fileDescriptor.findMessageTypeByName(messageName);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException("Error inferring descriptor: " + e.getMessage(), e);
        }
    }

    /**
     * Infers the Protocol Buffers field type from an Object.
     *
     * @param value The object to infer the field type from.
     * @return The inferred Protocol Buffers field type.
     * @throws IllegalArgumentException If the object type is unsupported.
     */
    private Descriptors.FieldDescriptor.Type inferFieldType(Object value) {
        if (value instanceof Integer) {
            return Descriptors.FieldDescriptor.Type.INT32;
        } else if (value instanceof Long) {
            return Descriptors.FieldDescriptor.Type.INT64;
        } else if (value instanceof BigInteger) {
            BigInteger bigInt = (BigInteger) value;
            if (bigInt.compareTo(BigInteger.ZERO) < 0 || bigInt.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                throw new IllegalArgumentException("BigInteger value does not fit in uint64");
            }
            return Descriptors.FieldDescriptor.Type.UINT64;
        } else if (value instanceof Boolean) {
            return Descriptors.FieldDescriptor.Type.BOOL;
        } else if (value instanceof String) {
            return Descriptors.FieldDescriptor.Type.STRING;
        } else if (value instanceof Double) {
            return Descriptors.FieldDescriptor.Type.DOUBLE;
        } else if (value instanceof ByteBuffer) {
            return Descriptors.FieldDescriptor.Type.BYTES;
        } else if (value instanceof Float) {
            return Descriptors.FieldDescriptor.Type.FLOAT;
        } else if (value instanceof Map) {
            return Descriptors.FieldDescriptor.Type.MESSAGE;
        } else {
            throw new IllegalArgumentException("Unsupported field value type: " + value.getClass().getName());
        }
    }
}
