package dev.twister.proto;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ProtoDescriptorInferrer {
    public Descriptors.Descriptor descriptor(Map<String, Object> object, String messageName) {
        DescriptorProtos.FileDescriptorProto.Builder fileDescriptorBuilder = DescriptorProtos.FileDescriptorProto.newBuilder();
        DescriptorProtos.DescriptorProto.Builder messageBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        int fieldNumber = 1;

        // Set message name
        messageBuilder.setName(messageName);

        for (Map.Entry<String, Object> entry : object.entrySet()) {
            Descriptors.FieldDescriptor.Type fieldType;
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder();

            // Set field name and type
            fieldBuilder.setName(fieldName);
            fieldBuilder.setNumber(fieldNumber++);

            if (fieldValue instanceof List) {
                fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
                // assuming all elements in the list are of the same type
                fieldType = inferFieldType(((List<?>) fieldValue).get(0));
            } else {
                fieldBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
                fieldType = inferFieldType(fieldValue);
            }

            fieldBuilder.setType(fieldType.toProto());

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
        } else {
            throw new IllegalArgumentException("Unsupported field value type: " + value.getClass().getName());
        }
    }
}
