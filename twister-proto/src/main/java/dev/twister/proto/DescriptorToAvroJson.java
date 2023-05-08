package dev.twister.proto;

import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DescriptorToAvroJson {
    public static Map<String, Object> convert(Descriptors.Descriptor descriptor) {
        return descriptorToAvro(descriptor, new HashMap<>());
    }

    private static Map<String, Object> descriptorToAvro(Descriptors.Descriptor descriptor, Map<String, Map<String, Object>> processedMessages) {
        if (processedMessages.containsKey(descriptor.getFullName())) {
            Map<String, Object> ref = new HashMap<>();
            ref.put("type", descriptor.getFullName());
            return ref;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("type", "record");
        result.put("name", descriptor.getName());
        if (descriptor.getFile().getPackage() != null && descriptor.getFile().getPackage().length() > 0) {
            result.put("namespace", descriptor.getFile().getPackage());
        }
        processedMessages.put(descriptor.getFullName(), result);

        List<Map<String, Object>> fields = new ArrayList<>();
        for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            if (fieldDescriptor.getContainingOneof() == null) {
                fields.add(fieldDescriptorToAvro(fieldDescriptor, processedMessages));
            }
        }
        result.put("fields", fields);

        handleOneofs(descriptor, fields, processedMessages);

        return result;
    }

    private static void handleOneofs(Descriptors.Descriptor descriptor, List<Map<String, Object>> fields, Map<String, Map<String, Object>> processedMessages) {
        for (Descriptors.OneofDescriptor oneofDescriptor : descriptor.getOneofs()) {
            List<Object> unionTypes = new ArrayList<>();

            oneofDescriptor.getFields().forEach(fieldDescriptor -> {
                Object avroType = protobufTypeToAvroType(fieldDescriptor, processedMessages);
                if (avroType != null) {
                    unionTypes.add(new HashMap<String, Object>() {{
                        put("name", fieldDescriptor.getName());
                        put("type", avroType);
                    }});
                }
            });

            fields.add(new HashMap<String, Object>() {{
                put("name", oneofDescriptor.getName());
                put("type", unionTypes);
            }});
        }
    }

    private static Map<String, Object> fieldDescriptorToAvro(Descriptors.FieldDescriptor fieldDescriptor, Map<String, Map<String, Object>> processedMessages) {
        Map<String, Object> field = new HashMap<>();
        field.put("name", fieldDescriptor.getName());
        Object avroType = protobufTypeToAvroType(fieldDescriptor, processedMessages);
        if (avroType == null) {
            throw new UnsupportedOperationException("Unsupported Protobuf type: " + fieldDescriptor.getType());
        }
        field.put("type", avroType);
        if (fieldDescriptor.hasDefaultValue()) {
            field.put("default", fieldDescriptor.getDefaultValue());
        }
        return field;
    }

    private static Object protobufTypeToAvroType(Descriptors.FieldDescriptor fieldDescriptor, Map<String, Map<String, Object>> processedMessages) {
        Descriptors.FieldDescriptor.Type protobufType = fieldDescriptor.getType();
        switch (protobufType) {
            case BOOL:
                return "boolean";
            case DOUBLE:
                return "double";
            case FLOAT:
                return "float";
            case INT32:
            case SINT32:
            case SFIXED32:
                return "int";
            case FIXED32:
            case UINT32:
            case INT64:
            case SINT64:
            case SFIXED64:
                return "long";
            case FIXED64:
            case UINT64:
                Map<String, Object> decimalType = new HashMap<>();
                decimalType.put("type", "bytes");
                decimalType.put("logicalType", "decimal");
                decimalType.put("precision", 20);
                decimalType.put("scale", 0);
                return decimalType;
            case STRING:
                return "string";
            case BYTES:
                return "bytes";
            case ENUM:
                return enumTypeToAvro(fieldDescriptor.getEnumType());
            case MESSAGE:
                return descriptorToAvro(fieldDescriptor.getMessageType(), processedMessages);
            case GROUP:
                throw new UnsupportedOperationException("Group type is not supported");
            default:
                throw new UnsupportedOperationException("Unknown type: " + protobufType);
        }
    }

    private static Map<String, Object> enumTypeToAvro(Descriptors.EnumDescriptor enumDescriptor) {
        Map<String, Object> result = new HashMap<>();
        result.put("type", "enum");
        result.put("name", enumDescriptor.getName());
        List<String> symbols = new ArrayList<>();
        for (Descriptors.EnumValueDescriptor valueDescriptor : enumDescriptor.getValues()) {
            symbols.add(valueDescriptor.getName());
        }
        result.put("symbols", symbols);
        return result;
    }
}