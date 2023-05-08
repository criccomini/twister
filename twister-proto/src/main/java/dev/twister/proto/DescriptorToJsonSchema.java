package dev.twister.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.*;
import java.util.stream.Collectors;

public class DescriptorToJsonSchema {
    private static final Map<String, Map<String, Object>> visitedMessages = new HashMap<>();
    private static final Set<String> referencedMessages = new HashSet<>();

    public static Map<String, Object> convert(Descriptor descriptor) {
        visitedMessages.clear();
        Map<String, Object> schema = new HashMap<>(convertMessage(descriptor));
        Map<String, Object> referencedMessageDefs = visitedMessages
                .entrySet()
                .stream()
                .filter(e -> referencedMessages.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (referencedMessageDefs.size() > 0) {
            schema.put("$defs", referencedMessageDefs);
        }
        return schema;
    }

    private static Map<String, Object> convertMessage(Descriptor descriptor) {
        String messageName = descriptor.getFullName();

        if (visitedMessages.containsKey(messageName)) {
            Map<String, Object> ref = new HashMap<>();
            ref.put("$ref", "#/$defs/" + messageName);
            referencedMessages.add(messageName);
            return ref;
        }

        visitedMessages.put(messageName, new HashMap<>());

        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "object");
        schema.put("properties", buildProperties(descriptor));

        List<String> required = buildRequiredFields(descriptor);
        if (required.size() > 0) {
            schema.put("required", required);
        }

        visitedMessages.put(messageName, schema);

        return schema;
    }

    private static Map<String, Object> buildProperties(Descriptor descriptor) {
        Map<String, Object> properties = new HashMap<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            if (field.getContainingOneof() == null) {
                properties.put(field.getName(), buildProperty(field));
            }
        }

        // Handle oneof fields
        properties.putAll(buildOneOfFields(descriptor));

        return properties;
    }

    private static Map<String, Object> buildOneOfFields(Descriptor descriptor) {
        Map<String, Object> oneOfProperties = new HashMap<>();
        for (Descriptors.OneofDescriptor oneof : descriptor.getOneofs()) {
            List<Map<String, Object>> oneOfFields = new ArrayList<>();
            for (FieldDescriptor field : oneof.getFields()) {
                oneOfFields.add(Collections.singletonMap(field.getName(), buildProperty(field)));
            }
            oneOfProperties.put(oneof.getName(), Collections.singletonMap("anyOf", oneOfFields));
        }
        return oneOfProperties;
    }

    private static List<String> buildRequiredFields(Descriptor descriptor) {
        List<String> requiredFields = new ArrayList<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            if (field.isRequired()) {
                requiredFields.add(field.getName());
            }
        }
        return requiredFields;
    }

    private static Map<String, Object> buildProperty(FieldDescriptor field) {
        Map<String, Object> property = new HashMap<>();

        // Check if the field is a map entry
        if (field.isMapField()) {
            Map<String, Object> keySchema = buildProperty(field.getMessageType().findFieldByNumber(1));
            Map<String, Object> valueSchema = buildProperty(field.getMessageType().findFieldByNumber(2));

            if ("string".equals(keySchema.get("type"))) {
                property.put("type", "object");
                property.put("additionalProperties", valueSchema);
            } else {
                throw new UnsupportedOperationException("Protobuf map keys must be of type string");
            }

            return property;
        }

        switch (field.getType()) {
            case BOOL:
                property.put("type", "boolean");
                break;
            case STRING:
                property.put("type", "string");
                break;
            case BYTES:
                property.put("type", "string");
                property.put("format", "byte");
                break;
            case FLOAT:
            case DOUBLE:
                property.put("type", "number");
                break;
            case INT32:
            case SINT32:
            case SFIXED32:
            case UINT32:
            case FIXED32:
                property.put("type", "integer");
                property.put("format", "int32");
                break;
            case INT64:
            case SINT64:
            case SFIXED64:
            case UINT64:
            case FIXED64:
                property.put("type", "integer");
                property.put("format", "int64");
                break;
            case ENUM:
                property.put("type", "string");
                property.put("enum", buildEnum(field));
                break;
            case MESSAGE:
                property.putAll(convertMessage(field.getMessageType()));
                break;
            case GROUP:
                // Group is deprecated and should not be used
                throw new UnsupportedOperationException("Group type is not supported");
        }

        if (field.isRepeated()) {
            Map<String, Object> arrayProperty = new HashMap<>();
            arrayProperty.put("type", "array");
            arrayProperty.put("items", property);
            return arrayProperty;
        }

        return property;
    }

    private static List<String> buildEnum(FieldDescriptor field) {
        List<String> enumValues = new ArrayList<>();
        for (Descriptors.EnumValueDescriptor enumValue : field.getEnumType().getValues()) {
            enumValues.add(enumValue.getName());
        }
        return enumValues;
    }
}
