package dev.twister.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescriptorToAvroJsonTest extends TestCase  {
    public void testSimpleMessage() throws Exception {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("SimpleMessage.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("SimpleMessage")
                .addField("optional", "int32", "int32_field", 1)
                .addField("optional", "int64", "int64_field", 2)
                .addField("optional", "float", "float_field", 3)
                .addField("optional", "double", "double_field", 4)
                .addField("optional", "bool", "boolean_field", 5)
                .addField("optional", "bytes", "bytes_field", 6)
                .addField("optional", "string", "string_field", 7)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();
        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("SimpleMessage");

        Map<String, Object> avroSchema;
        avroSchema = DescriptorToAvroJson.convert(descriptor);

        assertNotNull(avroSchema);
        assertEquals("record", avroSchema.get("type"));
        assertEquals("SimpleMessage", avroSchema.get("name"));

        List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");
        assertNotNull(fields);
        assertEquals(7, fields.size());

        Map<String, Object> int32Field = fields.get(0);
        assertNotNull(int32Field);
        assertEquals("int32_field", int32Field.get("name"));
        assertEquals("int", int32Field.get("type"));

        Map<String, Object> int64Field = fields.get(1);
        assertNotNull(int64Field);
        assertEquals("int64_field", int64Field.get("name"));
        assertEquals("long", int64Field.get("type"));

        Map<String, Object> floatField = fields.get(2);
        assertNotNull(floatField);
        assertEquals("float_field", floatField.get("name"));
        assertEquals("float", floatField.get("type"));

        Map<String, Object> doubleField = fields.get(3);
        assertNotNull(doubleField);
        assertEquals("double_field", doubleField.get("name"));
        assertEquals("double", doubleField.get("type"));

        Map<String, Object> booleanField = fields.get(4);
        assertNotNull(booleanField);
        assertEquals("boolean_field", booleanField.get("name"));
        assertEquals("boolean", booleanField.get("type"));

        Map<String, Object> bytesField = fields.get(5);
        assertNotNull(bytesField);
        assertEquals("bytes_field", bytesField.get("name"));
        assertEquals("bytes", bytesField.get("type"));

        Map<String, Object> stringField = fields.get(6);
        assertNotNull(stringField);
        assertEquals("string_field", stringField.get("name"));
        assertEquals("string", stringField.get("type"));
    }

    public void testUnsignedAndFixedTypes() throws Exception {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("UnsignedAndFixed.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("UnsignedAndFixed")
                .addField("optional", "uint32", "unsignedInt32", 1)
                .addField("optional", "uint64", "unsignedInt64", 2)
                .addField("optional", "fixed32", "fixedInt32", 3)
                .addField("optional", "fixed64", "fixedInt64", 4)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();
        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("UnsignedAndFixed");

        Map<String, Object> avroSchema;
        avroSchema = DescriptorToAvroJson.convert(descriptor);

        Map<String, Object> decimalType = new HashMap<>();
        decimalType.put("type", "bytes");
        decimalType.put("logicalType", "decimal");
        decimalType.put("precision", 20);
        decimalType.put("scale", 0);

        assertNotNull(avroSchema);
        assertEquals("record", avroSchema.get("type"));
        assertEquals("UnsignedAndFixed", avroSchema.get("name"));

        List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");
        assertNotNull(fields);
        assertEquals(4, fields.size());

        Map<String, Object> uint32Field = fields.get(0);
        assertNotNull(uint32Field);
        assertEquals("unsignedInt32", uint32Field.get("name"));
        assertEquals("long", uint32Field.get("type"));

        Map<String, Object> uint64Field = fields.get(1);
        assertNotNull(uint64Field);
        assertEquals("unsignedInt64", uint64Field.get("name"));
        assertEquals(decimalType, uint64Field.get("type"));

        Map<String, Object> fixed32Field = fields.get(2);
        assertNotNull(fixed32Field);
        assertEquals("fixedInt32", fixed32Field.get("name"));
        assertEquals("long", fixed32Field.get("type"));

        Map<String, Object> fixed64Field = fields.get(3);
        assertNotNull(fixed64Field);
        assertEquals("fixedInt64", fixed64Field.get("name"));
        assertEquals(decimalType, fixed64Field.get("type"));
    }

    public void testEnumConversion() throws Exception {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("TestEnum.proto");

        EnumDefinition enumDef = EnumDefinition.newBuilder("TestEnum")
                .addValue("VALUE_1", 1)
                .addValue("VALUE_2", 2)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("optional", "TestEnum", "enum_field", 1)
                .build();

        schemaBuilder.addEnumDefinition(enumDef);
        schemaBuilder.addMessageDefinition(msgDef);

        DynamicSchema schema = schemaBuilder.build();
        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("TestMessage");

        Map<String, Object> avroSchema = DescriptorToAvroJson.convert(descriptor);

        assertNotNull(avroSchema);
        assertEquals("record", avroSchema.get("type"));
        assertEquals("TestMessage", avroSchema.get("name"));

        List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");
        assertNotNull(fields);
        assertEquals(1, fields.size());

        Map<String, Object> field = fields.get(0);
        assertNotNull(field);
        assertEquals("enum_field", field.get("name"));

        Map<String, Object> avroType = (Map<String, Object>) field.get("type");
        assertEquals("enum", avroType.get("type"));
        assertEquals("TestEnum", avroType.get("name"));

        List<String> symbols = (List<String>) avroType.get("symbols");
        assertNotNull(symbols);
        assertEquals(2, symbols.size());
        assertTrue(symbols.contains("VALUE_1"));
        assertTrue(symbols.contains("VALUE_2"));
    }

    public void testNestedMessageConversion() throws Exception {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("TestNestedMessage.proto");

        MessageDefinition nestedMsgDef = MessageDefinition.newBuilder("NestedMessage")
                .addField("optional", "int32", "nested_field", 1)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("optional", "NestedMessage", "nested_message_field", 1)
                .build();

        schemaBuilder.addMessageDefinition(nestedMsgDef);
        schemaBuilder.addMessageDefinition(msgDef);

        DynamicSchema schema = schemaBuilder.build();
        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("TestMessage");

        Map<String, Object> avroSchema = DescriptorToAvroJson.convert(descriptor);

        assertNotNull(avroSchema);
        assertEquals("record", avroSchema.get("type"));
        assertEquals("TestMessage", avroSchema.get("name"));

        List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");
        assertNotNull(fields);
        assertEquals(1, fields.size());

        Map<String, Object> field = fields.get(0);
        assertNotNull(field);
        assertEquals("nested_message_field", field.get("name"));

        Map<String, Object> avroType = (Map<String, Object>) field.get("type");
        assertEquals("record", avroType.get("type"));
        assertEquals("NestedMessage", avroType.get("name"));

        List<Map<String, Object>> nestedFields = (List<Map<String, Object>>) avroType.get("fields");
        assertNotNull(nestedFields);
        assertEquals(1, nestedFields.size());

        Map<String, Object> nestedField = nestedFields.get(0);
        assertNotNull(nestedField);
        assertEquals("nested_field", nestedField.get("name"));
        assertEquals("int", nestedField.get("type"));
    }

    public void testSelfReferencingMessage() throws Exception {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("SelfReferencingMessage.proto");

        MessageDefinition nodeMsgDef = MessageDefinition.newBuilder("Node")
                .addField("optional", "string", "name", 1)
                .addField("repeated", "Node", "children", 2)
                .build();

        schemaBuilder.addMessageDefinition(nodeMsgDef);
        DynamicSchema schema = schemaBuilder.build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("Node");
        Map<String, Object> avroSchema = DescriptorToAvroJson.convert(descriptor);

        assertNotNull(avroSchema);
        assertEquals("record", avroSchema.get("type"));
        assertEquals("Node", avroSchema.get("name"));

        List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");
        assertNotNull(fields);
        assertEquals(2, fields.size());

        Map<String, Object> nameField = fields.get(0);
        assertNotNull(nameField);
        assertEquals("name", nameField.get("name"));
        assertEquals("string", nameField.get("type"));

        Map<String, Object> childrenField = fields.get(1);
        assertNotNull(childrenField);
        assertEquals("children", childrenField.get("name"));

        Map<String, Object> childrenType = new HashMap<>();
        childrenType.put("type", "Node");

        assertEquals(childrenType, childrenField.get("type"));
    }

    public void testOneOfMessage() throws Exception {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("OneOfMessage.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("OneOfMessage")
                .addField("optional", "string", "name", 1)
                .addOneof("choice")
                .addField("int32", "int_choice", 2)
                .addField("string", "str_choice", 3)
                .msgDefBuilder()
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("OneOfMessage");
        Map<String, Object> avroSchema = DescriptorToAvroJson.convert(descriptor);
        System.err.println(avroSchema);

        assertNotNull(avroSchema);
        assertEquals("record", avroSchema.get("type"));
        assertEquals("OneOfMessage", avroSchema.get("name"));

        List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");
        assertNotNull(fields);
        assertEquals(2, fields.size());

        Map<String, Object> nameField = fields.get(0);
        assertNotNull(nameField);
        assertEquals("name", nameField.get("name"));
        assertEquals("string", nameField.get("type"));

        Map<String, Object> choiceField = fields.get(1);
        assertNotNull(choiceField);
        assertEquals("choice", choiceField.get("name"));

        List<Object> unionTypes = (List<Object>) choiceField.get("type");
        assertNotNull(unionTypes);
        assertEquals(2, unionTypes.size());

        boolean intChoiceFound = false;
        boolean strChoiceFound = false;

        for (Object unionType : unionTypes) {
            Map<String, Object> field = (Map<String, Object>) unionType;
            String fieldName = (String) field.get("name");

            if ("int_choice".equals(fieldName)) {
                assertEquals("int", field.get("type"));
                intChoiceFound = true;
            } else if ("str_choice".equals(fieldName)) {
                assertEquals("string", field.get("type"));
                strChoiceFound = true;
            }
        }

        assertTrue(intChoiceFound);
        assertTrue(strChoiceFound);
    }
}
