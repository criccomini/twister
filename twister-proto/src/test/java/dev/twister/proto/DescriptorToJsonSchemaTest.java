package dev.twister.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescriptorToJsonSchemaTest extends TestCase {
    public void testSimpleMessage() throws Exception {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("SimpleMessage.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("SimpleMessage")
                .addField("optional", "int32", "id", 1)
                .addField("optional", "string", "name", 2)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();
        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("SimpleMessage");

        Map<String, Object> jsonSchema = DescriptorToJsonSchema.convert(descriptor);

        assertNotNull(jsonSchema);
        assertEquals("object", jsonSchema.get("type"));

        Map<String, Object> properties = (Map<String, Object>) jsonSchema.get("properties");
        assertNotNull(properties);
        assertEquals(2, properties.size());

        Map<String, Object> idField = (Map<String, Object>) properties.get("id");
        assertNotNull(idField);
        assertEquals("integer", idField.get("type"));

        Map<String, Object> nameField = (Map<String, Object>) properties.get("name");
        assertNotNull(nameField);
        assertEquals("string", nameField.get("type"));
    }

    public void testNestedMessage() throws Exception {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("NestedMessage.proto");

        MessageDefinition nestedMsgDef = MessageDefinition.newBuilder("Address")
                .addField("optional", "string", "street", 1)
                .addField("optional", "string", "city", 2)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("Person")
                .addField("optional", "string", "name", 1)
                .addField("optional", "Address", "address", 2)
                .build();

        schemaBuilder.addMessageDefinition(nestedMsgDef);
        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("Person");
        Map<String, Object> jsonSchema = DescriptorToJsonSchema.convert(descriptor);

        assertNotNull(jsonSchema);
        assertEquals("object", jsonSchema.get("type"));

        Map<String, Object> properties = (Map<String, Object>) jsonSchema.get("properties");
        assertNotNull(properties);
        assertEquals(2, properties.size());

        Map<String, Object> nameField = (Map<String, Object>) properties.get("name");
        assertNotNull(nameField);
        assertEquals("string", nameField.get("type"));

        Map<String, Object> addressField = (Map<String, Object>) properties.get("address");
        assertNotNull(addressField);
        assertEquals("object", addressField.get("type"));

        Map<String, Object> nestedProperties = (Map<String, Object>) addressField.get("properties");
        assertNotNull(nestedProperties);
        assertEquals(2, nestedProperties.size());

        Map<String, Object> streetField = (Map<String, Object>) nestedProperties.get("street");
        assertNotNull(streetField);
        assertEquals("string", streetField.get("type"));

        Map<String, Object> cityField = (Map<String, Object>) nestedProperties.get("city");
        assertNotNull(cityField);
        assertEquals("string", cityField.get("type"));
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
        Map<String, Object> jsonSchema = DescriptorToJsonSchema.convert(descriptor);

        assertNotNull(jsonSchema);
        assertEquals("object", jsonSchema.get("type"));

        Map<String, Object> properties = (Map<String, Object>) jsonSchema.get("properties");
        assertNotNull(properties);
        assertEquals(2, properties.size());

        Map<String, Object> nameField = (Map<String, Object>) properties.get("name");
        assertNotNull(nameField);
        assertEquals("string", nameField.get("type"));

        Map<String, Object> childrenField = (Map<String, Object>) properties.get("children");
        assertNotNull(childrenField);
        assertEquals("array", childrenField.get("type"));

        Map<String, Object> items = (Map<String, Object>) childrenField.get("items");
        assertNotNull(items);
        assertEquals("#/$defs/Node", items.get("$ref"));

        // Copy the schema and remove the $defs section. This is necessary because schema defintions don't,
        // themselves, have $defs. Only the root does.
        Map<String, Object> expectedSchema = new HashMap<>(jsonSchema);
        expectedSchema.remove("$defs");
        assertEquals(expectedSchema, ((Map<String, Object>) jsonSchema.get("$defs")).get("Node"));
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
        Map<String, Object> jsonSchema = DescriptorToJsonSchema.convert(descriptor);

        assertNotNull(jsonSchema);
        assertEquals("object", jsonSchema.get("type"));

        Map<String, Object> properties = (Map<String, Object>) jsonSchema.get("properties");
        assertNotNull(properties);
        assertEquals(2, properties.size());

        Map<String, Object> nameField = (Map<String, Object>) properties.get("name");
        assertNotNull(nameField);
        assertEquals("string", nameField.get("type"));

        Map<String, Object> choiceField = (Map<String, Object>) properties.get("choice");
        assertNotNull(choiceField);
        List<Map<String, Object>> anyOfList = (List<Map<String, Object>>) choiceField.get("anyOf");
        assertNotNull(anyOfList);
        assertEquals(2, anyOfList.size());

        boolean intChoiceFound = false;
        boolean strChoiceFound = false;

        for (Map<String, Object> anyOf : anyOfList) {
            Map.Entry<String, Object> field = anyOf.entrySet().iterator().next();
            if (field.getKey().equals("int_choice")) {
                assertEquals("integer", ((Map<String, Object>) field.getValue()).get("type"));
                intChoiceFound = true;
            } else if (field.getKey().equals("str_choice")) {
                assertEquals("string", ((Map<String, Object>) field.getValue()).get("type"));
                strChoiceFound = true;
            }
        }

        assertTrue(intChoiceFound);
        assertTrue(strChoiceFound);
    }
}