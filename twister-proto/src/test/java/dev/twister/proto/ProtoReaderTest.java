package dev.twister.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import junit.framework.TestCase;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ProtoReaderTest extends TestCase {
    public void testRead() throws Exception {
        // Load the schema from the .proto schema string
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("PersonSchemaDynamic.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("required", "int32", "int_field", 1)
                .addField("required", "string", "str_field", 2)
                .addField("repeated", "int32", "repeated_field", 3)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();
        Descriptor descriptor = schema.getMessageDescriptor("TestMessage");

        // Create a test message using the descriptor
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("int_field"), 150);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("str_field"), "foo");
        dynamicMessageBuilder.addRepeatedField(descriptor.findFieldByName("repeated_field"), 2147483647);
        dynamicMessageBuilder.addRepeatedField(descriptor.findFieldByName("repeated_field"), 7);

        // Serialize the DynamicMessage to a ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(dynamicMessageBuilder.build().toByteArray());

        // Convert the byte buffer to a map using the descriptor
        Map<String, Object> resultMap = ProtoReader.read(byteBuffer, descriptor);

        // Verify the result
        assertEquals(150, resultMap.get("int_field"));
        assertEquals("foo", resultMap.get("str_field"));
        assertEquals(List.of(2147483647, 7), resultMap.get("repeated_field"));
    }

    public void testReadOneof() throws Exception {
        // Load the schema from the .proto schema string
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("OneofSchemaDynamic.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("OneofTestMessage")
                .addField("optional", "int32", "optional_field", 3)
                .addField("required", "string", "required_field", 4)
                .addOneof("oneof_field")
                .addField("int32", "int_field", 1)
                .addField("string", "str_field", 2)
                .msgDefBuilder()
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();
        Descriptor descriptor = schema.getMessageDescriptor("OneofTestMessage");

        // Create a test message using the descriptor
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("required_field"), "bar");
        dynamicMessageBuilder.setField(descriptor.findFieldByName("str_field"), "oneof string");

        // Serialize the DynamicMessage to a ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(dynamicMessageBuilder.build().toByteArray());

        // Convert the byte buffer to a map using the descriptor
        Map<String, Object> resultMap = ProtoReader.read(byteBuffer, descriptor);

        // Verify the result
        assertNull(resultMap.get("int_field"));
        assertNull(resultMap.get("optional_field"));
        assertEquals("bar", resultMap.get("required_field"));
        assertEquals("oneof string", resultMap.get("str_field"));
        assertNull(resultMap.get("float_field"));
    }

    public void testReadNestedMessage() throws Exception {
        // Load the schema from the .proto schema string
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("NestedSchemaDynamic.proto");

        MessageDefinition nestedMsgDef = MessageDefinition.newBuilder("NestedMessage")
                .addField("required", "int32", "nested_int_field", 1)
                .addField("required", "string", "nested_str_field", 2)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("OuterMessage")
                .addField("required", "int32", "outer_int_field", 1)
                .addField("required", "NestedMessage", "nested_message", 2)
                .build();

        schemaBuilder.addMessageDefinition(nestedMsgDef);
        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        Descriptor nestedDescriptor = schema.getMessageDescriptor("NestedMessage");
        Descriptor outerDescriptor = schema.getMessageDescriptor("OuterMessage");

        // Create a nested message using the descriptor
        DynamicMessage.Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedDescriptor);
        nestedMessageBuilder.setField(nestedDescriptor.findFieldByName("nested_int_field"), 42);
        nestedMessageBuilder.setField(nestedDescriptor.findFieldByName("nested_str_field"), "nested string");

        // Create an outer message using the descriptor
        DynamicMessage.Builder outerMessageBuilder = DynamicMessage.newBuilder(outerDescriptor);
        outerMessageBuilder.setField(outerDescriptor.findFieldByName("outer_int_field"), 100);
        outerMessageBuilder.setField(outerDescriptor.findFieldByName("nested_message"), nestedMessageBuilder.build());

        // Serialize the outer DynamicMessage to a ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(outerMessageBuilder.build().toByteArray());

        // Convert the byte buffer to a map using the descriptor
        Map<String, Object> resultMap = ProtoReader.read(byteBuffer, outerDescriptor);

        // Verify the result
        assertEquals(100, resultMap.get("outer_int_field"));
        Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested_message");
        assertNotNull(nestedResultMap);
        assertEquals(42, nestedResultMap.get("nested_int_field"));
        assertEquals("nested string", nestedResultMap.get("nested_str_field"));
    }

    public void testPrimitiveTypes() throws Exception {
        // Load the schema from the .proto schema string
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("PrimitiveSchemaDynamic.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("PrimitiveMessage")
                .addField("optional", "int32", "int32_field", 1)
                .addField("optional", "int64", "int64_field", 2)
                .addField("optional", "uint32", "uint32_field", 3)
                .addField("optional", "uint64", "uint64_field", 4)
                .addField("optional", "sint32", "sint32_field", 5)
                .addField("optional", "sint64", "sint64_field", 6)
                .addField("optional", "bool", "bool_field", 7)
                .addField("optional", "fixed64", "fixed64_field", 8)
                .addField("optional", "sfixed64", "sfixed64_field", 9)
                .addField("optional", "double", "double_field", 10)
                .addField("optional", "fixed32", "fixed32_field", 11)
                .addField("optional", "sfixed32", "sfixed32_field", 12)
                .addField("optional", "float", "float_field", 13)
                .addField("optional", "bytes", "bytes_field", 14)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();
        Descriptor descriptor = schema.getMessageDescriptor("PrimitiveMessage");

        // Create a test message using the descriptor
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("int32_field"), 123);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("int64_field"), 1234567890123456789L);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("uint32_field"), 456);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("uint64_field"), Long.parseUnsignedLong("9876543210987654321"));
        dynamicMessageBuilder.setField(descriptor.findFieldByName("sint32_field"), -789);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("sint64_field"), -1234567890987654321L);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("bool_field"), true);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("fixed64_field"), 1234567890L);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("sfixed64_field"), -1234567890L);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("double_field"), 1234.5678);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("fixed32_field"), 12345678);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("sfixed32_field"), -12345678);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("float_field"), 12.34f);
        dynamicMessageBuilder.setField(descriptor.findFieldByName("bytes_field"), new byte[]{1, 2, 3, 4, 5});

        // Serialize the DynamicMessage to a ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(dynamicMessageBuilder.build().toByteArray());

        // Convert the byte buffer to a map using the descriptor
        Map<String, Object> resultMap = ProtoReader.read(byteBuffer, descriptor);

        // Verify the result
        assertEquals(123, resultMap.get("int32_field"));
        assertEquals(1234567890123456789L, resultMap.get("int64_field"));
        assertEquals(456L, resultMap.get("uint32_field"));
        assertEquals(new BigInteger("9876543210987654321"), resultMap.get("uint64_field"));
        assertEquals(-789, resultMap.get("sint32_field"));
        assertEquals(-1234567890987654321L, resultMap.get("sint64_field"));
        assertEquals(true, resultMap.get("bool_field"));
        assertEquals(new BigInteger("1234567890"), resultMap.get("fixed64_field"));
        assertEquals(-1234567890L, resultMap.get("sfixed64_field"));
        assertEquals(1234.5678, resultMap.get("double_field"));
        assertEquals(12345678L, resultMap.get("fixed32_field"));
        assertEquals(-12345678, resultMap.get("sfixed32_field"));
        assertEquals(12.34f, resultMap.get("float_field"));
        assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}), resultMap.get("bytes_field"));
    }

    public void testEnumType() throws Exception {
        // Define an enum type
        EnumDefinition enumDefinition = EnumDefinition.newBuilder("TestEnum")
                .addValue("ENUM_VALUE_1", 1)
                .addValue("ENUM_VALUE_2", 2)
                .build();

        // Load the schema from the .proto schema string
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("EnumSchemaDynamic.proto");

        schemaBuilder.addEnumDefinition(enumDefinition);

        MessageDefinition msgDef = MessageDefinition.newBuilder("EnumMessage")
                .addField("optional", "TestEnum", "enum_field", 1)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema schema = schemaBuilder.build();

        Descriptor descriptor = schema.getMessageDescriptor("EnumMessage");
        Descriptors.EnumDescriptor enumDescriptor = schema.getEnumDescriptor("TestEnum");

        // Create a test message using the descriptor
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);
        Descriptors.EnumValueDescriptor enumValueDescriptor = enumDescriptor.findValueByName("ENUM_VALUE_2");
        dynamicMessageBuilder.setField(descriptor.findFieldByName("enum_field"), enumValueDescriptor);

        // Serialize the DynamicMessage to a ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(dynamicMessageBuilder.build().toByteArray());

        // Convert the byte buffer to a map using the descriptor
        Map<String, Object> resultMap = ProtoReader.read(byteBuffer, descriptor);

        // Verify the result
        assertEquals("ENUM_VALUE_2", resultMap.get("enum_field"));
    }
}
