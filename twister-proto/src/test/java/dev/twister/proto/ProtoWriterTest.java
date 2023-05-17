package dev.twister.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import junit.framework.TestCase;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

public class ProtoWriterTest extends TestCase {
    public void testWrite() throws Exception {
        // Define a message with all necessary fields
        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("required", "int32", "int32_field", 1)
                .addField("required", "int64", "int64_field", 2)
                .addField("required", "uint32", "uint32_field", 3)
                .addField("required", "uint64", "uint64_field", 4)
                .addField("required", "sint32", "sint32_field", 5)
                .addField("required", "sint64", "sint64_field", 6)
                .addField("required", "bool", "bool_field", 7)
                .addField("required", "string", "enum_field", 8)
                .addField("required", "fixed64", "fixed64_field", 9)
                .addField("required", "sfixed64", "sfixed64_field", 10)
                .addField("required", "double", "double_field", 11)
                .addField("required", "string", "string_field", 12)
                .addField("required", "bytes", "bytes_field", 13)
                .addField("required", "fixed32", "fixed32_field", 14)
                .addField("required", "sfixed32", "sfixed32_field", 15)
                .addField("required", "uint32", "big_uint32_field", 16)
                .addField("required", "uint64", "big_uint64_field", 17)
                .addField("required", "fixed32", "big_fixed32_field", 18)
                .addField("required", "fixed64", "big_fixed64_field", 19)
                .build();

        // Create a dynamic schema
        DynamicSchema schema = DynamicSchema.newBuilder()
                .setName("TestSchema.proto")
                .addMessageDefinition(msgDef)
                .build();

        // Get descriptor for the dynamic message
        Descriptor descriptor = schema.getMessageDescriptor("TestMessage");

        // Create a map with the same field names and values
        Map<String, Object> object = new HashMap<>();
        object.put("int32_field", 1);
        object.put("int64_field", 2L);
        object.put("uint32_field", 3L);
        object.put("uint64_field", BigInteger.valueOf(4));
        object.put("sint32_field", 5);
        object.put("sint64_field", 6L);
        object.put("bool_field", true);
        object.put("enum_field", "ENUM_VALUE");
        object.put("fixed64_field", BigInteger.valueOf(9));
        object.put("sfixed64_field", 10L);
        object.put("double_field", 11.0);
        object.put("string_field", "test string");
        object.put("bytes_field", ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        object.put("fixed32_field", 14L);
        object.put("sfixed32_field", 15);
        object.put("big_uint32_field", 4294967295L);
        object.put("big_uint64_field", new BigInteger("18446744073709551615"));
        object.put("big_fixed32_field", 4294967295L);
        object.put("big_fixed64_field", new BigInteger("18446744073709551615"));


        // Create ProtoWriter instance and write the map into a ByteBuffer
        ProtoWriter writer = new ProtoWriter();
        ByteBuffer outputBuffer = writer.write(object, descriptor);

        // Parse the ByteBuffer back into a DynamicMessage
        byte[] outputBytes = new byte[outputBuffer.remaining()];
        outputBuffer.get(outputBytes);
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, outputBytes);

        // Assert that the values of each field are as expected
        assertEquals(1, dynamicMessage.getField(descriptor.findFieldByName("int32_field")));
        assertEquals(2L, dynamicMessage.getField(descriptor.findFieldByName("int64_field")));
        assertEquals(3, dynamicMessage.getField(descriptor.findFieldByName("uint32_field")));
        assertEquals(4L, dynamicMessage.getField(descriptor.findFieldByName("uint64_field")));
        assertEquals(5, dynamicMessage.getField(descriptor.findFieldByName("sint32_field")));
        assertEquals(6L, dynamicMessage.getField(descriptor.findFieldByName("sint64_field")));
        assertEquals(true, dynamicMessage.getField(descriptor.findFieldByName("bool_field")));
        assertEquals("ENUM_VALUE", dynamicMessage.getField(descriptor.findFieldByName("enum_field")));
        assertEquals(9L, dynamicMessage.getField(descriptor.findFieldByName("fixed64_field")));
        assertEquals(10L, dynamicMessage.getField(descriptor.findFieldByName("sfixed64_field")));
        assertEquals(11.0, dynamicMessage.getField(descriptor.findFieldByName("double_field")));
        assertEquals("test string", dynamicMessage.getField(descriptor.findFieldByName("string_field")));
        assertTrue(Arrays.equals(new byte[]{1, 2, 3, 4}, ((ByteString) dynamicMessage.getField(descriptor.findFieldByName("bytes_field"))).toByteArray()));
        assertEquals(14, dynamicMessage.getField(descriptor.findFieldByName("fixed32_field")));
        assertEquals(15, dynamicMessage.getField(descriptor.findFieldByName("sfixed32_field")));
        assertEquals((int) 4294967295L, dynamicMessage.getField(descriptor.findFieldByName("big_uint32_field")));
        assertEquals(Long.parseUnsignedLong("18446744073709551615"), dynamicMessage.getField(descriptor.findFieldByName("big_uint64_field")));
        assertEquals((int) 4294967295L, dynamicMessage.getField(descriptor.findFieldByName("big_fixed32_field")));
        assertEquals(Long.parseUnsignedLong("18446744073709551615"), dynamicMessage.getField(descriptor.findFieldByName("big_fixed64_field")));
    }

    public void testRepeatedFields() throws Exception {
        // Define a message with a repeated field
        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("repeated", "int32", "int32_field", 1)
                .build();

        // Create a dynamic schema
        DynamicSchema schema = DynamicSchema.newBuilder()
                .setName("TestSchema.proto")
                .addMessageDefinition(msgDef)
                .build();

        // Get descriptor for the dynamic message
        Descriptor descriptor = schema.getMessageDescriptor("TestMessage");

        // Create a map with a repeated field
        Map<String, Object> object = new HashMap<>();
        object.put("int32_field", Arrays.asList(1, 2, 3, 4, 5));

        // Create ProtoWriter instance and write the map into a ByteBuffer
        ByteBuffer outputBuffer = new ProtoWriter().write(object, descriptor);

        // Parse the ByteBuffer back into a DynamicMessage
        byte[] outputBytes = new byte[outputBuffer.remaining()];
        outputBuffer.get(outputBytes);
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, outputBytes);

        // Assert that the values of the repeated field are as expected
        @SuppressWarnings("unchecked")
        List<Integer> actualValues = (List<Integer>) dynamicMessage.getField(descriptor.findFieldByName("int32_field"));
        List<Integer> expectedValues = Arrays.asList(1, 2, 3, 4, 5);
        assertEquals(expectedValues, actualValues);
    }

    public void testNestedMessage() throws Exception {
        // Define a nested message
        MessageDefinition nestedMsgDef = MessageDefinition.newBuilder("NestedMessage")
                .addField("required", "int32", "nested_int32_field", 1)
                .build();

        // Define a message with a nested message field
        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("required", "NestedMessage", "nested_message_field", 1)
                .build();

        // Create a dynamic schema
        DynamicSchema schema = DynamicSchema.newBuilder()
                .setName("TestSchema.proto")
                .addMessageDefinition(nestedMsgDef)
                .addMessageDefinition(msgDef)
                .build();

        // Get descriptor for the dynamic message
        Descriptor descriptor = schema.getMessageDescriptor("TestMessage");

        // Create a map with a nested map
        Map<String, Object> nestedObject = new HashMap<>();
        nestedObject.put("nested_int32_field", 42);

        Map<String, Object> object = new HashMap<>();
        object.put("nested_message_field", nestedObject);

        // Create ProtoWriter instance and write the map into a ByteBuffer
        ByteBuffer outputBuffer = new ProtoWriter().write(object, descriptor);

        // Parse the ByteBuffer back into a DynamicMessage
        byte[] outputBytes = new byte[outputBuffer.remaining()];
        outputBuffer.get(outputBytes);
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, outputBytes);

        // Assert that the values of the nested message field are as expected
        DynamicMessage nestedMessage = (DynamicMessage) dynamicMessage.getField(descriptor.findFieldByName("nested_message_field"));
        assertEquals(42, nestedMessage.getField(nestedMessage.getDescriptorForType().findFieldByName("nested_int32_field")));
    }

    public void testWriteWithInferredDescriptor() throws Exception {
        // Define a simple message
        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("required", "int32", "id", 1)
                .addField("optional", "string", "name", 2)
                .build();

        // Create a dynamic schema
        DynamicSchema schema = DynamicSchema.newBuilder()
                .setName("TestSchema.proto")
                .addMessageDefinition(msgDef)
                .build();

        // Create a map with primitive values
        Map<String, Object> object = new LinkedHashMap<>();
        object.put("id", 42);
        object.put("name", "Test");

        // Create ProtoWriter instance and write the map into a ByteBuffer
        ByteBuffer outputBuffer = new ProtoWriter().write(object, "TestMessage");

        // Parse the ByteBuffer back into a DynamicMessage
        byte[] outputBytes = new byte[outputBuffer.remaining()];
        outputBuffer.get(outputBytes);

        Descriptor descriptor = schema.getMessageDescriptor("TestMessage");
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, outputBytes);

        // Assert that the values of the message fields are as expected
        assertEquals(42, dynamicMessage.getField(descriptor.findFieldByName("id")));
        assertEquals("Test", dynamicMessage.getField(descriptor.findFieldByName("name")));
    }
}
