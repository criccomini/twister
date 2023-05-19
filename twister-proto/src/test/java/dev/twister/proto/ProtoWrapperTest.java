package dev.twister.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import junit.framework.TestCase;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ProtoWrapperTest extends TestCase {
    private final ProtoWrapper wrapper = new ProtoWrapper();

    public void testWrapPrimitives() throws Exception {
        MessageDefinition msgDef = MessageDefinition.newBuilder("Test")
                .addField("optional", "int32", "testInt32", 1)
                .addField("optional", "sint32", "testSint32", 2)
                .addField("optional", "sfixed32", "testSfixed32", 3)
                .addField("optional", "int64", "testInt64", 4)
                .addField("optional", "sint64", "testSint64", 5)
                .addField("optional", "bool", "testBool", 6)
                .addField("optional", "string", "testString", 7)
                .addField("optional", "double", "testDouble", 8)
                .addField("optional", "float", "testFloat", 9)
                .addField("optional", "uint32", "testUint32", 10)
                .addField("optional", "fixed32", "testFixed32", 11)
                .addField("optional", "uint64", "testUint64", 12)
                .addField("optional", "fixed64", "testFixed64", 13)
                .addField("optional", "sfixed64", "testSfixed64", 14)
                .addField("optional", "bytes", "testBytes", 15)
                .build();

        DynamicSchema schema = DynamicSchema.newBuilder()
                .addMessageDefinition(msgDef)
                .build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("Test");
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("testInt32"), 123)
                .setField(descriptor.findFieldByName("testSint32"), 123)
                .setField(descriptor.findFieldByName("testSfixed32"), 123)
                .setField(descriptor.findFieldByName("testInt64"), 123L)
                .setField(descriptor.findFieldByName("testSint64"), 123L)
                .setField(descriptor.findFieldByName("testBool"), true)
                .setField(descriptor.findFieldByName("testString"), "string")
                .setField(descriptor.findFieldByName("testDouble"), 123.45)
                .setField(descriptor.findFieldByName("testFloat"), 123.45f)
                .setField(descriptor.findFieldByName("testUint32"), Integer.parseUnsignedInt("4294967295")) // Value larger than maximum int
                .setField(descriptor.findFieldByName("testFixed32"), Integer.parseUnsignedInt("4294967295"))
                .setField(descriptor.findFieldByName("testUint64"), Long.parseUnsignedLong("18446744073709551615")) // Maximum uint64 value
                .setField(descriptor.findFieldByName("testFixed64"), Long.parseUnsignedLong("18446744073709551615"))
                .setField(descriptor.findFieldByName("testSfixed64"), 123L)
                .setField(descriptor.findFieldByName("testBytes"), ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();

        Map<String, Object> result = wrapper.wrap(message);

        assertEquals(123, result.get("testInt32"));
        assertEquals(123, result.get("testSint32"));
        assertEquals(123, result.get("testSfixed32"));
        assertEquals(123L, result.get("testInt64"));
        assertEquals(123L, result.get("testSint64"));
        assertEquals(true, result.get("testBool"));
        assertEquals("string", result.get("testString"));
        assertEquals(123.45, result.get("testDouble"));
        assertEquals(123.45f, result.get("testFloat"));
        assertEquals(4294967295L, result.get("testUint32"));
        assertEquals(4294967295L, result.get("testFixed32"));
        assertEquals(new BigInteger("18446744073709551615"), result.get("testUint64"));
        assertEquals(new BigInteger("18446744073709551615"), result.get("testFixed64"));
        assertEquals(123L, result.get("testSfixed64"));
        assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3}), result.get("testBytes"));
    }

    public void testWrapNestedMessage() throws Exception {
        MessageDefinition nestedMsgDef = MessageDefinition.newBuilder("Nested")
                .addField("optional", "int32", "testNestedInt", 1)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("Test")
                .addField("optional", "Nested", "testNested", 1)
                .build();

        DynamicSchema schema = DynamicSchema.newBuilder()
                .addMessageDefinition(nestedMsgDef)
                .addMessageDefinition(msgDef)
                .build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("Test");
        Descriptors.Descriptor nestedDescriptor = schema.getMessageDescriptor("Nested");

        DynamicMessage nestedMessage = DynamicMessage.newBuilder(nestedDescriptor)
                .setField(nestedDescriptor.findFieldByName("testNestedInt"), 456)
                .build();

        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("testNested"), nestedMessage)
                .build();

        Map<String, Object> result = wrapper.wrap(message);
        Map<String, Object> nestedResult = (Map<String, Object>) result.get("testNested");

        assertEquals(456, nestedResult.get("testNestedInt"));
    }

    public void testWrapRepeated() throws Exception {
        MessageDefinition msgDef = MessageDefinition.newBuilder("Test")
                .addField("repeated", "int32", "testRepeatedInt", 1)
                .build();

        DynamicSchema schema = DynamicSchema.newBuilder()
                .addMessageDefinition(msgDef)
                .build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("Test");

        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(descriptor.findFieldByName("testRepeatedInt"), 123)
                .addRepeatedField(descriptor.findFieldByName("testRepeatedInt"), 456)
                .addRepeatedField(descriptor.findFieldByName("testRepeatedInt"), 789)
                .build();

        Map<String, Object> result = wrapper.wrap(message);
        List<Integer> repeatedResult = (List<Integer>) result.get("testRepeatedInt");

        assertEquals(Arrays.asList(123, 456, 789), repeatedResult);
    }

    public void testWrapEnum() throws Exception {
        EnumDefinition enumDef = EnumDefinition.newBuilder("TestEnum")
                .addValue("FIRST", 1)
                .addValue("SECOND", 2)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("Test")
                .addField("optional", "TestEnum", "testEnum", 1)
                .build();

        DynamicSchema schema = DynamicSchema.newBuilder()
                .addEnumDefinition(enumDef)
                .addMessageDefinition(msgDef)
                .build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("Test");
        Descriptors.EnumDescriptor enumDescriptor = schema.getEnumDescriptor("TestEnum");

        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("testEnum"), enumDescriptor.getValues().get(1))
                .build();

        Map<String, Object> result = wrapper.wrap(message);

        assertEquals("SECOND", result.get("testEnum"));
    }

    public void testWrapRepeatedMessages() throws Exception {
        MessageDefinition repeatedMsgDef = MessageDefinition.newBuilder("Repeated")
                .addField("optional", "int32", "testRepeatedInt", 1)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("Test")
                .addField("repeated", "Repeated", "testRepeated", 1)
                .build();

        DynamicSchema schema = DynamicSchema.newBuilder()
                .addMessageDefinition(repeatedMsgDef)
                .addMessageDefinition(msgDef)
                .build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("Test");
        Descriptors.Descriptor repeatedDescriptor = schema.getMessageDescriptor("Repeated");

        DynamicMessage repeatedMessage1 = DynamicMessage.newBuilder(repeatedDescriptor)
                .setField(repeatedDescriptor.findFieldByName("testRepeatedInt"), 123)
                .build();

        DynamicMessage repeatedMessage2 = DynamicMessage.newBuilder(repeatedDescriptor)
                .setField(repeatedDescriptor.findFieldByName("testRepeatedInt"), 456)
                .build();

        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .addRepeatedField(descriptor.findFieldByName("testRepeated"), repeatedMessage1)
                .addRepeatedField(descriptor.findFieldByName("testRepeated"), repeatedMessage2)
                .build();

        Map<String, Object> result = wrapper.wrap(message);
        List<Map<String, Object>> repeatedResult = (List<Map<String, Object>>) result.get("testRepeated");

        assertEquals(123, repeatedResult.get(0).get("testRepeatedInt"));
        assertEquals(456, repeatedResult.get(1).get("testRepeatedInt"));
    }

    public void testOneOfFieldHandling() throws Exception {
        // Define a protobuf message type with a 'oneof' field using the dynamic schema library.
        MessageDefinition msgDef = MessageDefinition.newBuilder("MyMessage")
                .addField("optional", "string", "option_one", 1)
                .addOneof("my_oneof")
                .addField("string", "oneof_option_one", 3)
                .addField("string", "oneof_option_two", 4)
                .msgDefBuilder()
                .build();

        DynamicSchema schema = DynamicSchema.newBuilder()
                .setName("MyMessage.proto")
                .addMessageDefinition(msgDef)
                .build();

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("MyMessage");
        // Build a dynamic message where 'oneof_option_two' is set.
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("option_one"), "a string")
                .setField(descriptor.findFieldByName("oneof_option_two"), "some value")
                .build();

        ProtoWrapper protoWrapper = new ProtoWrapper();
        Map<String, Object> map = protoWrapper.wrap(message);

        // Check basic map usage
        assertEquals("a string", map.get("option_one"));
        assertEquals("some value", map.get("oneof_option_two"));
        assertNull(map.get("oneof_option_one"));

        // Check size()
        assertEquals(2, map.size());

        // Check that iteration works
        Iterator<Map.Entry<String, Object>> entrySet = map.entrySet().iterator();
        assertTrue(entrySet.hasNext());
        Map.Entry<String, Object> entry = entrySet.next();
        assertEquals("option_one", entry.getKey());
        assertEquals("a string", entry.getValue());
        assertTrue(entrySet.hasNext());
        entry = entrySet.next();
        assertEquals("oneof_option_two", entry.getKey());
        assertEquals("some value", entry.getValue());
        assertFalse(entrySet.hasNext());
    }
}
