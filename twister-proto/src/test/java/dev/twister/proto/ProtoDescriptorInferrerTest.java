package dev.twister.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import junit.framework.TestCase;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ProtoDescriptorInferrerTest extends TestCase {

    public void testPrimitives() throws Exception {
        Map<String, Object> object = new LinkedHashMap<>();
        object.put("fieldInt", Integer.valueOf(32));
        object.put("fieldLong", Long.valueOf(64));
        object.put("fieldBool", Boolean.TRUE);
        object.put("fieldStr", "string");
        object.put("fieldDouble", Double.valueOf(64.0));
        object.put("fieldBytes", ByteBuffer.wrap(new byte[] {1, 2, 3}));
        object.put("fieldFloat", Float.valueOf(32.0f));
        object.put("fieldBigInt", BigInteger.valueOf(Long.MAX_VALUE));

        Descriptors.Descriptor descriptor = new ProtoDescriptorInferrer().descriptor(object, "TestMessage");

        // Create expected schema using DynamicSchema
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("TestMessage.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("optional", "int32", "fieldInt", 1)
                .addField("optional", "int64", "fieldLong", 2)
                .addField("optional", "bool", "fieldBool", 3)
                .addField("optional", "string", "fieldStr", 4)
                .addField("optional", "double", "fieldDouble", 5)
                .addField("optional", "bytes", "fieldBytes", 6)
                .addField("optional", "float", "fieldFloat", 7)
                .addField("optional", "uint64", "fieldBigInt", 8)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema expectedSchema = schemaBuilder.build();
        Descriptors.Descriptor expectedDescriptor = expectedSchema.getMessageDescriptor("TestMessage");

        // Compare the descriptors
        assertEquals(expectedDescriptor.toProto(), descriptor.toProto());
    }

    public void testRepeatedFields() throws Exception {
        Map<String, Object> object = new LinkedHashMap<>();
        List<Integer> intList = Arrays.asList(32, 33, 34);
        object.put("fieldRepeated", intList);

        Descriptors.Descriptor descriptor = new ProtoDescriptorInferrer().descriptor(object, "TestMessage");

        // Create expected schema using DynamicSchema
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("TestMessage.proto");

        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("repeated", "int32", "fieldRepeated", 1)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema expectedSchema = schemaBuilder.build();
        Descriptors.Descriptor expectedDescriptor = expectedSchema.getMessageDescriptor("TestMessage");

        // Compare the descriptors
        assertEquals(expectedDescriptor.toProto(), descriptor.toProto());
    }

    public void testNestedMessages() throws Exception {
        ProtoDescriptorInferrer inferrer = new ProtoDescriptorInferrer();

        Map<String, Object> nestedObject = new LinkedHashMap<>();
        nestedObject.put("nestedFieldInt", Integer.valueOf(32));

        Map<String, Object> nestedNestedObject = new LinkedHashMap<>();
        nestedNestedObject.put("nestedNestedFieldStr", "nested string");
        nestedObject.put("nestedNestedMessage", nestedNestedObject);

        Map<String, Object> object = new LinkedHashMap<>();
        object.put("fieldStr", "string");
        object.put("nestedMessage", nestedObject);

        Descriptors.Descriptor descriptor = inferrer.descriptor(object, "TestMessage");

        // Create expected schema using DynamicSchema
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setName("TestMessage.proto");

        MessageDefinition nestedNestedMsgDef = MessageDefinition.newBuilder("TestMessage_nestedMessage_nestedNestedMessage")
                .addField("optional", "string", "nestedNestedFieldStr", 1)
                .build();

        MessageDefinition nestedMsgDef = MessageDefinition.newBuilder("TestMessage_nestedMessage")
                .addField("optional", "int32", "nestedFieldInt", 1)
                .addMessageDefinition(nestedNestedMsgDef)
                .addField("optional", "TestMessage_nestedMessage_nestedNestedMessage", "nestedNestedMessage", 2)
                .build();

        MessageDefinition msgDef = MessageDefinition.newBuilder("TestMessage")
                .addField("optional", "string", "fieldStr", 1)
                .addMessageDefinition(nestedMsgDef)
                .addField("optional", "TestMessage_nestedMessage", "nestedMessage", 2)
                .build();

        schemaBuilder.addMessageDefinition(msgDef);
        DynamicSchema expectedSchema = schemaBuilder.build();

        Descriptors.Descriptor expectedDescriptor = expectedSchema.getMessageDescriptor("TestMessage");

        // Compare the descriptors
        assertEquals(expectedDescriptor.toProto(), descriptor.toProto());
    }
}
