package dev.twister.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtoReader {

    public static Map<String, Object> read(ByteBuffer inputBuffer, Descriptor descriptor) {
        Map<String, Object> resultMap = new HashMap<>();

        // Initialize resultMap with default values
        for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            if (fieldDescriptor.hasDefaultValue()) {
                String fieldName = fieldDescriptor.getName();
                Object defaultValue = fieldDescriptor.getDefaultValue();
                resultMap.put(fieldName, defaultValue);
            }
        }

        while (inputBuffer.hasRemaining()) {
            int key = (int) readVarint(inputBuffer);
            int wireType = key & 0x07;
            int fieldNumber = key >>> 3;

            FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(fieldNumber);

            if (fieldDescriptor == null) {
                throw new IllegalArgumentException("Unknown field number: " + fieldNumber);
            }

            OneofDescriptor oneofDescriptor = fieldDescriptor.getContainingOneof();
            boolean isOneof = oneofDescriptor != null;

            String fieldName = fieldDescriptor.getName();

            switch (wireType) {
                case 0: // Varint, SignedVarint, Bool, Enum
                    long rawValue = readVarint(inputBuffer);
                    Object value;

                    switch (fieldDescriptor.getType()) {
                        case BOOL:
                            value = rawValue != 0;
                            break;
                        case INT32:
                            value = (int) rawValue;
                            break;
                        case SINT32:
                            value = decodeZigZag32((int) rawValue);
                            break;
                        case SINT64:
                            value = decodeZigZag64(rawValue);
                            break;
                        case ENUM:
                            Descriptors.EnumValueDescriptor enumValueDescriptor = fieldDescriptor.getEnumType().findValueByNumber((int) rawValue);
                            if (enumValueDescriptor == null) {
                                throw new IllegalArgumentException("Unknown enum value: " + rawValue + " for field number: " + fieldNumber);
                            }
                            value = enumValueDescriptor.getName();
                            break;
                        case UINT32:
                        case FIXED32:
                            value = rawValue & 0xFFFFFFFFL; // convert to long treating as unsigned
                            break;
                        case UINT64:
                        case FIXED64:
                            value = new BigInteger(Long.toUnsignedString(rawValue)); // convert to BigInteger treating as unsigned
                            break;
                        default:
                            value = rawValue;
                            break;
                    }

                    addToResultMap(resultMap, fieldName, value, fieldDescriptor.isRepeated(), isOneof);
                    break;
                case 1: // Fixed64, SFixed64, Double
                    long rawFixed64Value = inputBuffer.order(ByteOrder.LITTLE_ENDIAN).getLong();
                    switch (fieldDescriptor.getType()) {
                        case DOUBLE:
                            value = Double.longBitsToDouble(rawFixed64Value);
                            break;
                        case FIXED64:
                            value = BigInteger.valueOf(rawFixed64Value);
                            break;
                        case SFIXED64:
                            value = rawFixed64Value;
                            break;
                        default:
                            throw new UnsupportedOperationException("Unsupported type: " + fieldDescriptor.getType());
                    }
                    addToResultMap(resultMap, fieldName, value, fieldDescriptor.isRepeated(), isOneof);
                    break;
                case 2: // Length-Delimited
                    // Check if the field is a map field
                    if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE && fieldDescriptor.getMessageType().getOptions().getMapEntry()) {
                        Descriptor mapEntryDescriptor = fieldDescriptor.getMessageType();
                        FieldDescriptor keyField = mapEntryDescriptor.findFieldByNumber(1);
                        FieldDescriptor valueField = mapEntryDescriptor.findFieldByNumber(2);

                        int length = (int) readVarint(inputBuffer);
                        byte[] bytes = new byte[length];
                        inputBuffer.get(bytes);
                        ByteBuffer nestedByteBuffer = ByteBuffer.wrap(bytes);
                        Map<String, Object> mapEntry = read(nestedByteBuffer, mapEntryDescriptor);

                        Object mapKey = mapEntry.get(keyField.getName());
                        Object mapValue = mapEntry.get(valueField.getName());

                        Map<Object, Object> map = (Map<Object, Object>) resultMap.computeIfAbsent(fieldName, k -> new HashMap<>());
                        map.put(mapKey, mapValue);
                    } else {
                        int length = (int) readVarint(inputBuffer);
                        byte[] bytes = new byte[length];
                        inputBuffer.get(bytes);
                        if (fieldDescriptor.getType() == FieldDescriptor.Type.STRING) {
                            String string = new String(bytes, StandardCharsets.UTF_8);
                            addToResultMap(resultMap, fieldName, string, fieldDescriptor.isRepeated(), isOneof);
                        } else if (fieldDescriptor.getType() == FieldDescriptor.Type.BYTES) {
                            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                            addToResultMap(resultMap, fieldName, byteBuffer, fieldDescriptor.isRepeated(), isOneof);
                        } else if (!isOneof && fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
                            ByteBuffer nestedByteBuffer = ByteBuffer.wrap(bytes);
                            Map<String, Object> nestedMessage = read(nestedByteBuffer, fieldDescriptor.getMessageType());
                            addToResultMap(resultMap, fieldName, nestedMessage, fieldDescriptor.isRepeated(), isOneof);
                        } else {
                            addToResultMap(resultMap, fieldName, bytes, fieldDescriptor.isRepeated(), isOneof);
                        }
                    }
                    break;
                case 5: // Fixed32, SFixed32, Float
                    int rawFixed32Value = inputBuffer.order(ByteOrder.LITTLE_ENDIAN).getInt();
                    switch (fieldDescriptor.getType()) {
                        case FLOAT:
                            value = Float.intBitsToFloat(rawFixed32Value);
                            break;
                        case FIXED32:
                            value = Long.valueOf(rawFixed32Value);
                            break;
                        case SFIXED32:
                            value = rawFixed32Value;
                            break;
                        default:
                            throw new UnsupportedOperationException("Unsupported type: " + fieldDescriptor.getType());
                    }
                    addToResultMap(resultMap, fieldName, value, fieldDescriptor.isRepeated(), isOneof);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported wire type: " + wireType);
            }
        }

        return resultMap;
    }

    private static void addToResultMap(Map<String, Object> resultMap, String fieldName, Object value, boolean isRepeated, boolean isOneof) {
        if (isOneof || !isRepeated) {
            resultMap.put(fieldName, value);
        } else {
            if (resultMap.containsKey(fieldName)) {
                @SuppressWarnings("unchecked")
                List<Object> values = (List<Object>) resultMap.get(fieldName);
                values.add(value);
            } else {
                List<Object> values = new ArrayList<>();
                values.add(value);
                resultMap.put(fieldName, values);
            }
        }
    }

    private static long readVarint(ByteBuffer byteBuffer) {
        long result = 0;
        int shift = 0;
        int currentByte;

        do {
            currentByte = byteBuffer.get() & 0xFF;
            result |= (long) (currentByte & 0x7F) << shift;
            shift += 7;
        } while ((currentByte & 0x80) != 0);

        return result;
    }

    private static int decodeZigZag32(int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    private static long decodeZigZag64(long n) {
        return (n >>> 1) ^ -(n & 1);
    }
}
