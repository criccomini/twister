package dev.twister.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroReader {
    public Map<String, Object> read(ByteBuffer inputBuffer, Schema schema) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputBuffer.array(), null);
        return (Map<String, Object>) readBasedOnSchema(decoder, schema);
    }

    private Object readBasedOnSchema(BinaryDecoder decoder, Schema schema) throws IOException {
        switch (schema.getType()) {
            case RECORD:
                Map<String, Object> resultMap = new HashMap<>();
                for (Schema.Field field : schema.getFields()) {
                    resultMap.put(field.name(), readBasedOnSchema(decoder, field.schema()));
                }
                return resultMap;
            case ENUM:
                return schema.getEnumSymbols().get(decoder.readEnum());
            case UNION:
                int unionIndex = decoder.readIndex();
                return readBasedOnSchema(decoder, schema.getTypes().get(unionIndex));
            case FIXED:
                byte[] fixedBytes = new byte[schema.getFixedSize()];
                decoder.readFixed(fixedBytes);
                return ByteBuffer.wrap(fixedBytes);
            case ARRAY:
                Schema elementSchema = schema.getElementType();
                long arraySize = decoder.readArrayStart();
                List<Object> array = new ArrayList<>();
                while (arraySize > 0) {
                    for (long i = 0; i < arraySize; i++) {
                        array.add(readBasedOnSchema(decoder, elementSchema));
                    }
                    arraySize = decoder.arrayNext();
                }
                return array;
            case MAP:
                Schema valueSchema = schema.getValueType();
                long mapSize = decoder.readMapStart();
                Map<String, Object> map = new HashMap<>();
                while (mapSize > 0) {
                    for (long i = 0; i < mapSize; i++) {
                        String key = decoder.readString();
                        map.put(key, readBasedOnSchema(decoder, valueSchema));
                    }
                    mapSize = decoder.mapNext();
                }
                return map;
            default:
                return readPrimitive(decoder, schema.getType());
        }
    }

    private Object readPrimitive(BinaryDecoder decoder, Schema.Type type) throws IOException {
        switch (type) {
            case BOOLEAN:
                return decoder.readBoolean();
            case INT:
                return decoder.readInt();
            case LONG:
                return decoder.readLong();
            case FLOAT:
                return decoder.readFloat();
            case DOUBLE:
                return decoder.readDouble();
            case STRING:
                return decoder.readString();
            case BYTES:
                return decoder.readBytes(null);
            case NULL:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + type);
        }
    }
}
