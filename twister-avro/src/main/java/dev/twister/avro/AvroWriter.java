package dev.twister.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class AvroWriter {
    public class MapDatumWriter implements DatumWriter<Map<String, Object>> {
        private Schema schema;

        public MapDatumWriter(Schema schema) {
            this.schema = schema;
        }

        @Override
        public void setSchema(Schema schema) {
            this.schema = schema;
        }

        private void writeObject(Object value, Schema schema, Encoder out) throws IOException {
            switch (schema.getType()) {
                case BOOLEAN:
                    out.writeBoolean((Boolean) value);
                    break;
                case INT:
                    out.writeInt((Integer) value);
                    break;
                case LONG:
                    out.writeLong((Long) value);
                    break;
                case FLOAT:
                    out.writeFloat((Float) value);
                    break;
                case DOUBLE:
                    out.writeDouble((Double) value);
                    break;
                case STRING:
                    out.writeString((String) value);
                    break;
                case BYTES:
                    out.writeBytes((ByteBuffer) value);
                    break;
                case RECORD:
                    Map<String, Object> recordValue = (Map<String, Object>) value;
                    MapDatumWriter recordWriter = new MapDatumWriter(schema);
                    recordWriter.write(recordValue, out);
                    break;
                case ENUM:
                    String enumValue = (String) value;
                    int index = schema.getEnumSymbols().indexOf(enumValue);
                    if (index < 0) {
                        throw new IOException("Invalid enum value: " + enumValue + " for schema: " + schema);
                    }
                    out.writeEnum(index);
                    break;
                case ARRAY:
                    List<Object> arrayValue = (List<Object>) value;
                    out.writeArrayStart();
                    out.setItemCount(arrayValue.size());
                    Schema arraySchema = schema.getElementType();
                    for (Object item : arrayValue) {
                        out.startItem();
                        writeObject(item, arraySchema, out);
                    }
                    out.writeArrayEnd();
                    break;
                case MAP:
                    Map<String, Object> mapValue = (Map<String, Object>) value;
                    out.writeMapStart();
                    out.setItemCount(mapValue.size());
                    Schema mapValueSchema = schema.getValueType();
                    for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
                        out.startItem();
                        out.writeString(entry.getKey());
                        writeObject(entry.getValue(), mapValueSchema, out);
                    }
                    out.writeMapEnd();
                    break;
                case UNION:
                    List<Schema> unionSchemas = schema.getTypes();
                    boolean isWritten = false;
                    for (Schema unionSchema : unionSchemas) {
                        try {
                            writeObject(value, unionSchema, out);
                            isWritten = true;
                            break;
                        } catch (Exception e) {
                            // Ignore exception and try next schema
                        }
                    }
                    if (!isWritten) {
                        throw new IOException("Invalid union value: " + value + " for schema: " + schema);
                    }
                    break;
                case FIXED:
                    ByteBuffer fixedValueBuffer = (ByteBuffer) value;
                    if (fixedValueBuffer.remaining() != schema.getFixedSize()) {
                        throw new IOException("Invalid fixed value size: " + fixedValueBuffer.remaining() + " for schema: " + schema);
                    }
                    out.writeFixed(fixedValueBuffer);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + schema.getType());
            }
        }

        @Override
        public void write(Map<String, Object> datum, Encoder out) throws IOException {
            for (Schema.Field field : schema.getFields()) {
                Object value = datum.get(field.name());
                if (value == null) {
                    out.writeNull();
                } else {
                    writeObject(value, field.schema(), out);
                }
            }
        }
    }

    public ByteBuffer writeAvro(Map<String, Object> object, String recordName) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        MapDatumWriter writer = new MapDatumWriter(new AvroSchemaInferrer().schema(object, recordName));
        writer.write(object, encoder);
        encoder.flush();
        return ByteBuffer.wrap(outputStream.toByteArray());
    }

    public ByteBuffer writeAvro(Map<String, Object> object, Schema schema) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        MapDatumWriter writer = new MapDatumWriter(schema);
        writer.write(object, encoder);
        encoder.flush();
        return ByteBuffer.wrap(outputStream.toByteArray());
    }
}
