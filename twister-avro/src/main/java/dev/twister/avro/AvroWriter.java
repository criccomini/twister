package dev.twister.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

        @Override
        public void write(Map<String, Object> datum, Encoder out) throws IOException {
            for (Schema.Field field : schema.getFields()) {
                Object value = datum.get(field.name());

                if (value == null) {
                    out.writeNull();
                } else {
                    switch (field.schema().getType()) {
                        case BOOLEAN:
                            out.writeBoolean((boolean) value);
                            break;
                        case INT:
                            out.writeInt((int) value);
                            break;
                        case LONG:
                            out.writeLong((long) value);
                            break;
                        case FLOAT:
                            out.writeFloat((float) value);
                            break;
                        case DOUBLE:
                            out.writeDouble((double) value);
                            break;
                        case STRING:
                            out.writeString((String) value);
                            break;
                        case BYTES:
                            out.writeBytes((ByteBuffer) value);
                            break;
                        // Add more cases here for other types
                        default:
                            throw new UnsupportedOperationException("Unsupported type: " + field.schema().getType());
                    }
                }
            }
        }
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
