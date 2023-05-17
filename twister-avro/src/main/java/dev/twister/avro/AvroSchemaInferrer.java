package dev.twister.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A utility class to infer Avro schema from Java objects.
 */
public class AvroSchemaInferrer {

    /**
     * Flag indicating whether maps should be treated as records during Avro schema inference.
     * Default value is {@code true}.
     */
    private final boolean mapAsRecord;

    /**
     * Creates an AvroSchemaInferrer with the default behavior of treating maps as records.
     */
    public AvroSchemaInferrer() {
        this(true);
    }

    /**
     * Creates an AvroSchemaInferrer.
     *
     * @param mapAsRecord A flag to indicate whether maps should be treated as records.
     */
    public AvroSchemaInferrer(boolean mapAsRecord) {
        this.mapAsRecord = mapAsRecord;
    }

    /**
     * Infers an Avro schema from a given Java Map and a record name.
     *
     * @param object The Java Map to infer the schema from.
     * @param recordName The name of the record.
     * @return The inferred Avro schema.
     */
    public Schema schema(Map<String, Object> object, String recordName) {
        return getSchemaBasedOnObjectType(object, recordName, null);
    }

    /**
     * Infers an Avro schema based on the type of the given object.
     *
     * @param value The object to infer the schema from.
     * @param fieldName The name of the field for the object.
     * @param parentName The name of the parent field, or null if there's no parent.
     * @return The inferred Avro schema.
     * @throws IllegalArgumentException If the object's type is unsupported or if the object is an empty array.
     */
    private Schema getSchemaBasedOnObjectType(Object value, String fieldName, String parentName) {
        Schema schema;
        String finalRecordName = (parentName != null) ? parentName + "_" + fieldName : fieldName;

        if (value == null) {
            schema = SchemaBuilder.builder().nullType();
        } else if (value instanceof Integer) {
            schema = SchemaBuilder.builder().intType();
        } else if (value instanceof Long) {
            schema = SchemaBuilder.builder().longType();
        } else if (value instanceof Float) {
            schema = SchemaBuilder.builder().floatType();
        } else if (value instanceof Double) {
            schema = SchemaBuilder.builder().doubleType();
        } else if (value instanceof Boolean) {
            schema = SchemaBuilder.builder().booleanType();
        } else if (value instanceof String) {
            schema = SchemaBuilder.builder().stringType();
        } else if (value instanceof Byte || value instanceof byte[] || value instanceof ByteBuffer) {
            // Byte, byte array, byte buffer
            schema = SchemaBuilder.builder().bytesType();
        } else if (value instanceof Map) {
            // Recursive call for nested map
            Map<String, Object> sortedMap = new TreeMap<>((Map<String, Object>) value);

            if (mapAsRecord) {
                schema = handleMapAsRecord(sortedMap, finalRecordName);
            } else {
                schema = handleMapAsMap(sortedMap, finalRecordName);
            }
        } else if (value instanceof List) {
            // Array type
            List<?> list = (List<?>) value;
            if (!list.isEmpty()) {
                Object firstItem = list.get(0);
                Schema elementType = getSchemaBasedOnObjectType(firstItem, fieldName, finalRecordName);
                schema = SchemaBuilder.array().items(nullableSchema(elementType));
            } else {
                throw new IllegalArgumentException("Cannot infer schema for an empty array");
            }
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
        }

        return schema;
    }

    /**
     * Handles the inference of an Avro schema for a map treated as a record.
     *
     * @param sortedMap The sorted map to infer the schema from.
     * @param finalRecordName The final name of the record.
     * @return The inferred Avro schema.
     */
    private Schema handleMapAsRecord(Map<String, Object> sortedMap, String finalRecordName) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(finalRecordName).fields();

        for (Map.Entry<String, Object> entry : sortedMap.entrySet()) {
            Schema fieldSchema = getSchemaBasedOnObjectType(entry.getValue(), entry.getKey(), finalRecordName);
            fields.name(entry.getKey()).type(nullableSchema(fieldSchema)).noDefault();
        }

        return fields.endRecord();
    }

    /**
     * Handles the inference of an Avro schema for a map treated as a map.
     *
     * @param sortedMap The sorted map to infer the schema from.
     * @param finalRecordName The final name of the record.
     * @return The inferred Avro schema.
     */
    private Schema handleMapAsMap(Map<String, Object> sortedMap, String finalRecordName) {
        Set<Schema> fieldSchemas = new HashSet<>();
        Set<String> schemaTypes = new HashSet<>();

        for (Map.Entry<String, Object> entry : sortedMap.entrySet()) {
            Schema fieldSchema = getSchemaBasedOnObjectType(entry.getValue(), entry.getKey(), finalRecordName);

            // Only add the schema if its type hasn't been added before
            if (schemaTypes.add(fieldSchema.getType().getName())) {
                fieldSchemas.add(fieldSchema);
            }
        }

        SchemaBuilder.UnionAccumulator<Schema> union = SchemaBuilder.unionOf().nullType();
        for (Schema schema : fieldSchemas) {
            union = union.and().type(schema);
        }

        return SchemaBuilder.map().values(union.endUnion());
    }

    /**
     * Returns a nullable version of the given schema.
     *
     * @param schema The schema to make nullable.
     * @return The nullable schema.
     */
    private Schema nullableSchema(Schema schema) {
        if (schema.getType() != Schema.Type.NULL) {
            return SchemaBuilder.unionOf().nullType().and().type(schema).endUnion();
        } else {
            return schema;
        }
    }
}
