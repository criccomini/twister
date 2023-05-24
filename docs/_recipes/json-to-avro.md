---
layout: default
title: "JSON to Avro"
---

# Convert JSON to Avro

Twister pairs nicely with [Jackson’s](https://github.com/FasterXML/jackson) [`ObjectMapper`](https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/ObjectMapper.html) since they both use POJOs.

## Convert a JSON String to Avro Bytes

```java
String jsonString = "{ \"id\": 1, \"name\": \"John Doe\" }";
Map<String, Object> personMap = new ObjectMapper().readValue(jsonString, Map.class);
ByteBuffer byteBuffer = new AvroWriter().write(personMap, "Person");
```

## Infer an Avro Schema from JSON String

```java
String jsonString = "{\"name\": \"John Doe\", \"age\": 30, \"isStudent\": true}";
Map<String, Object> personMap = new ObjectMapper().readValue(jsonString, Map.class);
Schema schema = new AvroSchemaInferrer().infer(personMap, "Person");
```