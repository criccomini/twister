---
layout: default
title: "JSON to Protocol Buffers"
---

# Convert JSON to Protocol Buffers

Twister pairs nicely with [Jackson’s](https://github.com/FasterXML/jackson) [`ObjectMapper`](https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/ObjectMapper.html) since they both use POJOs.

## Convert a JSON String to Protocol Buffer Bytes

```java
String jsonString = "{ \"id\": 1, \"name\": \"John Doe\" }";
Map<String, Object> personMap = new ObjectMapper().readValue(jsonString, Map.class);
ByteBuffer byteBuffer = new ProtoWriter().write(personMap, "Person");
```

## Infer a Protocol Buffer Descriptor from a JSON String

```java
String jsonString = "{\"name\": \"John Doe\", \"age\": 30, \"isStudent\": true}";
Map<String, Object> personMap = new ObjectMapper().readValue(jsonString, Map.class);
Descriptors.Descriptor descriptor = new ProtoDescriptorInferrer().infer(personMap, "Person");
```