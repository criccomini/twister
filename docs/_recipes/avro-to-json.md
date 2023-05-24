---
layout: default
title: "Avro to JSON"
---

# Convert Avro to JSON

Twister pairs nicely with [Jackson’s](https://github.com/FasterXML/jackson) [`ObjectMapper`](https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/ObjectMapper.html) since they both use POJOs.

## Encode Avro Records to a JSON String

```java
GenericRecord avroRecord = ...;
Map<String, Object> personMap = new AvroWrapper().wrap(avroRecord);
String jsonString = new ObjectMapper().writeValueAsString(personMap);
```

## Convert Avro Record Bytes to a JSON String

```java
Schema avroSchema = ...;
ByteBuffer avroBytes = ...;
Map<String, Object> personMap = new AvroReader().read(avroBytes, avroSchema);
String jsonString = new ObjectMapper().writeValueAsString(personMap);
```
