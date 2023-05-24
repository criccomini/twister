---
layout: default
title: "Protocol Buffers to JSON"
---

# Convert Protocol Buffers to JSON

Twister pairs nicely with [Jackson’s](https://github.com/FasterXML/jackson) [`ObjectMapper`](https://fasterxml.github.io/jackson-databind/javadoc/2.7/com/fasterxml/jackson/databind/ObjectMapper.html) since they both use POJOs.

## Encode Protocol Buffer Messages to a JSON String

```java
Person person = Person.newBuilder()
        .setId(1)
        .setName("John Doe")
        .setAge(30)
        .setCity("New York")
        .build();

Map<String, Object> personMap = new ProtoMapper().wrap(person);
String json = new ObjectMapper().writeValueAsString(personMap);
```

## Convert Protocol Buffer Bytes to a JSON String

```java
Descriptor personDescriptor = Person.getDescriptor();
ByteBuffer personProtoBytes = ...;
Map<String, Object> personMap = ProtoReader.read(personProtoBytes, personDescriptor);
String json = new ObjectMapper().writeValueAsString(personMap);
```
