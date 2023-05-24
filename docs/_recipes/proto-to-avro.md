---
layout: default
title: "Protocol Buffers to Avro"
---

# Convert Protocol Buffers to Avro

You can use Twister's POJOs as a bridge between Protocol Buffers and Avro.

## Encode Protocol Buffer Messages to Avro Record Bytes

```java
Person person = Person.newBuilder()
        .setId(1)
        .setName("John Doe")
        .setAge(30)
        .setCity("New York")
        .build();

Map<String, Object> personMap = new ProtoWrapper().wrap(person);
ByteBuffer personAvroBytes = new AvroWriter().write(personMap, "Person");
```

## Convert Protocol Buffer Bytes to Avro Record Bytes

```java
Descriptor personDescriptor = Person.getDescriptor();
ByteBuffer personProtoBytes = ...;
Map<String, Object> personMap = ProtoReader.read(personProtoBytes, personDescriptor);
ByteBuffer personAvroBytes = new AvroWriter().write(personMap, "Person");
```

## Infer Avro Schema from Protocol Buffer Message

```java
Person person = Person.newBuilder()
        .setId(1)
        .setName("John Doe")
        .setAge(30)
        .setCity("New York")
        .build();

Map<String, Object> personMap = new ProtoWrapper().wrap(person);
Schema avroSchema = new AvroSchemaInferrer().infer(personMap, "Person");
```