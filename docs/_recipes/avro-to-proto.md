---
layout: default
title: "Avro to Protocol Buffers"
---

# Convert Avro to Protocol Buffers

You can use Twister's POJOs as a bridge between Protocol Buffers and Avro.

## Encode Avro Records to Protocol Buffer Bytes

```java
IndexedRecord avroRecord = ...;
Map<String, Object> avroMap = new AvroWrapper().wrap(avroRecord);
ProtoWriter protoWriter = new ProtoWriter();
ByteBuffer protoBytes = protoWriter.write(avroMap, "Person");
```

## Convert Avro Bytes to Protocol Buffer Record Bytes

```java
Schema avroSchema = ...;
ByteBuffer avroBytes = ...;
Map<String, Object> avroData = new AvroReader().read(avroBytes, avroSchema);
ByteBuffer protoBytes = new ProtoWriter().write(avroData, "Person");
```

## Infer a Protocol Buffer Descriptor from an Avro Record

```java
IndexedRecord avroRecord = ...;
Map<String, Object> avroMap = new AvroWrapper().wrap(avroRecord);
Descriptor personDescriptor = new ProtoDescriptorInferrer().infer(avroMap, "Person");
```