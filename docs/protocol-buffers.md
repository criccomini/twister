---
layout: default
title: "Protocol Buffers"
---

# Working With Protocol Buffers
{: .no_toc }

Read and write Java objects as Protocol Buffer bytes, infer Protocol Buffer descriptors from Java objects, and treat Procotol Buffer messages as Java maps.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Read Protocol Buffer Bytes as POJOs

Read [Protocol Buffers](https://developers.google.com/protocol-buffers) message bytes as a Java `Map<String, Object>` using [`ProtoReader`](https://github.com/criccomini/twister/blob/main/twister-proto/src/main/java/dev/twister/proto/ProtoReader.java). `ProtoReader` allows you to deserialize Protocol Buffers messages directly into a map representation, bypassing the need for specific Protocol Buffers message classes.

```java
ByteBuffer protobufData = ...;
Descriptors.Descriptor descriptor = ...;
Map<String, Object> data = ProtoReader.read(protobufData, descriptor);
```

The `ProtoReader` supports Protocol Buffer primitive and complex types (maps, enums, repeated). Nested structures (Messages of repeated Messages, etc.) are also supported.

## Infer Protocol Buffer Descriptors from POJOs

Infer [Protocol Buffers](https://developers.google.com/protocol-buffers) message descriptors from a `Map<String, Object>` using [`ProtoDescriptorInferrer`](https://github.com/criccomini/twister/blob/main/twister-proto/src/main/java/dev/twister/proto/ProtoDescriptorInferrer.java). `ProtoDescriptorInferrer` automatically generates Protocol Buffers message descriptors based on the structure of a `Map` Java object.

```java
Map<String, Object> object = new HashMap<>();
object.put("name", "John Doe");
object.put("age", 25);

Descriptors.Descriptor descriptor = new ProtoDescriptorInferrer().infer(object, "PersonMessage");
```

## Treat Protocol Buffer Messages as Java Maps

Convert [Protocol Buffer Messages](https://developers.google.com/protocol-buffers/docs/proto3) into a Java `Map<String, Object>` using [`ProtoWrapper`](https://github.com/criccomini/twister/blob/main/twister-proto/src/main/java/dev/twister/proto/ProtoWrapper.java). This provides a convenient way to work with Protobuf messages using familiar Java collection interfaces.

`ProtoWrapper` creates a facade over Protobuf messages, repeated fields and nested structures, presenting them as native Java collections. The wrapper only converts fields that are accessed, so performance penalties are only incurred for fields that are used. Nested messages and repeated fields are wrapped into facades as well, so even deeply nested structures are only converted as they are read

```java
Message message = ...;
Map<String, Object> data = new ProtoWrapper().wrap(message);
```

Fields are converted to more Java-friendly types when necessary. For example, byte fields are converted to ByteBuffer instances, and enum fields are converted to the names of the enum values. The class also handles 'oneof' fields: only the field that is currently set in the 'oneof' is included in the map.

## Write Java POJOs to Protocol Buffer Bytes

Use [`ProtoWriter`](https://github.com/criccomini/twister/blob/main/twister-proto/src/main/java/dev/twister/proto/ProtoWriter.java) to write Java Maps to Protocol Buffers bytes.

`ProtoWriter` requires a descriptor to write Protocol Buffers bytes. You can either provide a specific Protocol Buffers descriptor or let the `ProtoWriter` infer the descriptor from the Java Map.

Here's an example of how to write Java Maps to Protocol Buffers bytes using the `ProtoWriter`:

```java
Map<String, Object> data = new HashMap<>();
data.put("name", "John Doe");
data.put("age", 25);

ByteBuffer protoBytes = new ProtoWriter().write(data, "PersonRecord");
```

You can also provide a specific Protocol Buffers descriptor instead of inferring it:

```java
Map<String, Object> data = new HashMap<>();
data.put("name", "John Doe");
data.put("age", 25);

Descriptor descriptor = ...;

ByteBuffer protoBytes = new ProtoWriter().write(data, descriptor);
```

The `ProtoWriter` class supports Protocol Buffers basic types, complex types, and enum types. It also supports nested structures (Messages of repeated Messages, etc.).


## Type Conversion

### Protocol Buffer to Java

The following table shows how Protocol Buffer types are converted to Java types:

| Protocol Buffer Type | Java Type |
| --------- | --------- |
| `BOOL` | `Boolean` |
| `INT32` | `Integer` |
| `SINT32` | `Integer` |
| `SINT64` | `Long` |
| `ENUM` | `String` (Enum name) |
| `UINT32`, `FIXED32` | `Long` |
| `UINT64`, `FIXED64` | `java.math.BigInteger` |
| `DOUBLE` | `Double` |
| `FIXED64` | `java.math.BigInteger` |
| `SFIXED64` | `Long` |
| `STRING` | `String` |
| `BYTES` | `java.nio.ByteBuffer` |
| `MESSAGE` | `java.util.Map<String, Object>` |
| `FLOAT` | `Float` |
| `FIXED32` | `Long` |
| `SFIXED32` | `Integer` |

In the case of repeated fields, values are placed into a `java.util.List<Object>`. Map fields (fields of type `MESSAGE` with map entry option set) are converted to `java.util.Map<Object, Object>`. Oneof fields are represented in the resulting map as a key-value pair where the key is the name of the chosen field and the value is the value of that field.

### Java to Protocol Buffers

The following table shows how Java types are converted to Protocol Buffer types:

| Java Type | Protocol Buffer Type |
| --------- | --------- |
| `Integer`   | `INT32`     |
| `Long`      | `INT64`     |
| `BigInteger` | `UINT64` (Note: only fits in `uint64` if its value is between 0 and `Long.MAX_VALUE` inclusive) |
| `Boolean`   | `BOOL`      |
| `String`    | `STRING`    |
| `Double`    | `DOUBLE`    |
| `ByteBuffer`| `BYTES`     |
| `Float`     | `FLOAT`     |
| `Map`       | `MESSAGE`   |
| `List`      | Repeated field type (assuming all elements in the list are of the same type) |

Note: All fields in a message are treated as optional in this inference process. The `LABEL_REPEATED` label is used for Lists, whereas the `LABEL_OPTIONAL` label is used for other types, including nested messages (Maps).