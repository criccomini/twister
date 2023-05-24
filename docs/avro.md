---
layout: default
title: "Avro"
---

# Working With Avro
{: .no_toc }

Read and write Java objects as Avro bytes, infer Avro schemas from Java objects, and treat Avro records as Java maps.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Read Avro Bytes as POJOs

Read [Avro Record](https://avro.apache.org/docs/1.10.2/spec.html#schema_record) bytes as a Java `Map<String, Object>` using `AvroReader`. `AvroReader` bypasses Avro classes, so it is more efficient that reading bytes to Avro records and then converting to Java objects.

```java
ByteBuffer avroData = ...;
Schema schema = ...;
Map<String, Object> data = new AvroReader().read(avroData, schema);
```

The `AvroReader` supports Avro primitive and complex types, including logical types. Nested structures (Records of Arrays of Records, etc.) are also supported.

You can also provide custom logical type converters.

## Infer Avro Schemas from POJOs

Infer [Avro Schemas](https://avro.apache.org/docs/1.8.0/api/java/org/apache/avro/Schema.html) from POJOs using [`AvroSchemaInferrer`](https://github.com/criccomini/twister/blob/main/twister-avro/src/main/java/dev/twister/avro/AvroSchemaInferrer.java).

```java
Map<String, Object> data = new HashMap<>();
data.put("name", "John Doe");
data.put("age", 25);

Schema schema = new AvroSchemaInferrer().infer(data, "PersonRecord");
```

AvroSchemaInferrer supports Avro primitive and complex types, including logical types. Nested structures (Maps of Lists of Maps, etc.) are also supported.

You can also provide custom logical type inferrers to handle specific Java classes.

## Treat Avro Records as Java Maps

Transform [Avro Records](https://avro.apache.org/docs/1.10.2/spec.html#schema_record) into a Java `Map<String, Object>` using [`AvroWrapper`](https://github.com/criccomini/twister/blob/main/twister-avro/src/main/java/dev/twister/avro/AvroWrapper.java). This approach allows you to access Avro data using familiar Java collection interfaces.

`AvroWrapper` creates a facade over Avro records, arrays, and maps, presenting them as native Java collections. The wrapper only converts fields that are read, so performance penalties are only incurred for fields that are read. Nested structures are wrapped in facades as well, so even deeply nested structures are only converted as they are read.

```java
IndexedRecord record = ...;
Map<String, Object> data = new AvroWrapper().wrap(record);
```

You can also provide custom logical type converters.

## Write Java POJOs to Avro Bytes

Use [`AvroWriter`](https://github.com/criccomini/twister/blob/main/twister-avro/src/main/java/dev/twister/avro/AvroWriter.java) to write Java POJOs to Avro bytes.

`AvroWriter` requires a schema to write Avro bytes. You can either provide a specific Avro schema or let the `AvroWriter` infer the schema from the Java POJO.

Here's an example of how to write Java POJOs to Avro bytes using the `AvroWriter`:

```java
Map<String, Object> data = new HashMap<>();
data.put("name", "John Doe");
data.put("age", 25);

ByteBuffer avroBytes = new AvroWriter().write(data, "PersonRecord");
```

You can also provide a specific Avro schema instead of inferring it:

```java
Map<String, Object> data = new HashMap<>();
data.put("name", "John Doe");
data.put("age", 25);

Schema schema = ...;

ByteBuffer avroBytes = new AvroWriter().write(data, schema);
```

The `AvroWriter` class supports Avro primitive types, complex types, and logical types. Logical types such as decimal, uuid, date, time, and timestamp are automatically handled by the `AvroWriter` using the logical type writers defined in the code.

## Type Conversion

### Avro to Java

The following table shows how Avro types are converted to Java types:

| Avro Type | Java Type |
| --------- | --------- |
| `null` | `null` |
| `boolean` | `boolean` |
| `int` | `int` |
| `long` | `long` |
| `float` | `float` |
| `double` | `double` |
| `bytes` | `byte[]` |
| `string` | `String` |
| `array` | `List<T>` |
| `map` | `Map<K, V>` |
| `fixed` | `byte[]` |
| `record` | `Map<String, Object>` |
| `enum` | `String` |
| `union` | `Object` |
| `decimal` | `java.math.BigDecimal` |
| `uuid` | `java.util.UUID` |
| `date` | `java.time.LocalDate` |
| `time-millis` | `java.time.LocalTime` |
| `time-micros` | `java.time.LocalTime` |
| `timestamp-millis` | `java.time.Instant` |
| `timestamp-micros` | `java.time.Instant` |
| `local-timestamp-millis` | `java.time.LocalDateTime` |
| `local-timestamp-micros` | `java.time.LocalDateTime` |


For `array`, `map`, `fixed`, and `record` types, the corresponding Java types are collections (`List`, `Map`) or generic `Map` objects. The element types (`T` for `List<T>`, `K` and `V` for `Map<K, V>`) are inferred based on the contained Avro types.

For `union` types, the corresponding Java type is `Object`. The actual Java type of the object depends on the specific Avro type that is contained in the union. You can use the `instanceof` operator to determine the actual type of the object.

### Java to Avro

The following table shows how Java types are converted to Avro types:

| Java Type | Avro Type |
| --------- | --------- |
| `boolean` | `boolean` |
| `byte` | `int` |
| `short` | `int` |
| `int` | `int` |
| `long` | `long` |
| `float` | `float` |
| `double` | `double` |
| `char` | `string` |
| `String` | `string` |
| `byte[]` | `bytes` |
| `java.nio.ByteBuffer` | `bytes` |
| `java.math.BigDecimal` | `bytes` (logical type: `decimal`) |
| `java.util.UUID` | `string` (logical type: `uuid`) |
| `java.time.LocalDate` | `int` (logical type: `date`) |
| `java.time.LocalTime` | `int` (logical type: `time-millis`) |
| `java.time.LocalTime` | `long` (logical type: `time-micros`) |
| `java.time.Instant` | `long` (logical types: `timestamp-millis` or `timestamp-micros`) |
| `java.time.LocalDateTime` | `long` (logical types: `local-timestamp-millis` or `local-timestamp-micros`) |
| `List<T>` | `array` of Avro schema inferred from `T` |
| `Map<K, V>` | `map` with keys of Avro schema inferred from `K` and values of Avro schema inferred from `V` |

For `List<T>`, the Avro type is an `array` where the element type `T` is inferred based on its Java type.

For `Map<K, V>`, the Avro type is a `map` where the key type `K` and value type `V` are inferred based on their respective Java types.
