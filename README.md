# Twister

Twister converts [Avro](https://avro.apache.org/) and [Protobuf](https://protobuf.dev) bytes and objects to and from Java POJOs. 'Cuz sometimes you just want some low-key POJOs.

Twister does some trippy stuff:

* Avro Record bytes ↔️ Map POJOs
* Avro Record objects ➡️ Map wrapper
* Protobuf Message bytes ↔️ Map POJOs
* Protobuf Message objects ➡️ Map wrapper
* Map POJOs ➡️ Avro Schema
* Map POJOs ➡️ Protobuf Descriptor

**Note**: Twister also allows you to convert between Avro/Proto and JSON objects using [Jackson's ObjectMapper](https://github.com/FasterXML/jackson-databind), which can convert map POJOs ↔️ JSON.

## Usage

### Protobuf

Decode Protobuf message bytes directly into POJO maps, bypassing Protobuf message classes:

```java
Map<String, Object> object = new ProtoReader().read(bytes, descriptor);
```

Wrap an existing Protobuf message in a POJO map:

```java
Map<String, Object> wrapped = new ProtoWrapper().wrap(message);
```

Encode POJO objects directly into Protobuf bytes:

```java
ByteBuffer protobufBytes = new ProtoWriter().write(object, "MessageName");
```

And infer Protobuf message descriptors from POJO maps:

```java
Descriptors.Descriptor descriptor = new ProtoDescriptorInferrer().infer(object, "MessageName");
```

### Avro

Decode Avro bytes directly into POJO objects, bypassing Avro record classes:

```java
Map<String, Object> object = new AvroReader().read(bytes, schema);
```
Wrap an existing Avro record in a POJO map:

```java
Map<String, Object> wrapped = new AvroWrapper().wrap(record);
```

Encode POJO objects directly into Avro bytes:

```java
ByteBuffer avroBytes = new AvroWriter().write(object, "RecordName");
```

And infer Avro Record schemas from POJO maps:

```java
Schema schema = new AvroSchemaInferrer().infer(object, "RecordName");
```

## Getting Started

For `twister-avro`:

```xml
<dependencies>
    <dependency>
        <groupId>dev.twister</groupId>
        <artifactId>twister-avro</artifactId>
        <version>0.1.1</version>
    </dependency>
</dependencies>
```

```groovy
dependencies {
    implementation 'dev.twister:twister-avro:0.1.1'
}
```

For `twister-proto`:

```xml
<dependencies>
    <dependency>
        <groupId>dev.twister</groupId>
        <artifactId>twister-proto</artifactId>
        <version>0.1.1</version>
    </dependency>
</dependencies>
```

```groovy
dependencies {
    implementation 'dev.twister:twister-proto:0.1.1'
}
```

## Why?

Working with Avro and Protobuf can be complex due to their schema-based data encoding. Twister simplifies this by allowing users to operate on these data as Plain Old Java Objects (POJOs), making it a more natural fit for Java applications.

Key benefits of Twister include:

* **Ease of Use**: Handles Avro and Protobuf intricacies, letting developers focus on data manipulation with familiar Java collections.

* **Flexibility**: Allows seamless conversion between Avro and Protobuf data and POJOs, useful in systems dealing with both data types.

* **Dynamic Inference**: Infers Avro schemas or Protobuf descriptors from POJO maps at runtime, enabling dynamic data handling.

* **Interoperability**: Facilitates conversions between Protobuf messages and Avro records, enhancing system compatibility.

Despite its simplicity and flexibility, Twister attempts to maintain decent performance. It directly decodes and encodes bytes to POJOs, rather than going through intermediary Protobuf Messages or Avro Records, ensuring more efficient operation.

## Future Work

* Protobuf WKT support
* JDBC row ➡️ Map wrapper

## 🤖 GPT-4 Generated

Twister is written almost entirely by [OpenAI's ChatGPT 4 LLM](https://openai.com/product/gpt-4). This includes not only Twister's code, but its tests, docs, commit messages, and even this README.

Check out the [AI at the Helm: Building an Entire Open Source Project With GPT-4](https://cnr.sh/essays/ai-helm-building-open-source-project-gpt-4) to learn more about building Twister with GPT-4.
