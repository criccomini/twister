---
layout: default
title: "Home"
nav_order: 1
---

# Twister
{: .no_toc }

Convert Avro and Protobuf bytes and objects to and from Java POJOs.
{: .fs-6 .fw-300 }


| From                     | To                      | Supported  |
|--------------------------|-------------------------|------------|
| Avro Record bytes        | Map POJOs               | ✅️         |
| Avro Record objects      | Map wrapper             | ✅️         |
| Map POJOs                | Avro Record bytes       | ✅️         |
| Map POJOs                | Avro Schema             | ✅️         |
| Map POJOs                | Protobuf Descriptor     | ✅️         |
| Map POJOs                | Protobuf Message bytes  | ✅️         |
| Protobuf Message bytes   | Map POJOs               | ✅️         |
| Protobuf Message objects | Map wrapper             | ✅️         |

## About Twister
[Twister](https://github.com/criccomini/twister) is an open-source project that converts between Avro and Protobuf bytes and objects and Java Plain Old Java Objects ([POJOs](https://en.wikipedia.org/wiki/Plain_old_Java_object)).

Libraries that process data must have a data model. Library developers are forced to pick:

1. Use a single data model like Proto, Avro, or JSON. This is too rigid for users.
2. Make the data model pluggable. This is cumbersome for users.

Twister allows developers to use standard Java objects in their code, and convert them to and from [Avro](https://avro.apache.org/), [Protobuf](https://protobuf.dev), or JSON data types under the hood.

## Features

1. **Bi-directional:** Twister converts both ways - you can convert Avro and Protobuf data to Java POJOs and vice versa.

2. **Efficient:** Twister encodes and decodes data to and from Java POJOs efficiently, without using Avro record or Protobuf message classes.

3. **Wrappers:** If you already use records or messages, you can use Twister's wrapper classes to treat them as POJOs without any performance penalty.

3. **Easy:** Twister is designed with simplicity in mind. Its API is clean, straightforward, and easy to use.

## Getting Started

Add Twister to your Maven pom.xml:

```xml
<dependencies>
    <dependency>
        <groupId>dev.twister</groupId>
        <artifactId>twister-avro</artifactId>
        <version>0.2.0</version>
    </dependency>
    <dependency>
        <groupId>dev.twister</groupId>
        <artifactId>twister-proto</artifactId>
        <version>0.2.0</version>
    </dependency>
</dependencies>
```

Or build.gradle:

```groovy
dependencies {
    implementation 'dev.twister:twister-avro:0.2.0'
    implementation 'dev.twister:twister-proto:0.2.0'
}
```

And check out the documentation!