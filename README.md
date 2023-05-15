# Twister

Twister converts Avro and Protobuf bytes and objects to and from Java POJOs. 'Cuz sometimes you just want some low-key POJOs.

Twister does some trippy stuff:

* Avro Record bytes ↔ Map POJO
* Protobuf Message bytes ↔ Map POJO
* Map POJO → Avro Schema
* Map POJO → Protobuf Descriptor

## Examples

Decode Avro and Protobuf bytes directly into POJOs, bypassing Avro Records and Protobuf Message data types.

Wrap Avro Record and Protobuf Message data types in a Java Map facade, so you can treat it like a Map POJO.

Infer Avro Schemas and Protobuf Descriptors.

## Missing Features

* Avro default support
* Avro logical type support
* Protobuf WKT support
* Avro Record → Map wrapper
* Protobuf Message → Map wrapper

## A GPT Experiment

Twister is written almost entirely by OpenAI's ChatGPT 4 LLM. Tests, docs, and even the converter code.
