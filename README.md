# Twister

[Twister](https://twister.dev) converts [Avro](https://avro.apache.org/) and [Protobuf](https://protobuf.dev) bytes and objects to and from Java [POJOs](https://en.wikipedia.org/wiki/Plain_old_Java_object).

| From                     | To                      | Supported  |
|--------------------------|-------------------------|------------|
| Avro Record bytes        | Map POJOs               | ☑️         |
| Avro Record objects      | Map wrapper             | ☑️         |
| Map POJOs                | Avro Record bytes       | ☑️         |
| Map POJOs                | Avro Schema             | ☑️         |
| Map POJOs                | Protobuf Descriptor     | ☑️         |
| Map POJOs                | Protobuf Message bytes  | ☑️         |
| Protobuf Message bytes   | Map POJOs               | ☑️         |
| Protobuf Message objects | Map wrapper             | ☑️         |

**Note**: Twister makes it easy to convert between Avro/Proto and JSON objects using [Jackson's ObjectMapper](https://github.com/FasterXML/jackson-databind), which can convert map POJOs ↔️ JSON.

## Documentation

Check out [https://twister.dev](https://twister.dev) for documentation and examples.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](https://github.com/criccomini/twister/blob/main/CONTRIBUTING.md).

## Made With ❤️ by a Robot

Twister is written almost entirely by [OpenAI's ChatGPT 4 LLM](https://openai.com/product/gpt-4). This includes not only Twister's code, but its tests, docs, commit messages, and even this README.

Check out the [AI at the Helm: Building an Entire Open Source Project With GPT-4](https://cnr.sh/essays/ai-helm-building-open-source-project-gpt-4) to learn more about building Twister with GPT-4.
