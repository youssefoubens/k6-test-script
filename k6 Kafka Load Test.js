import { check, sleep } from "k6";
import {
  Writer,
  Reader,
  Connection,
  SchemaRegistry,
  KEY,
  VALUE,
  TOPIC_NAME_STRATEGY,
  RECORD_NAME_STRATEGY,
  SCHEMA_TYPE_AVRO,
} from "k6/x/kafka";

// Configuration
const brokers = ["localhost:9092"];
const inputTopic = "com.example.nested_event";
const outputTopic = "com.example.nested_event.processed";

// Setup Kafka
const writer = new Writer({
  brokers: brokers,
  topic: inputTopic,
  autoCreateTopic: true,
});

const reader = new Reader({
  brokers: brokers,
  topic: outputTopic,
});

const connection = new Connection({
  address: brokers[0],
});

const schemaRegistry = new SchemaRegistry({
  url: "http://localhost:8081",
});

// Create topics if needed
if (__VU == 0) {
  connection.createTopic({ topic: inputTopic });
  connection.createTopic({ topic: outputTopic });
}

// Avro schemas
const keySchema = `{
  "name": "KeySchema",
  "type": "record",
  "namespace": "com.example.key",
  "fields": [
    { "name": "id", "type": "string" }
  ]
}`;

const valueSchema = `{
  "type": "record",
  "name": "NestedEvent",
  "namespace": "com.example.avro",
  "fields": [
    {
      "name": "x",
      "type": {
        "type": "record",
        "name": "XRecord",
        "fields": [
          {
            "name": "y",
            "type": {
              "type": "record",
              "name": "YRecord",
              "fields": [
                {
                  "name": "z",
                  "type": {
                    "type": "record",
                    "name": "ZRecord",
                    "fields": [
                      { "name": "t", "type": "string" }
                    ]
                  }
                }
              ]
            }
          }
        ]
      }
    }
  ]
}`;

// Register schemas
const keySubjectName = schemaRegistry.getSubjectName({
  topic: inputTopic,
  element: KEY,
  subjectNameStrategy: TOPIC_NAME_STRATEGY,
  schema: keySchema,
});

const valueSubjectName = schemaRegistry.getSubjectName({
  topic: inputTopic,
  element: VALUE,
  subjectNameStrategy: RECORD_NAME_STRATEGY,
  schema: valueSchema,
});

const keySchemaObject = schemaRegistry.createSchema({
  subject: keySubjectName,
  schema: keySchema,
  schemaType: SCHEMA_TYPE_AVRO,
});

const valueSchemaObject = schemaRegistry.createSchema({
  subject: valueSubjectName,
  schema: valueSchema,
  schemaType: SCHEMA_TYPE_AVRO,
});

// Test function
export default function () {
  // Produce 100 nested messages
  for (let index = 0; index < 100; index++) {
    const valueData = {
      x: {
        y: {
          z: {
            t: "value-" + index
          }
        }
      }
    };

    const message = {
      key: schemaRegistry.serialize({
        data: { id: "id-" + index },
        schema: keySchemaObject,
        schemaType: SCHEMA_TYPE_AVRO,
      }),
      value: schemaRegistry.serialize({
        data: valueData,
        schema: valueSchemaObject,
        schemaType: SCHEMA_TYPE_AVRO,
      }),
    };

    writer.produce({ messages: [message] });
  }

  // Allow your Spring Boot app to process
  sleep(5);

  // Consume 20 processed messages
  const messages = reader.consume({ limit: 20 });

  // Validate processing
  check(messages, {
    "20 messages returned": (msgs) => msgs.length === 20,
    "all 't' fields processed correctly": (msgs) => msgs.every((msg) => {
      const value = schemaRegistry.deserialize({
        data: msg.value,
        schema: valueSchemaObject,
        schemaType: SCHEMA_TYPE_AVRO,
      });
      return value.x.y.z.t.endsWith(" - processed");
    }),
  });
}

// Cleanup
export function teardown() {
  if (__VU == 0) {
    connection.deleteTopic(inputTopic);
    connection.deleteTopic(outputTopic);
  }
  writer.close();
  reader.close();
  connection.close();
}