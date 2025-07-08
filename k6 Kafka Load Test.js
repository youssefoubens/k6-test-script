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

const writer = new Writer({
    brokers,
    topic: inputTopic,
    autoCreateTopic: true,
});

const reader = new Reader({
    brokers,
    topic: outputTopic,
});

const connection = new Connection({
    address: brokers[0],
});

const schemaRegistry = new SchemaRegistry({
    url: "http://localhost:8081",
});

// Decimal encoder helper
function decimalToBytes(decimalStr, scale) {
    let scaled = BigInt(Math.round(parseFloat(decimalStr) * Math.pow(10, scale)));
    let hex = scaled.toString(16);
    if (hex.length % 2 !== 0) hex = "0" + hex;

    const len = hex.length / 2;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
    }
    return bytes;
}

// Avro schemas
const keySchema = `{
    "type": "record",
    "name": "KeySchema",
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
                                            { "name": "t", "type": "string" },
                                            {
                                                "name": "amount",
                                                "type": ["null", "bytes"],
                                                "logicalType": "decimal",
                                                "precision": 10,
                                                "scale": 2,
                                                "default": null
                                            }
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

export function setup() {
    try {
        connection.createTopic({ topic: inputTopic });
    } catch (_) {}
    try {
        connection.createTopic({ topic: outputTopic });
    } catch (_) {}

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

    schemaRegistry.createSchema({
        subject: keySubjectName,
        schema: keySchema,
        schemaType: SCHEMA_TYPE_AVRO,
    });

    schemaRegistry.createSchema({
        subject: valueSubjectName,
        schema: valueSchema,
        schemaType: SCHEMA_TYPE_AVRO,
    });
}

export default function () {
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

    for (let index = 0; index < 100; index++) {
        const encodedDecimal = decimalToBytes("89.45", 2);

        const valueData = {
            x: {
                y: {
                    z: {
                        t: `value-${index}`,
                        amount: { bytes: encodedDecimal }
                    },
                },
            },
        };

        const message = {
            key: schemaRegistry.serialize({
                data: { id: `id-${index}` },
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

    sleep(5);

    const messages = reader.consume({ limit: 20 });

    check(messages, {
        "20 messages returned": (msgs) => msgs.length === 20,
        "all 't' fields processed correctly": (msgs) =>
            msgs.every((msg) => {
                const value = schemaRegistry.deserialize({
                    data: msg.value,
                    schema: valueSchemaObject,
                    schemaType: SCHEMA_TYPE_AVRO,
                });
                return value.x.y.z.t.endsWith(" - processed");
            }),
    });
}

export function teardown() {
    if (__ENV.CLEANUP === "true") {
        try {
            connection.deleteTopic({ topic: inputTopic });
        } catch (_) {}
        try {
            connection.deleteTopic({ topic: outputTopic });
        } catch (_) {}
    }
    writer.close();
    reader.close();
    connection.close();
}
