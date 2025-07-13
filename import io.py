import io
import json
import decimal
import time
from locust import User, task, between, events
from confluent_kafka import Producer, Consumer, KafkaError
import fastavro
import requests

# Your existing configuration
BROKERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
INPUT_TOPIC = "com.example.nested_event"
OUTPUT_TOPIC = "com.example.nested_event.processed"

# Your existing helper functions (unchanged)
def register_schema(subject, schema_str):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    resp = requests.post(url, json={"schema": schema_str})
    if resp.status_code == 409:
        latest = get_latest_schema(subject)
        return latest["id"]
    resp.raise_for_status()
    return resp.json()["id"]

def get_latest_schema(subject):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def decimal_to_bytes(dec, precision=20, scale=10):
    unscaled = int(dec * (10 ** scale))
    length = (unscaled.bit_length() + 8) // 8 or 1
    return unscaled.to_bytes(length, byteorder="big", signed=True)

def parse_schema(schema_str):
    return fastavro.parse_schema(json.loads(schema_str))

producer_conf = {"bootstrap.servers": BROKERS}
consumer_conf = {
    "bootstrap.servers": BROKERS,
    "group.id": "locust-consumer-group",
    "auto.offset.reset": "earliest",
}

class KafkaUser(User):
    wait_time = between(1, 3)

    def on_start(self):
        # Your existing schema definitions
        self.key_schema_str = """
        {
            "type": "record",
            "name": "KeySchema",
            "namespace": "com.example.key",
            "fields": [{"name": "id", "type": "string"}]
        }
        """
        self.value_schema_str = """
        {
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
                                                    {"name": "t", "type": "string"},
                                                    {
                                                        "name": "amount",
                                                        "type": [
                                                            "null",
                                                            {
                                                                "type": "bytes",
                                                                "logicalType": "decimal",
                                                                "precision": 20,
                                                                "scale": 10
                                                            }
                                                        ],
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
        }
        """

        self.key_subject = f"{INPUT_TOPIC}-key"
        self.value_subject = f"{INPUT_TOPIC}-value"
        self.key_schema_id = register_schema(self.key_subject, self.key_schema_str)
        self.value_schema_id = register_schema(self.value_subject, self.value_schema_str)
        self.key_schema = parse_schema(self.key_schema_str)
        self.value_schema = parse_schema(self.value_schema_str)
        self.producer = Producer(producer_conf)
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([OUTPUT_TOPIC])

    def avro_serialize(self, data, schema, schema_id):
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, data)
        raw_bytes = buf.getvalue()
        result = b"\0" + schema_id.to_bytes(4, byteorder="big") + raw_bytes
        return result

    def avro_deserialize(self, data, schema):
        buf = io.BytesIO(data)
        buf.read(5)
        return fastavro.schemaless_reader(buf, schema)

    @task
    def produce_and_consume_with_stats(self):
        """Modified version of your original task WITH statistics tracking"""
        start_time = time.time()

        try:
            print("[Locust] Producing 100 messages...")

            # Produce 100 messages
            for i in range(100):
                key = {"id": f"id-{i}"}
                dec_val = decimal.Decimal("500.54")
                dec_bytes = decimal_to_bytes(dec_val)
                value = {
                    "x": {
                        "y": {
                            "z": {
                                "t": f"value-{i}",
                                "amount": dec_bytes
                            }
                        }
                    }
                }
                key_bytes = self.avro_serialize(key, self.key_schema, self.key_schema_id)
                value_bytes = self.avro_serialize(value, self.value_schema, self.value_schema_id)
                self.producer.produce(INPUT_TOPIC, key=key_bytes, value=value_bytes)

            self.producer.flush()
            produce_end_time = time.time()

            print("[Locust] Consuming 20 processed messages...")

            # Consume 20 processed messages
            messages = []
            consume_start = time.time()

            while len(messages) < 20 and time.time() - consume_start < 10:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print("[Locust] Kafka error:", msg.error())
                    continue
                value = self.avro_deserialize(msg.value(), self.value_schema)
                if value["x"]["y"]["z"]["t"].endswith(" - processed"):
                    messages.append(value)

            # Validation
            if len(messages) != 20:
                raise Exception(f"Expected 20 messages, got {len(messages)}")

            if not all(m["x"]["y"]["z"]["t"].endswith(" - processed") for m in messages):
                raise Exception("Validation failed")

            # Calculate timings
            total_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            produce_time = (produce_end_time - start_time) * 1000
            consume_time = (time.time() - consume_start) * 1000

            print(f"[Locust] Task complete: 20 processed messages verified.")
            print(f"[Locust] Timings - Total: {total_time:.2f}ms, Produce: {produce_time:.2f}ms, Consume: {consume_time:.2f}ms")

            # Fire success event - THIS IS THE KEY PART!
            events.request.fire(
                request_type="kafka_worker",
                name="produce_and_consume_100_20",
                response_time=total_time,
                response_length=120,  # 100 produced + 20 consumed
                context=self.context(),
                exception=None
            )

        except Exception as e:
            # Fire failure event
            events.request.fire(
                request_type="kafka_worker",
                name="produce_and_consume_100_20",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                context=self.context(),
                exception=e
            )
            raise

    def on_stop(self):
        self.consumer.close()
        self.producer.flush()
        print("[Locust] KafkaUser stopped and resources cleaned up.")