import json
import logging
from confluent_kafka import Consumer, Producer
from pydantic import ValidationError
from config import Config
from schema import Telemetry
from storage import archive_raw, audit, initialize_schema, save_reading

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def publish(producer, topic, payload):
    producer.produce(topic, key=payload.get("instrument_id"), value=json.dumps(payload, default=str))
    producer.poll(0)

def main():
    initialize_schema()
    consumer = Consumer({"bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS, "group.id": Config.CONSUMER_GROUP, "auto.offset.reset": "earliest", "enable.auto.commit": False})
    output = Producer({"bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([Config.RAW_TOPIC])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None: continue
            if message.error(): logging.error("Kafka error: %s", message.error()); continue
            try:
                payload = json.loads(message.value())
                telemetry = Telemetry.model_validate(payload)
                normalized = telemetry.model_dump(mode="json")
                raw_id = archive_raw(payload, "validated")
                save_reading(normalized, raw_id)
                audit("telemetry_validated", payload, message)
                publish(output, Config.VALIDATED_TOPIC, normalized)
            except (json.JSONDecodeError, ValidationError, ValueError) as error:
                payload = locals().get("payload", {"raw": message.value().decode(errors="replace")})
                archive_raw(payload, "invalid", str(error))
                audit("telemetry_rejected", payload, message, str(error))
                publish(output, Config.INVALID_TOPIC, {"payload": payload, "reason": str(error)})
            consumer.commit(message=message, asynchronous=False)
    finally:
        output.flush(5); consumer.close()

if __name__ == "__main__": main()
