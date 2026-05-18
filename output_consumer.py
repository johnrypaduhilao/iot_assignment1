import json
import signal
import sys
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

from kafka_config import KafkaConfig

TOPIC_NAME = "predictions"
GROUP_ID = "output-consumer-final"
POLL_TIMEOUT = 1.0


def build_consumer() -> Consumer:
    config = KafkaConfig().set_config()

    config.update({
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest", 
        "enable.auto.commit": True,   
    })
    return Consumer(config)


def format_message(payload: dict) -> str:
    ts = datetime.now().strftime("%H:%M:%S")
    key = payload.get("key", "?")
    actual = payload.get("actual_co", "?")
    predicted = payload.get("predicted_class", "?")
    status = payload.get("status", "?")
    
    actual_display = "MISSING" if actual == -200.0 else str(actual)

    return f"[{ts}]  {key:<22}  actual={str(actual_display):<6}  predicted={str(predicted):<8}  [{status}]"


def main() -> None:
    consumer = build_consumer()
    consumer.subscribe([TOPIC_NAME])
    print(f"Subscribed to '{TOPIC_NAME}'. Waiting for predictions...\n")

    def handle_sigint(_sig, _frame):
        print("\nStopping consumer...")
        consumer.close()
        sys.exit(0)
    signal.signal(signal.SIGINT, handle_sigint)

    count = 0
    while True:
        msg = consumer.poll(POLL_TIMEOUT)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"  consumer error: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"  bad message, skipping: {e}")
            continue

        count += 1
        print(f"  #{count:4d}  {format_message(payload)}")


if __name__ == "__main__":
    main()