"""Step 2 — cross-network consumer (Mac -> Kafka on Pi-node1).

Subscribes to the topic and prints every message. Leave it running, then run
`producer.py` in another terminal. Ctrl-C to stop.
"""
import json

from confluent_kafka import Consumer

from config import KAFKA_BROKER, TOPIC


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "telemetry-readers",      # consumers in the same group split partitions
        "auto.offset.reset": "earliest",      # on first run, read from the start of the topic
    })
    consumer.subscribe([TOPIC])
    print(f"listening on '{TOPIC}' via {KAFKA_BROKER} ... Ctrl-C to stop")

    try:
        while True:
            msg = consumer.poll(1.0)  # wait up to 1s for a record
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue
            event = json.loads(msg.value().decode("utf-8"))
            print(f"got: {event}  [partition {msg.partition()} offset {msg.offset()}]")
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        consumer.close()  # commit final offsets and leave the group cleanly


if __name__ == "__main__":
    main()
