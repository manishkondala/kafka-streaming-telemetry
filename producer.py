"""Step 2 — cross-network producer (Mac -> Kafka on Pi-node1).

Sends 10 simple JSON messages so we can prove the network connection works
BEFORE we add the real telemetry schema. Run `consumer.py` in another terminal
to watch them arrive.
"""
import json
import time

from confluent_kafka import Producer

from config import KAFKA_BROKER, TOPIC


def delivery_report(err, msg):
    """Called once per message after the broker acks (or fails) it."""
    if err is not None:
        print(f"FAILED: {err}")
    else:
        print(f"delivered -> {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    print(f"producing to '{TOPIC}' via {KAFKA_BROKER}")

    for i in range(10):
        event = {"seq": i, "msg": f"hello from the mac #{i}"}
        producer.produce(
            TOPIC,
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # serve delivery callbacks without blocking
        time.sleep(0.5)

    producer.flush()  # block until every queued message is delivered or fails
    print("done.")


if __name__ == "__main__":
    main()
