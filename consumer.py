from confluent_kafka import Consumer
from datetime import datetime, UTC
import json

print("starting Kafka Consumer")

consumer_config = {
    'bootstrap.servers': '100.82.145.53:9092',
    'group.id': 'telemetry.consumers',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['wifi.telemetry.data'])

print("listening for messages...")

try:
    while True:
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            continue

        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8')

        print(f"Partition: {msg.partition()} | Topic: {msg.topic()} | Key: {key} | Value: {value}")

except Exception as e:
    print(f"Exception raised: {e}")
finally:
    consumer.close()