import json
from datetime import datetime, UTC
import device
from confluent_kafka import Producer

pi_ips = {'100.64.128.15', '100.87.172.17'}
pi_ips = {'100.82.145.53'} # -> pi-node1

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

kafka_topic_name = "wifi.telemetry.data"

print("Starting Kafka Producer...")

producer_config = {
    'bootstrap.servers' : '100.82.145.53:9092',
}

print("connecting to Kafka Topic...")

producer = Producer(producer_config)

try:
    for ip in pi_ips:
        data = device.get_device_data(ip)
        print("------------")
        print(data)
        print("------------")
        producer.produce(topic=kafka_topic_name, key=data["device_id"].encode('utf-8'), value=json.dumps(data).encode('utf-8'), on_delivery=delivery_report)
        producer.flush()

except Exception as e:
    print(f"Exception occured: {e}")

finally:
    print("Kafka producer stopped)")
