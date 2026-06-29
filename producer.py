import json
import random
import device
from confluent_kafka import Producer

# pi_ips = {'100.64.128.15', '100.87.172.17'}
pi_ips = {'100.82.145.53'} # -> pi-node1

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

kafka_topic_name = "wifi.telemetry.data"

producer_config = {
    'bootstrap.servers': '100.82.145.53:9092',
}
producer = Producer(producer_config)

print("Fetching device identity over SSH...")
devices = [device.Device(**device.get_device_data(ip)) for ip in pi_ips]
print(f"Built {len(devices)} device(s). Starting stream...")

try:
    for _ in range(20):
        d = random.choice(devices)
        reading = d.generate_reading()
        producer.produce(
            topic=kafka_topic_name,
            key=d.device_id.encode('utf-8'),
            value=json.dumps(reading).encode('utf-8'),
            on_delivery=delivery_report,
        )
        producer.poll(0)
    producer.flush()

except Exception as e:
    print(f"Exception occured: {e}")

finally:
    print("Kafka producer stopped")
