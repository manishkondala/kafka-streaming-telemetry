from confluent_kafka import Consumer
import psycopg2
import json

print("Connecting to DB")

conn = psycopg2.connect(host='100.82.145.53', port=5432, dbname='wifi_telemetry', user='telemetry', password='telemetry')
cur = conn.cursor()

print("Connection to DB SUCCESSFUL")
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

        data = json.loads(msg.value().decode('utf-8'))

        cur.execute(
            """
            INSERT INTO wifi_telemetry_streaming_data
            (device_id, device_mac, timestamp_utc, freq_band, channel_num,
             rtt_ms, link_budget_db, avg_link_utilization_pct, cwnd_bytes, rwnd_bytes, buffer_event)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data["device_id"],
                data["device_mac"],
                data["timestamp_utc"],
                data["freq_band"],
                data["channel_num"],
                data["rtt_ms"],
                data["link_budget_db"],
                data["avg_link_utilization_pct"],
                data["cwnd_bytes"],
                data["rwnd_bytes"],
                data["buffer_event"],
            ),
        )
        conn.commit()

        print(f"Inserted | partition {msg.partition()} | {data['device_id']} | buffer_event={data['buffer_event']}")

except Exception as e:
    print(f"Exception raised: {e}")
finally:
    cur.close()
    conn.close()
    consumer.close()
