"""Central config. The Python client runs on your Mac; the Kafka broker runs on
Pi-node1. So KAFKA_BROKER must be the Pi's LAN address — NOT localhost.

Set it in your shell before running:
    export KAFKA_BROKER="192.168.x.x:9092"   # the Pi's IP, from `hostname -I` on the Pi
"""
import os

# bootstrap.servers — the address the client first dials to discover the cluster.
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

# Topic we produce to / consume from.
TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry")
