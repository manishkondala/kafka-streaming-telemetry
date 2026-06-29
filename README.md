# Wi-Fi Telemetry Pipeline

A streaming Wi-Fi/TCP telemetry analytics pipeline, built as a clean-room
re-creation of a production system: synthetic 802.11 + TCP telemetry →
**Apache Kafka** → Python consumer → **PostgreSQL** → SQL window-function
transforms → buffering-prediction model.

## Architecture

```
synthetic producer  -->  Kafka (Pi-node1)  -->  Python consumer  -->  PostgreSQL (Pi)
 (802.11 + TCP)          (apache/kafka)         (validate/batch)      (SQL transforms)
                                                                            |
                                                              buffering-prediction model
```

- **Kafka broker:** `apache/kafka:latest` (KRaft, no ZooKeeper) running in Docker on **Pi-node1**.
- **Clients:** Python (`confluent-kafka`) running on the Mac, over the LAN — so the
  broker must advertise its **Pi LAN address**, not `localhost` (see below).
- **Storage:** PostgreSQL on the Pi (row-store OLTP; window functions for the transforms).

## Run

```bash
pip install -r requirements.txt
export KAFKA_BROKER="192.168.x.x:9092"   # the Pi's IP (run `hostname -I` on the Pi)

python consumer.py     # terminal 1 — leave running
python producer.py     # terminal 2 — sends 10 messages
```

## The cross-network gotcha: `advertised.listeners`

The CLI produce/consume worked *inside* the container because the broker advertises
`localhost` — which, inside the container, IS the broker. From the Mac, `localhost`
means the Mac, so a remote client connects, gets told "reach me at localhost:9092,"
dials its own machine, and fails.

Fix on the **broker** (the Pi): set `KAFKA_ADVERTISED_LISTENERS` to the Pi's LAN IP,
publish port 9092 on the host, and allow 9092 through the Pi's firewall.

## Status

- [x] Kafka broker up on Pi-node1, manual CLI produce/consume verified
- [x] Cross-network Python producer/consumer
- [x] Synthetic 802.11 + TCP telemetry schema
- [ ] Consumer: validate -> dead-letter -> Postgres
- [ ] SQL window-function transforms
- [ ] Buffering-prediction model
