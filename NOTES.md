# Study Notes — SpaceX DE Interview

Skim this Tuesday morning. Everything framed on the telemetry data in this repo.

---

## OLTP vs OLAP — two different *jobs* a database does

| | **OLTP** (Online Transaction Processing) | **OLAP** (Online Analytical Processing) |
|---|---|---|
| Job | Run the **app** | Analyze the **app** |
| Operations | Many tiny reads/writes, a few rows each | Few huge scans, millions of rows each |
| Example | "insert this reading", "fetch terminal X" | "p95 RTT across 900K terminals per hour" |
| Tools | **Postgres**, MySQL | **BigQuery**, Snowflake, Redshift |

> One-liner: *"OLTP is for running the app — small, fast, row-at-a-time. OLAP is for
> analyzing it — big scans, aggregations over millions of rows. Telemetry analytics
> is fundamentally OLAP."*

This project **ingests** like OLTP (a stream of individual readings) but its **value**
(rolling avg, p95, top-K) is OLAP.

---

## Row vs columnar — *how* the data sits on disk

The table:

```
 terminal_id │  ts    │ rssi │ rtt_ms │ retx
─────────────┼────────┼──────┼────────┼──────
     t1      │ 10:00  │ -55  │   40   │   2
     t2      │ 10:00  │ -70  │   95   │  11
     t3      │ 10:01  │ -60  │   50   │   3
```

A disk is one long line of bytes. The DB type just changes the *order* cells are written.

### Row storage (Postgres) — one full row, then the next
```
┌──────────────────────┐┌──────────────────────┐┌──────────────────────┐
│ t1 10:00 -55 40 2    ││ t2 10:00 -70 95 11   ││ t3 10:01 -60 50 3    │
└──────────────────────┘└──────────────────────┘└──────────────────────┘
  \___ row 1 ________/    \___ row 2 ________/    \___ row 3 ________/
```
Everything about t1 sits together.

### Columnar storage (BigQuery / Parquet) — one full column, then the next
```
┌────────────┐┌─────────────────────┐┌──────────────┐┌────────────┐┌─────────┐
│ t1  t2  t3 ││ 10:00 10:00 10:01   ││ -55 -70 -60  ││ 40  95  50 ││ 2 11 3  │
└────────────┘└─────────────────────┘└──────────────┘└────────────┘└─────────┘
  terminal_id      ts                    rssi            rtt_ms       retx
```
All the rtt_ms values sit together.

---

## What gets read for two query types

(🟩 = must read off disk · ⬜ = skipped)

### Query A (OLTP): "everything about terminal t2"
- **Row → fast.** One contiguous chunk = t2's whole row. ✅
- **Columnar → slow.** Must jump into all 5 column files to rebuild the row. ❌

### Query B (OLAP): "average rtt_ms across a BILLION rows"
- **Row → slow.** Drags every full row off disk, throws away 4/5 of each. ❌
- **Columnar → fast.** Reads ONLY the rtt_ms column file, ignores the other four. ✅
  On a 5-column table = 1/5 the data. On a 50-column telemetry table = 1/50.

### Compression bonus (columnar only)
A column is all the same kind of value (all small RTT ints), so it compresses hard.
Mixed-type rows compress poorly. Less disk read = faster AND cheaper (BigQuery bills
by bytes scanned).

---

## The whole thing in one picture
```
                        WHAT YOU'RE DOING
        ┌─────────────────────┴─────────────────────┐
  "fetch/update a few           "scan millions of rows, crunch
   whole records, fast"          to a summary (avg, p95, top-K)"
      OLTP                                          OLAP
   ROW storage                                  COLUMNAR storage
   (Postgres) ◄─ you chose             (BigQuery) ◄─ resume says
  great for the app                    great for the analytics
```

> Interview line: *"I prototyped on Postgres (row-store) to own the stack locally — but
> rolling RTT and p95 across 900K terminals is an OLAP scan, so production would be a
> columnar warehouse like BigQuery, partitioned by time and clustered by terminal,
> reading only the columns each query needs. Row-store Postgres would choke on those scans."*

---

## The `advertised.listeners` gotcha (cross-network Kafka)

CLI produce/consume worked *inside* the container because the broker advertises
`localhost` — which, inside the container, IS the broker. From the Mac, `localhost`
means the Mac, so a remote client connects, gets told "reach me at localhost:9092,"
dials its own machine, and fails.

**Fix on the broker (Pi):** set `KAFKA_ADVERTISED_LISTENERS` to the Pi's LAN IP,
publish port 9092 on the host, allow 9092 through the Pi firewall.

> Interview line: *"I hit the advertised.listeners problem connecting cross-host — the
> broker advertises an address clients must be able to resolve. Default was localhost,
> so I set it to the Pi's LAN IP."*

---

## Kafka is a LOG, not a queue (the big one)

A traditional queue (RabbitMQ, SQS) is a **mailbox**: a consumer reads a message and it's
**removed**. Kafka is a **durable, append-only log** — like a camera writing to tape.

```
 PRODUCER ─► append to disk ─► [ msg0 | msg1 | msg2 | msg3 | ... ]   (partition log on the Pi)
                                   ▲                  ▲
                              consumer A         consumer B
                              (offset 1)         (offset 3)
```

- **Write and read are decoupled.** Once the broker acks (`Message delivered`), the message
  is on disk. Whether anyone consumes is irrelevant — it's already persisted. **With no
  consumer, messages just wait on disk.** Kafka is NOT dropping them.
- **Reading does NOT delete.** A consumer just keeps a bookmark — its **offset**. Many
  consumers read the same data independently. Reads are non-destructive → **replayable**:
  a new consumer can reprocess history from offset 0.
- **What deletes data = retention, NOT consumption.** By time (default **7 days**) or by
  size. Until then it stays.

> Interview line: *"Kafka is a replayable log, not a queue — consumers track offsets and
> reads are non-destructive, so I can add a consumer that reprocesses from offset 0.
> Retention is by time/size, not by consumption."*

### Docker durability catch (learned the hard way)
Messages persist to the broker's log dir **inside the container**. Ran without a `-v`
volume → data lives in the container's ephemeral filesystem and is **lost on `docker rm`**
(that's why yesterday's data vanished). Fix later: mount `-v kafka-data:/var/lib/kafka/data`.

### Prove it
`kafka-console-consumer.sh --topic wifi.telemetry.data --from-beginning` replays everything
already sent — with no DB and no prior consumer. Proof Kafka stored it.

---

## Two kinds of "partitioning" — different layers, different answers

| | Kafka partition | Postgres/warehouse table partition |
|---|---|---|
| Set by | the **message key** | a **storage layout** on a column |
| Job | parallelism + per-key ordering of the **stream** | prune scanned data for **queries** |
| My choice | **`device_id`** | **by day** (date range) |
| Wrong choice | keying by *day* → 1 hot partition, no parallelism | — |

> Interview line: *"I key the stream by `device_id` for per-device ordering and to avoid a
> hot partition, but I partition the **table** by day because the queries are time-ranged.
> Different layers, different goals."*

`listeners` vs `advertised.listeners`: **listeners = the door** the broker binds/opens
(bind broad, `0.0.0.0`); **advertised.listeners = the sign** telling clients where to come
back (advertise specific, the reachable IP). "Connection refused" = door problem; connects-
then-fails = sign problem.

---

## Build learnings (real-fleet telemetry over SSH)

- **Non-interactive SSH has a stripped PATH.** `ssh host "iw ..."` runs a non-login shell
  that excludes `/sbin` + `/usr/sbin`, so `iw`/`ip`/`ss` give `command not found` (exit 127).
  Fix: use the **full path** (`/usr/sbin/iw`). Tools in `/usr/bin` (hostname, cat) work fine.
- **Secrets in a gitignored `.env`**, loaded with `python-dotenv` (`load_dotenv()` then
  `os.getenv("PI_PASSWORD")`). The security = the file isn't committed, NOT any transform.
- **Encoding ≠ encryption.** base64 is trivially reversible — never use it to "hide" a
  password. The `.encode('utf-8')` in the producer is a **type conversion** (str→bytes,
  because Kafka only moves bytes), not security.
- **Static vs dynamic split (efficiency).** Device identity (id/mac/band/channel) doesn't
  change → SSH **once at startup** to build each `Device`. Metrics change per reading →
  generated locally, no SSH. Only re-SSH for *live* metrics, reusing the open session.
- **Real telemetry sources on a Pi:** `iw dev wlan0 info` → channel/band; `iw dev wlan0
  link` → `signal: -49 dBm` (real RSSI); `ss -ti` → real `cwnd`, `rtt`, `retrans`.
- **Correlated label, not random.** `buffer_event` is derived from the metrics (more likely
  as RTT/utilization rise, signal/cwnd fall) + noise → so the ML model has real signal but
  isn't trivially separable (lands ~70-80%, an honest result).
- Instance vs class method: `generate_reading(self)` needs a **Device object**, not
  `Device.generate_reading()`. `flush()` blocks → call `poll(0)` in the loop, `flush()` after.

---

## Consumer learnings (confluent-kafka)

- **Don't mix libraries.** `confluent-kafka` and `kafka-python` have *different* APIs.
  confluent-kafka does NOT iterate (`for m in consumer`) and uses a config dict, not kwargs.
- **Consumer setup:**
  ```python
  consumer = Consumer({
      'bootstrap.servers': '100.82.145.53:9092',   # Pi's Tailscale IP from the Mac (localhost only works ON the Pi)
      'group.id': 'telemetry-consumers',            # REQUIRED — consumers in a group split partitions
      'auto.offset.reset': 'earliest',              # first run reads from offset 0
  })
  consumer.subscribe(['wifi.telemetry.data'])       # a LIST, separate from construction
  ```
- **Poll loop, not iteration:**
  ```python
  while True:
      msg = consumer.poll(1.0)        # wait up to 1s
      if msg is None: continue
      if msg.error(): continue        # skip errored records
      ...
  finally: consumer.close()           # leaves group cleanly, commits offsets
  ```
- **Message accessors are METHODS, lowercase, with `()`:** `msg.topic()`, `msg.partition()`,
  `msg.offset()`, `msg.key()`, `msg.value()`. (`.Topic()` → AttributeError.)
- **`key()`/`value()` return bytes** → `.decode('utf-8')`, then `json.loads(...)` to get the
  dict back. Mirror image of the producer's `json.dumps(...).encode('utf-8')`.
- **`group.id` + offsets** = how Kafka tracks per-consumer-group progress. Same group splits
  the partitions for scale; a *new* group with `earliest` replays the whole topic from 0.
