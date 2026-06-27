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
