# Pipeline Latency Breakdown — What Happens at Each Step

**Last updated:** 2026-03-16

This document explains exactly what happens at each stage of the demo pipeline, where the time goes, and what can be tuned.

---

## End-to-End Flow (Zerobus Path)

```
User clicks Inject
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 1: INJECT                                    ~1-2s   │
│                                                             │
│ The FastAPI endpoint opens a gRPC stream to the Zerobus     │
│ server, sends a single JSON record, waits for an ACK,      │
│ and closes the stream. The ACK confirms the server has      │
│ received the record — not that it's in Delta yet.           │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 2: INGEST TABLE                              ~4-5s   │
│                                                             │
│ Zerobus buffers records server-side before flushing to      │
│ Delta. The buffer is internal and not tuneable — Databricks │
│ documents it as "as low as 5 seconds" from producer write   │
│ to data being readable in the Delta table.                  │
│                                                             │
│ This includes:                                              │
│   - Server-side buffer wait        ~3-4s                    │
│   - Delta commit (Parquet + txn)   ~500ms-1s                │
│                                                             │
│ This is the single largest component of end-to-end latency  │
│ and cannot be reduced.                                      │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 3: SPARK STRUCTURED STREAMING               ~1-3s    │
│                                                             │
│ The streaming job runs continuously with a 1-second trigger │
│ interval. Every second it checks the source Delta table's   │
│ transaction log for new commits.                            │
│                                                             │
│ When it finds new rows, foreachBatch executes:              │
│                                                             │
│   a) Read new rows from source Delta table      ~200ms     │
│   b) Stream-static join with ref_midas_sites    ~200ms     │
│      (19,518 reference rows with lat/lon)                   │
│   c) Compute H3 indexes at resolution 10 + 7   ~100ms     │
│   d) Extract road_name + direction via regex    ~50ms      │
│   e) Add ingest_method + processed_ts columns   ~10ms      │
│   f) APPEND to gold_current_traffic             ~300ms     │
│   g) Compute 15-min window aggregates by road   ~200ms     │
│   h) APPEND to gold_traffic_by_road             ~200ms     │
│   i) Compute 15-min window aggregates by H3     ~200ms     │
│   j) APPEND to gold_traffic_by_h3              ~200ms      │
│   k) INSERT to Lakebase PostgreSQL             ~300ms      │
│                                                             │
│ Total foreachBatch processing: ~1-2s                        │
│ Plus trigger detection wait: ~0-1s                          │
│                                                             │
│ NOTE: Previously this stage used a MERGE (scan + update)    │
│ for gold_current_traffic, which added 5-15s per batch.      │
│ Switching to append-only eliminated this.                   │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 4: GOLD TABLE                               instant  │
│                                                             │
│ The record is now in the Delta table as part of the commit  │
│ from Stage 3. There is no separate step — the APPEND in     │
│ foreachBatch is the Delta commit.                           │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 5: API QUERY                                ~1-2s    │
│                                                             │
│ The poll endpoint queries the SQL Warehouse:                │
│   SELECT ... FROM gold_current_traffic                      │
│   WHERE trace_id = '{trace_id}'                             │
│                                                             │
│ The warehouse detects the new Delta table version and       │
│ returns the matching row. First query after a new commit    │
│ may take slightly longer as the warehouse refreshes its     │
│ metadata cache.                                             │
│                                                             │
│ The demo UI polls every 1.5s with a 2s server-side timeout, │
│ so there can be up to 1.5s of polling overhead on top of    │
│ the actual query time.                                      │
└─────────────────────────────────────────────────────────────┘


TOTAL (warm cluster, Zerobus): ~6-8 seconds
```

---

## End-to-End Flow (Event Hub XML — Delta Simulation Path)

The only difference is Stage 1 and 2:

```
┌─────────────────────────────────────────────────────────────┐
│ STAGE 1: INJECT                                   ~10-15s  │
│                                                             │
│ The FastAPI endpoint builds an XML payload and executes:    │
│   INSERT INTO eventhub_xml_ingest (value, ingestion_ts,     │
│   trace_id) VALUES (...)                                    │
│                                                             │
│ This goes via the SQL Statement API, which sends the SQL    │
│ to the SQL Warehouse, waits for execution, and returns.     │
│ The INSERT itself is fast, but the overhead of the          │
│ Statement API (HTTP round-trip, warehouse scheduling,       │
│ Delta commit) adds ~10-15s.                                 │
│                                                             │
│ This is the dominant latency in the Delta sim path and is   │
│ an artefact of the simulation — real Event Hub would be     │
│ ~0.5s here.                                                 │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 2: INGEST TABLE                             instant   │
│                                                             │
│ The record is already in the Delta table — the INSERT in    │
│ Stage 1 committed it directly. No buffer wait (unlike       │
│ Zerobus).                                                   │
└─────────────────────────────────────────────────────────────┘

Stages 3-5 are identical to the Zerobus path.

TOTAL (warm cluster, Delta sim): ~15-20 seconds
TOTAL (warm cluster, real Event Hub on Azure): ~3-5 seconds (projected)
```

---

## End-to-End Flow (Real Event Hub — Azure Only)

```
┌─────────────────────────────────────────────────────────────┐
│ STAGE 1: INJECT                                   ~0.5-1s  │
│                                                             │
│ The FastAPI endpoint builds an XML payload and publishes    │
│ it to Azure Event Hub using the azure-eventhub SDK (AMQP    │
│ protocol). Event Hub acknowledges receipt almost instantly.  │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ STAGE 2: EVENT HUB → SPARK KAFKA CONSUMER         ~0.5-1s  │
│                                                             │
│ Spark reads from Event Hub's Kafka-compatible endpoint      │
│ (port 9093). The Kafka consumer detects the new message     │
│ within the next trigger interval (1 second).                │
│                                                             │
│ NOTE: This only works when Spark runs on Azure in the same  │
│ network as Event Hub. AWS clusters cannot reach port 9093.  │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
    Stages 3-5 identical (~2-4s)

TOTAL (warm cluster, real Event Hub): ~3-5 seconds
```

---

## Cluster Startup — The Hidden Latency

If the streaming job has just been started (or restarted after a failure), the cluster needs to provision before any data processing can begin.

| Cluster State | Additional Latency | When This Happens |
|--------------|-------------------|-------------------|
| **Warm (running)** | 0s | Normal operation — stream is already running |
| **Pool available** | ~30s | Cluster grabs a pre-warmed instance from the pool |
| **Cold start** | ~3-5 minutes | Fresh cluster provisioning (EC2 instance launch, Spark startup, library install) |

In normal operation, the streaming jobs run continuously and the clusters stay warm. Cold starts only happen when:
- The job is first deployed
- The job is manually cancelled and restarted
- The job fails and auto-restarts
- The cluster is terminated due to inactivity (shouldn't happen for continuous streaming jobs)

### Mitigation options

**Cluster pools (recommended):** Pre-allocate idle instances that are ready to go. When the job starts, it grabs a warm instance (~30s) instead of provisioning from scratch (~3-5 min). Idle instances cost less than running instances. This is the standard approach for production streaming workloads.

**Serverless + scheduled trigger:** Instead of a continuous stream, run the job on a frequent schedule (e.g. every 30s or every minute) using serverless compute. Serverless is pre-warmed — no provisioning delay. Each run processes all available records and exits. Trades ~1s trigger latency for ~10-15s schedule latency, but eliminates cold start entirely and only charges for actual compute time.

**All-purpose cluster:** Keep an interactive cluster running permanently and point the job at it. More expensive (no auto-termination), but guarantees the cluster is always warm. Not recommended for production.

---

## Summary Table

| Component | Zerobus Path | Delta Sim Path | Real Event Hub Path |
|-----------|-------------|---------------|-------------------|
| Inject (API call) | ~1-2s | ~10-15s | ~0.5-1s |
| Ingest buffer/commit | ~4-5s (Zerobus buffer) | instant (already committed) | ~0.5-1s (Kafka delivery) |
| Stream trigger detection | ~0-1s | ~0-1s | ~0-1s |
| foreachBatch processing | ~1-2s | ~1-2s | ~1-2s |
| API poll + query | ~1-2s | ~1-2s | ~1-2s |
| **Total (warm cluster)** | **~6-8s** | **~15-20s** | **~3-5s** |
| Cluster cold start (if applicable) | +3-5 min | +3-5 min | +3-5 min |
| Cluster pool start (if configured) | +30s | +30s | +30s |

---

## What Can Be Tuned

| Knob | Current Value | Effect of Changing |
|------|--------------|-------------------|
| Streaming trigger interval | 1 second | Lower = faster detection, but more frequent empty batches. 1s is already the practical minimum for Delta sources. |
| Zerobus buffer flush | ~5s (not tuneable) | Cannot change — internal to Zerobus server. |
| Cluster pool | Not configured | Adding a pool reduces cold start from ~4 min to ~30s. |
| Number of gold tables written | 3 (current + road + h3) | Reducing to 1 would save ~400ms per batch, but loses the aggregated views. |
| Lakebase write | Enabled | Disabling saves ~300ms per batch. Only needed if using Lakebase for API serving (we currently use SQL Warehouse). |
| MERGE vs APPEND | APPEND (current) | MERGE added 5-15s per batch. Already switched to append-only. |
| Poll interval (demo UI) | 1.5s client-side | Lower = faster detection in the UI, but more load on SQL Warehouse. |

---

## What Cannot Be Tuned

| Constraint | Why |
|-----------|-----|
| Zerobus ~5s buffer | Internal server-side batching, not exposed as a config. This is the price of eliminating the message bus. |
| Delta commit latency (~500ms) | Inherent to Delta's transaction log model. Every write is a Parquet file + JSON transaction log entry. |
| SQL Statement API INSERT overhead (~10-15s) | The Statement API is designed for analytical queries, not low-latency writes. This is only an issue for the Delta simulation path. |
| Stream-static join cost | The join with 19K reference rows is fast (~200ms) but cannot be eliminated — it's where the enrichment happens. |
