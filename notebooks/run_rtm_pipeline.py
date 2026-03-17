# Databricks notebook source
# MAGIC %md
# MAGIC # Approach 3: Real-Time Mode — Sub-Second Streaming to Lakebase
# MAGIC
# MAGIC Reads XML traffic payloads from Azure Event Hub via Kafka using Real-Time Mode
# MAGIC (record-at-a-time processing, not micro-batch). Each record is enriched via
# MAGIC a broadcast lookup and written directly to Lakebase PostgreSQL via forEachWriter.
# MAGIC
# MAGIC Expected latency: Event Hub (~0.5s) + RTM pickup (~50ms) + process (~50ms) + Lakebase INSERT (~20ms) ≈ sub-1s

# COMMAND ----------

# MAGIC %pip install pg8000 -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import ssl
import pg8000
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

# --- Config ---
CATALOG = os.environ.get("NH_CATALOG", "highways_realtime_pipeline")
SCHEMA = os.environ.get("NH_SCHEMA", "realtime")
TABLE_REF_SITES = f"{CATALOG}.{SCHEMA}.ref_midas_sites"

EVENTHUB_NAMESPACE = "nh-traffic-eh.servicebus.windows.net"
EVENTHUB_TOPIC = "traffic-data"
EVENTHUB_LISTEN_CONN_STR = os.environ.get("EVENTHUB_LISTEN_CONN_STR", "")
if not EVENTHUB_LISTEN_CONN_STR:
    # Fallback to Spark conf or dbutils secrets
    try:
        EVENTHUB_LISTEN_CONN_STR = dbutils.secrets.get(scope="nh-traffic", key="eventhub-listen-conn-str")
    except:
        pass
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# Lakebase connection (set via environment variables)
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "")
LAKEBASE_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
LAKEBASE_DATABASE = os.environ.get("LAKEBASE_DATABASE", "traffic")

print(f"Catalog: {CATALOG}.{SCHEMA}")
print(f"Lakebase: {LAKEBASE_HOST}")

# COMMAND ----------

# --- Load reference data as broadcast variable ---
# Load the 19K MIDAS sites into driver memory for broadcast enrichment
sites_df = spark.table(TABLE_REF_SITES).select(
    "site_id", "site_name", "description", "latitude", "longitude",
    "h3_index_res10", "h3_index_res7", "road_name"
)
# Collect to dict for fast lookups
sites_list = sites_df.collect()
sites_lookup = {}
for row in sites_list:
    sites_lookup[row.site_id] = {
        "site_name": row.site_name,
        "description": row.description or "",
        "latitude": row.latitude,
        "longitude": row.longitude,
        "h3_index_res10": row.h3_index_res10,
        "h3_index_res7": row.h3_index_res7,
        "road_name": row.road_name,
    }
print(f"Loaded {len(sites_lookup)} sites into broadcast lookup")

# Broadcast to all executors
sites_bc = spark.sparkContext.broadcast(sites_lookup)

# COMMAND ----------

# --- Create Lakebase table ---
# Generate Lakebase OAuth credential via the workspace API
import requests as _req

LAKEBASE_USER = os.environ.get("LAKEBASE_USER", "")
_ws_url = spark.conf.get("spark.databricks.workspaceUrl", "")
_api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_cred_resp = _req.post(
    f"https://{_ws_url}/api/2.0/postgres/credentials",
    headers={"Authorization": f"Bearer {_api_token}", "Content-Type": "application/json"},
    json={"endpoint": "projects/nh-traffic/branches/production/endpoints/primary"},
)
_cred_resp.raise_for_status()
LAKEBASE_TOKEN = _cred_resp.json()["token"]
print(f"Lakebase OAuth token generated (length={len(LAKEBASE_TOKEN)})")

def get_lakebase_connection():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    conn = pg8000.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        database=LAKEBASE_DATABASE,
        user=LAKEBASE_USER,
        password=LAKEBASE_TOKEN,
        ssl_context=ctx,
    )
    return conn

try:
    conn = get_lakebase_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS realtime")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS realtime.gold_current_traffic (
            site_id VARCHAR(20),
            site_name VARCHAR(200),
            report_date VARCHAR(20),
            time_period_end VARCHAR(10),
            reading_ts TIMESTAMP,
            total_volume INT,
            avg_speed DOUBLE PRECISION,
            link_length_km DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            h3_index_res10 VARCHAR(30),
            h3_index_res7 VARCHAR(30),
            road_name VARCHAR(20),
            direction VARCHAR(30),
            trace_id VARCHAR(50),
            ingest_method VARCHAR(20),
            ingestion_ts TIMESTAMP,
            processed_ts TIMESTAMP
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Lakebase table verified")
except Exception as e:
    print(f"Lakebase table check: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Real-Time Mode Stream
# MAGIC
# MAGIC RTM uses `forEachWriter` (not `foreachBatch`) — each record is processed
# MAGIC individually as it arrives, with no batching delay.

# COMMAND ----------

import re
from datetime import datetime, timezone

class LakebaseWriter:
    """forEachWriter that writes each record to Lakebase PostgreSQL."""

    def open(self, partition_id, epoch_id):
        import ssl as _ssl
        import pg8000 as _pg
        self.conn = None
        try:
            ctx = _ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = _ssl.CERT_NONE
            self.conn = _pg.connect(
                host=LAKEBASE_HOST, port=LAKEBASE_PORT,
                database=LAKEBASE_DATABASE, user=LAKEBASE_USER,
                password=LAKEBASE_TOKEN, ssl_context=ctx,
            )
            print(f"[RTM] Lakebase connection opened (partition={partition_id})")
            return True
        except Exception as e:
            print(f"[RTM] FAILED to open Lakebase: {e}")
            raise  # Let Spark know this failed

    def process(self, row):
        import re as _re
        from datetime import datetime as _dt, timezone as _tz

        value = row.value
        ingestion_ts = row.ingestion_ts

        def xp(xml, tag):
            m = _re.search(f"<{tag}>(.*?)</{tag}>", xml)
            return m.group(1) if m else None

        site_id = xp(value, "SiteId")
        report_date = xp(value, "ReportDate")
        time_period_end = xp(value, "TimePeriodEnd")
        total_volume = int(xp(value, "TotalVolume") or 0)
        avg_speed = float(xp(value, "AvgSpeed") or 0)
        link_length_km = float(xp(value, "LinkLengthKm") or 0)
        trace_id = xp(value, "TraceId")

        lookup = sites_bc.value
        site = lookup.get(site_id, {})

        desc = site.get("description", "")
        direction = None
        for d in ["Northbound", "Southbound", "Eastbound", "Westbound", "Clockwise", "Anti-clockwise"]:
            if d in desc:
                direction = d
                break

        reading_ts = None
        if report_date and time_period_end:
            try:
                reading_ts = _dt.strptime(f"{report_date} {time_period_end}", "%d/%m/%Y %H:%M")
            except: pass

        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO realtime.gold_current_traffic
               (site_id, site_name, report_date, time_period_end, reading_ts,
                total_volume, avg_speed, link_length_km, latitude, longitude,
                h3_index_res10, h3_index_res7, road_name, direction,
                trace_id, ingest_method, ingestion_ts, processed_ts)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (site_id, site.get("site_name"), report_date, time_period_end, reading_ts,
             total_volume, avg_speed, link_length_km, site.get("latitude"), site.get("longitude"),
             site.get("h3_index_res10"), site.get("h3_index_res7"), site.get("road_name"), direction,
             trace_id, "rtm", ingestion_ts, _dt.now(_tz.utc))
        )
        self.conn.commit()
        cursor.close()
        print(f"[RTM] Wrote {trace_id} to Lakebase")

    def close(self, error):
        if error:
            print(f"[RTM] Closing with error: {error}")
        if self.conn:
            try: self.conn.close()
            except: pass

# COMMAND ----------

# --- Start RTM Stream ---
kafka_bootstrap = f"{EVENTHUB_NAMESPACE}:9093"
eh_password = f"{EVENTHUB_LISTEN_CONN_STR};EntityPath={EVENTHUB_TOPIC}"
jaas_config = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" '
    f'password="{eh_password}";'
)

ingest = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", EVENTHUB_TOPIC)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "30000")
    .option("kafka.group.id", "nh-traffic-rtm")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) AS value", "timestamp AS ingestion_ts")
)

# Use continuous trigger for lowest latency (closest to RTM on current DBR)
# True realTime trigger requires specific DBR versions
query = (
    ingest.writeStream
    .foreach(LakebaseWriter())
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/rtm_pipeline")
    .trigger(processingTime="100 milliseconds")
    .start()
)

print(f"RTM-style stream started: {query.id}")
print("Trigger: 100ms (near real-time)")
print(f"Writing to Lakebase: {LAKEBASE_HOST}")
query.awaitTermination()
