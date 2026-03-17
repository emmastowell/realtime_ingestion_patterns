# Databricks notebook source
# MAGIC %md
# MAGIC # Approach 2: Event Hub — XML Streaming Pipeline
# MAGIC
# MAGIC Reads XML traffic payloads from Azure Event Hub (Kafka) or Delta simulation,
# MAGIC parses XML natively, enriches with reference data, and appends to gold table.

# COMMAND ----------

import os
from pyspark.sql import functions as F

# --- Config ---
CATALOG = os.environ.get("NH_CATALOG", "transport")
SCHEMA = os.environ.get("NH_SCHEMA", "realtime")
TABLE_REF_SITES = f"{CATALOG}.{SCHEMA}.ref_midas_sites"
TABLE_GOLD_CURRENT = f"{CATALOG}.{SCHEMA}.gold_current_traffic"
TABLE_EVENTHUB_INGEST = f"{CATALOG}.{SCHEMA}.eventhub_xml_ingest"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

EVENTHUB_NAMESPACE = "nh-traffic-eh.servicebus.windows.net"
EVENTHUB_TOPIC = "traffic-data"
EVENTHUB_LISTEN_CONN_STR = os.environ.get("EVENTHUB_LISTEN_CONN_STR", "")
if not EVENTHUB_LISTEN_CONN_STR:
    # Fallback to Spark conf or dbutils secrets
    try:
        EVENTHUB_LISTEN_CONN_STR = dbutils.secrets.get(scope="nh-traffic", key="eventhub-listen-conn-str")
    except:
        pass

# Auto-detect Azure vs AWS
is_azure = "azuredatabricks.net" in spark.conf.get("spark.databricks.workspaceUrl", "")
USE_REAL_EVENTHUB = is_azure
print(f"Catalog: {CATALOG}.{SCHEMA}")
print(f"Cloud: {'Azure' if is_azure else 'AWS'} — USE_REAL_EVENTHUB={USE_REAL_EVENTHUB}")

# COMMAND ----------

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    sp = batch_df.sparkSession
    # Filter to valid XML traffic messages only (skip health checks, malformed data)
    batch_df = batch_df.filter(F.col("value").contains("<TrafficData>"))
    if batch_df.isEmpty():
        return
    # Parse XML
    parsed = batch_df.select(
        F.expr("xpath_string(value, '//TrafficData/SiteId')").alias("site_id"),
        F.expr("xpath_string(value, '//TrafficData/ReportDate')").alias("report_date"),
        F.expr("xpath_string(value, '//TrafficData/TimePeriodEnd')").alias("time_period_end"),
        F.expr("xpath_int(value, '//TrafficData/TotalVolume')").alias("total_volume"),
        F.expr("xpath_double(value, '//TrafficData/AvgSpeed')").alias("avg_speed"),
        F.expr("xpath_double(value, '//TrafficData/LinkLengthKm')").alias("link_length_km"),
        F.expr("xpath_string(value, '//TrafficData/TraceId')").alias("trace_id"),
        F.col("ingestion_ts"),
    )
    # Enrich with reference data
    sites = sp.table(TABLE_REF_SITES)
    enriched = (
        parsed.join(sites, parsed.site_id == sites.site_id, "left")
        .select(
            parsed.site_id,
            F.col("site_name"),
            parsed.report_date,
            parsed.time_period_end,
            F.to_timestamp(F.concat_ws(" ", parsed.report_date, parsed.time_period_end), "dd/MM/yyyy HH:mm").alias("reading_ts"),
            parsed.total_volume,
            parsed.avg_speed,
            parsed.link_length_km,
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.expr("CAST(h3_longlatash3(longitude, latitude, 10) AS STRING)").alias("h3_index_res10"),
            F.expr("CAST(h3_longlatash3(longitude, latitude, 7) AS STRING)").alias("h3_index_res7"),
            F.regexp_extract(F.col("description"), r"(M\d+|A\d+)", 1).alias("road_name"),
            F.regexp_extract(F.col("description"), r"(Northbound|Southbound|Eastbound|Westbound|Clockwise|Anti-clockwise)", 1).alias("direction"),
            parsed.trace_id,
            F.lit("eventhub").alias("ingest_method"),
            parsed.ingestion_ts.cast("timestamp").alias("ingestion_ts"),
            F.current_timestamp().alias("processed_ts"),
        )
    )
    enriched.write.format("delta").mode("append").saveAsTable(TABLE_GOLD_CURRENT)

# COMMAND ----------

if USE_REAL_EVENTHUB:
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
        .option("kafka.group.id", "nh-traffic-consumer")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS value", "timestamp AS ingestion_ts")
    )
    checkpoint = f"{CHECKPOINT_BASE}/eventhub_kafka_pipeline"
    print(f"Reading from Event Hub Kafka: {kafka_bootstrap}/{EVENTHUB_TOPIC}")
else:
    ingest = spark.readStream.format("delta").table(TABLE_EVENTHUB_INGEST)
    checkpoint = f"{CHECKPOINT_BASE}/eventhub_pipeline"
    print(f"Reading from Delta simulation: {TABLE_EVENTHUB_INGEST}")

query = (
    ingest.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime="1 second")
    .start()
)
print(f"Stream started: {query.id}")
query.awaitTermination()
