# Databricks notebook source
# MAGIC %md
# MAGIC # Approach 4: Event Hub — Full Medallion (Bronze → Silver → Gold)
# MAGIC
# MAGIC Production-style pipeline with intermediate Delta layers for governance,
# MAGIC data quality, and replayability. Each layer adds ~2-3s of Delta commit overhead.

# COMMAND ----------

import os
from pyspark.sql import functions as F

# --- Config ---
CATALOG = os.environ.get("NH_CATALOG", "highways_realtime_pipeline")
SCHEMA = os.environ.get("NH_SCHEMA", "realtime")
TABLE_REF_SITES = f"{CATALOG}.{SCHEMA}.ref_midas_sites"
TABLE_BRONZE = f"{CATALOG}.{SCHEMA}.bronze_traffic"
TABLE_SILVER = f"{CATALOG}.{SCHEMA}.silver_traffic"
TABLE_GOLD_CURRENT = f"{CATALOG}.{SCHEMA}.gold_current_traffic"
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

is_azure = "azuredatabricks.net" in spark.conf.get("spark.databricks.workspaceUrl", "")
print(f"Catalog: {CATALOG}.{SCHEMA}")
print(f"Cloud: {'Azure' if is_azure else 'AWS'}")

# COMMAND ----------

# Ensure bronze and silver tables exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_BRONZE} (
        value STRING, ingestion_ts TIMESTAMP, trace_id STRING,
        kafka_timestamp TIMESTAMP, bronze_ts TIMESTAMP
    )
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_SILVER} (
        site_id STRING, site_name STRING, report_date STRING, time_period_end STRING,
        reading_ts TIMESTAMP, total_volume INT, avg_speed DOUBLE, link_length_km DOUBLE,
        latitude DOUBLE, longitude DOUBLE, h3_index_res10 STRING, h3_index_res7 STRING,
        road_name STRING, direction STRING, trace_id STRING, ingest_method STRING,
        ingestion_ts TIMESTAMP, bronze_ts TIMESTAMP, silver_ts TIMESTAMP
    )
""")
print("Bronze and silver tables verified")

# COMMAND ----------

def process_medallion_batch(batch_df, batch_id):
    """Three-stage medallion: raw → parsed+enriched → gold."""
    if batch_df.isEmpty():
        return
    sp = batch_df.sparkSession

    # Filter to valid XML only
    xml_df = batch_df.filter(F.col("value").contains("<TrafficData>"))
    if xml_df.isEmpty():
        return

    # ── BRONZE: raw XML + metadata ──
    bronze = xml_df.select(
        F.col("value"),
        F.col("ingestion_ts"),
        F.expr("xpath_string(value, '//TrafficData/TraceId')").alias("trace_id"),
        F.col("ingestion_ts").alias("kafka_timestamp"),
        F.current_timestamp().alias("bronze_ts"),
    )
    bronze.write.format("delta").mode("append").saveAsTable(TABLE_BRONZE)

    # ── SILVER: parse XML + enrich with reference data ──
    parsed = xml_df.select(
        F.expr("xpath_string(value, '//TrafficData/SiteId')").alias("site_id"),
        F.expr("xpath_string(value, '//TrafficData/ReportDate')").alias("report_date"),
        F.expr("xpath_string(value, '//TrafficData/TimePeriodEnd')").alias("time_period_end"),
        F.expr("xpath_int(value, '//TrafficData/TotalVolume')").alias("total_volume"),
        F.expr("xpath_double(value, '//TrafficData/AvgSpeed')").alias("avg_speed"),
        F.expr("xpath_double(value, '//TrafficData/LinkLengthKm')").alias("link_length_km"),
        F.expr("xpath_string(value, '//TrafficData/TraceId')").alias("trace_id"),
        F.col("ingestion_ts"),
    )

    sites = sp.table(TABLE_REF_SITES)
    silver = (
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
            F.lit("medallion").alias("ingest_method"),
            parsed.ingestion_ts.cast("timestamp").alias("ingestion_ts"),
            F.current_timestamp().alias("bronze_ts"),
            F.current_timestamp().alias("silver_ts"),
        )
    )
    silver.write.format("delta").mode("append").saveAsTable(TABLE_SILVER)

    # ── GOLD: same enriched data to the gold serving table ──
    gold = silver.drop("bronze_ts", "silver_ts").withColumn("processed_ts", F.current_timestamp())
    gold.write.format("delta").mode("append").saveAsTable(TABLE_GOLD_CURRENT)

# COMMAND ----------

# --- Start stream from Event Hub Kafka ---
if is_azure:
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
        .option("kafka.group.id", "nh-traffic-medallion")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS value", "timestamp AS ingestion_ts")
    )
    print(f"Reading from Event Hub Kafka: {kafka_bootstrap}/{EVENTHUB_TOPIC}")
else:
    TABLE_EVENTHUB_INGEST = f"{CATALOG}.{SCHEMA}.eventhub_xml_ingest"
    ingest = spark.readStream.format("delta").table(TABLE_EVENTHUB_INGEST)
    print(f"Reading from Delta simulation: {TABLE_EVENTHUB_INGEST}")

query = (
    ingest.writeStream
    .foreachBatch(process_medallion_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/medallion_pipeline")
    .trigger(processingTime="1 second")
    .start()
)
print(f"Medallion pipeline started: {query.id}")
query.awaitTermination()
