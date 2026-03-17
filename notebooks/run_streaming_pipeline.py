# Databricks notebook source
# MAGIC %md
# MAGIC # Approach 1: Zerobus — Collapsed Streaming Pipeline
# MAGIC
# MAGIC Single-hop pipeline: Zerobus ingest table → Gold table in one foreachBatch.

# COMMAND ----------

import os
from pyspark.sql import functions as F

# --- Config ---
CATALOG = os.environ.get("NH_CATALOG", "transport")
SCHEMA = os.environ.get("NH_SCHEMA", "realtime")
TABLE_REF_SITES = f"{CATALOG}.{SCHEMA}.ref_midas_sites"
TABLE_GOLD_CURRENT = f"{CATALOG}.{SCHEMA}.gold_current_traffic"
TABLE_ZEROBUS_INGEST = f"{CATALOG}.{SCHEMA}.zerobus_traffic_ingest"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

print(f"Catalog: {CATALOG}.{SCHEMA}")

# COMMAND ----------

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    sp = batch_df.sparkSession
    sites = sp.table(TABLE_REF_SITES)
    enriched = (
        batch_df.join(sites, batch_df.site_id == sites.site_id, "left")
        .select(
            batch_df.site_id,
            F.col("site_name"),
            batch_df.report_date,
            batch_df.time_period_end,
            F.to_timestamp(F.concat_ws(" ", batch_df.report_date, batch_df.time_period_end), "dd/MM/yyyy HH:mm").alias("reading_ts"),
            batch_df.total_volume.cast("int"),
            batch_df.avg_speed.cast("double"),
            batch_df.link_length_km.cast("double"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.expr("CAST(h3_longlatash3(longitude, latitude, 10) AS STRING)").alias("h3_index_res10"),
            F.expr("CAST(h3_longlatash3(longitude, latitude, 7) AS STRING)").alias("h3_index_res7"),
            F.regexp_extract(F.col("description"), r"(M\d+|A\d+)", 1).alias("road_name"),
            F.regexp_extract(F.col("description"), r"(Northbound|Southbound|Eastbound|Westbound|Clockwise|Anti-clockwise)", 1).alias("direction"),
            batch_df.trace_id,
            F.lit("zerobus").alias("ingest_method"),
            (batch_df.ingestion_ts / 1_000_000).cast("timestamp").alias("ingestion_ts"),
            F.current_timestamp().alias("processed_ts"),
        )
    )
    enriched.write.format("delta").mode("append").saveAsTable(TABLE_GOLD_CURRENT)

# COMMAND ----------

ingest = spark.readStream.format("delta").table(TABLE_ZEROBUS_INGEST)

query = (
    ingest.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/collapsed_pipeline")
    .trigger(processingTime="1 second")
    .start()
)
print(f"Stream started: {query.id}")
query.awaitTermination()
