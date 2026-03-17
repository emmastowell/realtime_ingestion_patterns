"""Bronze layer: raw ingestion via Auto Loader from UC Volume."""

import dlt
from pyspark.sql import functions as F

from nh_dataflows.config import TABLE_REF_SITES, VOLUME_RAW
from nh_dataflows.pipelines.expectations import BRONZE_EXPECTATIONS


@dlt.table(
    name="bronze_traffic_readings",
    comment="Raw 15-min traffic readings ingested from WebTRIS API via Auto Loader",
    table_properties={"quality": "bronze"},
)
@dlt.expect_all(BRONZE_EXPECTATIONS)
def bronze_traffic_readings():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821 — spark provided by DLT runtime
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_RAW}/_schema")
        .load(VOLUME_RAW)
        .select(
            F.col("site_id"),
            F.col("report_date"),
            F.col("time_period_end"),
            F.col("interval").cast("int"),
            F.col("total_volume").cast("int"),
            F.col("avg_speed").cast("double"),
            F.col("link_length_km").cast("double"),
            F.col("link_length_miles").cast("double"),
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_ts"),
        )
    )


@dlt.table(
    name="bronze_ref_midas_sites",
    comment="MIDAS sensor site reference data with coordinates and H3 indexes",
    table_properties={"quality": "bronze"},
)
def bronze_ref_midas_sites():
    return spark.table(TABLE_REF_SITES)  # noqa: F821
