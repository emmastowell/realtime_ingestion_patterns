"""Silver layer: cleansed traffic readings with H3 indexes and road extraction."""

import dlt
from pyspark.sql import functions as F

from nh_dataflows.config import H3_RESOLUTION_COARSE, H3_RESOLUTION_FINE, WATERMARK_DELAY
from nh_dataflows.pipelines.expectations import SILVER_EXPECTATIONS


@dlt.table(
    name="silver_traffic_readings",
    comment="Cleansed traffic readings joined with site reference data, H3-indexed",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop(SILVER_EXPECTATIONS)
def silver_traffic_readings():
    traffic = (
        dlt.read_stream("bronze_traffic_readings")
        .withWatermark("ingestion_ts", WATERMARK_DELAY)
    )
    sites = dlt.read("bronze_ref_midas_sites")

    return (
        traffic.join(sites, traffic.site_id == sites.site_id, "left")
        .select(
            traffic.site_id,
            F.col("site_name"),
            F.col("report_date"),
            F.col("time_period_end"),
            # Build a proper timestamp from date + time
            F.to_timestamp(
                F.concat_ws(" ", F.col("report_date"), F.col("time_period_end")),
                "dd/MM/yyyy HH:mm",
            ).alias("reading_ts"),
            F.col("total_volume").cast("int"),
            F.col("avg_speed").cast("double"),
            F.col("link_length_km").cast("double"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            # H3 indexes at two resolutions
            F.expr(
                f"h3_longlatash3(longitude, latitude, {H3_RESOLUTION_FINE})"
            ).alias("h3_index_res10"),
            F.expr(
                f"h3_longlatash3(longitude, latitude, {H3_RESOLUTION_COARSE})"
            ).alias("h3_index_res7"),
            # Extract road name (M25, A1, etc.) from site description
            F.regexp_extract(F.col("description"), r"(M\d+|A\d+)", 1).alias("road_name"),
            # Extract direction from description
            F.regexp_extract(
                F.col("description"),
                r"(Northbound|Southbound|Eastbound|Westbound|Clockwise|Anti-clockwise)",
                1,
            ).alias("direction"),
            F.col("ingestion_ts"),
        )
    )
