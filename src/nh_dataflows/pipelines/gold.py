"""Gold layer: aggregated traffic views for API consumption."""

import dlt
from pyspark.sql import functions as F

from nh_dataflows.pipelines.expectations import GOLD_EXPECTATIONS


@dlt.table(
    name="gold_current_traffic",
    comment="Latest reading per site — powers the current conditions API",
    table_properties={"quality": "gold"},
)
@dlt.expect_all(GOLD_EXPECTATIONS)
def gold_current_traffic():
    """Deduplicate to latest reading per site using row_number window."""
    return (
        dlt.read("silver_traffic_readings")
        .withColumn(
            "rn",
            F.row_number().over(
                F.window("site_id").orderBy(F.col("reading_ts").desc())
            ),
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


@dlt.table(
    name="gold_traffic_by_road",
    comment="15-min windowed aggregations by road and direction",
    table_properties={"quality": "gold"},
)
def gold_traffic_by_road():
    return (
        dlt.read("silver_traffic_readings")
        .groupBy(
            F.col("road_name"),
            F.col("direction"),
            F.window("reading_ts", "15 minutes").alias("time_window"),
        )
        .agg(
            F.avg("avg_speed").alias("mean_speed_mph"),
            F.sum("total_volume").alias("total_flow"),
            F.count("site_id").alias("sensor_count"),
            F.min("avg_speed").alias("min_speed_mph"),
            F.max("avg_speed").alias("max_speed_mph"),
        )
        .select(
            "road_name",
            "direction",
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "mean_speed_mph",
            "total_flow",
            "sensor_count",
            "min_speed_mph",
            "max_speed_mph",
        )
    )


@dlt.table(
    name="gold_traffic_by_h3",
    comment="15-min windowed aggregations by H3 res-7 hex for map visualisation",
    table_properties={"quality": "gold"},
)
def gold_traffic_by_h3():
    return (
        dlt.read("silver_traffic_readings")
        .groupBy(
            F.col("h3_index_res7"),
            F.window("reading_ts", "15 minutes").alias("time_window"),
        )
        .agg(
            F.avg("avg_speed").alias("mean_speed_mph"),
            F.sum("total_volume").alias("total_flow"),
            F.count("site_id").alias("sensor_count"),
        )
        .select(
            "h3_index_res7",
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "mean_speed_mph",
            "total_flow",
            "sensor_count",
        )
    )
