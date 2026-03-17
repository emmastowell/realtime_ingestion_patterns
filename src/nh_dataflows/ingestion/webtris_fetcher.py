"""Scheduled job: fetch latest traffic data from WebTRIS API and write to UC Volume as JSON."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from nh_dataflows.config import FOCUS_ROADS, TABLE_REF_SITES, VOLUME_RAW
from nh_dataflows.ingestion.webtris_client import WebTRISClient

logger = logging.getLogger(__name__)

# Maximum sites per API call (WebTRIS limit)
MAX_SITES_PER_REQUEST = 100


def get_focus_site_ids(spark: SparkSession) -> list[str]:
    """Look up site IDs for the focus motorways from the reference table."""
    roads_filter = ", ".join(f"'{r}'" for r in FOCUS_ROADS)
    df = spark.sql(
        f"SELECT site_id FROM {TABLE_REF_SITES} "
        f"WHERE road_name IN ({roads_filter}) AND status = 'Active' "
        f"LIMIT {MAX_SITES_PER_REQUEST}"
    )
    return [row.site_id for row in df.collect()]


async def fetch_and_write(site_ids: list[str], output_path: str) -> int:
    """Fetch today's traffic data for given sites and write JSON to UC Volume.

    Returns:
        Number of rows fetched.
    """
    today = date.today()

    async with WebTRISClient() as client:
        report = await client.get_daily_report(
            site_ids=site_ids,
            start_date=today,
            end_date=today,
        )

    rows = [r.model_dump() for r in report.rows]

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"traffic_{timestamp}.json"
    filepath = f"{output_path}/{filename}"

    # Write via dbutils for UC Volume access
    json_content = json.dumps(rows, indent=None, default=str)

    # Write to local temp then copy — or write directly
    import tempfile
    from pathlib import Path

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(json_content)
        tmp_path = f.name

    # Use dbutils to copy to UC Volume
    spark = SparkSession.getActiveSession()
    if spark:
        dbutils = DBUtils(spark)
        dbutils.fs.cp(f"file:{tmp_path}", filepath.replace("/Volumes", "dbfs:/Volumes"))

    Path(tmp_path).unlink(missing_ok=True)

    logger.info("Wrote %d rows to %s", len(rows), filepath)
    return len(rows)


def run(spark: SparkSession) -> None:
    """Entry point for the scheduled traffic fetch job."""
    site_ids = get_focus_site_ids(spark)
    if not site_ids:
        logger.warning("No focus sites found — has the reference data been loaded?")
        return

    logger.info("Fetching traffic data for %d sites on roads: %s", len(site_ids), FOCUS_ROADS)
    import nest_asyncio
    nest_asyncio.apply()
    row_count = asyncio.run(fetch_and_write(site_ids, VOLUME_RAW))
    logger.info("Fetch complete: %d readings ingested", row_count)
