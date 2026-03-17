"""Health check endpoint."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter

from models import HealthResponse
from sql_warehouse import CATALOG_SCHEMA, query_one

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Check SQL Warehouse connectivity and pipeline freshness."""
    try:
        row = query_one(
            f"SELECT CAST(MAX(reading_ts) AS STRING) AS latest_ts "
            f"FROM {CATALOG_SCHEMA}.gold_current_traffic"
        )
        latest_ts_str = row.get("latest_ts") if row else None
        lag = None
        if latest_ts_str:
            latest_ts = datetime.fromisoformat(latest_ts_str)
            if latest_ts.tzinfo is None:
                latest_ts = latest_ts.replace(tzinfo=UTC)
            lag = (datetime.now(UTC) - latest_ts).total_seconds()
        return HealthResponse(
            status="healthy",
            sql_warehouse_connected=True,
            latest_reading_ts=latest_ts_str,
            pipeline_lag_seconds=lag,
        )
    except Exception:
        import logging
        logging.getLogger(__name__).exception("Health check failed")
        return HealthResponse(status="degraded", sql_warehouse_connected=False)
