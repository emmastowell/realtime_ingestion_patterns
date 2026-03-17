"""Traffic data endpoints — queries Unity Catalog via SQL Warehouse."""

from __future__ import annotations

from fastapi import APIRouter, Query

from models import H3Traffic, RoadTraffic, TrafficReading
from sql_warehouse import CATALOG_SCHEMA, query

router = APIRouter(prefix="/api/v1/traffic", tags=["traffic"])


@router.get("/current", response_model=list[TrafficReading])
async def get_current_traffic(
    road: str | None = Query(None, description="Filter by road name, e.g. M25"),
    direction: str | None = Query(None, description="Filter by direction, e.g. Clockwise"),
    limit: int = Query(100, ge=1, le=1000),
) -> list[TrafficReading]:
    """Get current traffic conditions across all monitored sites."""
    conditions = ["1=1"]
    if road:
        conditions.append(f"road_name = '{road}'")
    if direction:
        conditions.append(f"direction = '{direction}'")

    where = " AND ".join(conditions)
    sql = (
        f"SELECT site_id, site_name, road_name, direction, reading_ts, "
        f"avg_speed, total_volume, latitude, longitude, h3_index_res7, h3_index_res10 "
        f"FROM {CATALOG_SCHEMA}.gold_current_traffic "
        f"WHERE {where} "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY reading_ts DESC) = 1 "
        f"ORDER BY reading_ts DESC LIMIT {limit}"
    )
    rows = query(sql)
    return [TrafficReading(**_parse_traffic_row(r)) for r in rows]


@router.get("/current/{site_id}", response_model=TrafficReading | None)
async def get_site_traffic(site_id: str) -> TrafficReading | None:
    """Get current reading for a specific sensor site."""
    sql = (
        f"SELECT site_id, site_name, road_name, direction, reading_ts, "
        f"avg_speed, total_volume, latitude, longitude, h3_index_res7, h3_index_res10 "
        f"FROM {CATALOG_SCHEMA}.gold_current_traffic "
        f"WHERE site_id = '{site_id}' "
        f"ORDER BY reading_ts DESC LIMIT 1"
    )
    rows = query(sql)
    if not rows:
        return None
    return TrafficReading(**_parse_traffic_row(rows[0]))


@router.get("/road/{road_name}", response_model=list[RoadTraffic])
async def get_road_traffic(
    road_name: str,
    direction: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[RoadTraffic]:
    """Get aggregated traffic data for a specific road."""
    conditions = [f"road_name = '{road_name}'"]
    if direction:
        conditions.append(f"direction = '{direction}'")
    where = " AND ".join(conditions)

    sql = (
        f"SELECT road_name, direction, window_start, window_end, "
        f"mean_speed_mph, total_flow, sensor_count, min_speed_mph, max_speed_mph "
        f"FROM {CATALOG_SCHEMA}.v_traffic_by_road "
        f"WHERE {where} ORDER BY window_start DESC LIMIT {limit}"
    )
    rows = query(sql)
    return [RoadTraffic(**_parse_road_row(r)) for r in rows]


@router.get("/h3/{h3_index}", response_model=list[H3Traffic])
async def get_h3_traffic(
    h3_index: str,
    limit: int = Query(50, ge=1, le=500),
) -> list[H3Traffic]:
    """Get traffic data for an H3 hex."""
    sql = (
        f"SELECT h3_index_res7, window_start, window_end, "
        f"mean_speed_mph, total_flow, sensor_count "
        f"FROM {CATALOG_SCHEMA}.v_traffic_by_h3 "
        f"WHERE h3_index_res7 = '{h3_index}' "
        f"ORDER BY window_start DESC LIMIT {limit}"
    )
    rows = query(sql)
    return [H3Traffic(**_parse_h3_row(r)) for r in rows]


def _parse_traffic_row(r: dict) -> dict:
    """Convert SQL Statement API string values to typed Python values."""
    return {
        "site_id": r.get("site_id"),
        "site_name": r.get("site_name"),
        "road_name": r.get("road_name"),
        "direction": r.get("direction"),
        "reading_ts": r.get("reading_ts"),
        "avg_speed": float(r["avg_speed"]) if r.get("avg_speed") else None,
        "total_volume": int(r["total_volume"]) if r.get("total_volume") else None,
        "latitude": float(r["latitude"]) if r.get("latitude") else None,
        "longitude": float(r["longitude"]) if r.get("longitude") else None,
        "h3_index_res7": r.get("h3_index_res7"),
        "h3_index_res10": r.get("h3_index_res10"),
    }


def _parse_road_row(r: dict) -> dict:
    return {
        "road_name": r.get("road_name"),
        "direction": r.get("direction"),
        "window_start": r.get("window_start"),
        "window_end": r.get("window_end"),
        "mean_speed_mph": float(r["mean_speed_mph"]) if r.get("mean_speed_mph") else None,
        "total_flow": int(r["total_flow"]) if r.get("total_flow") else None,
        "sensor_count": int(r.get("sensor_count", 0)),
        "min_speed_mph": float(r["min_speed_mph"]) if r.get("min_speed_mph") else None,
        "max_speed_mph": float(r["max_speed_mph"]) if r.get("max_speed_mph") else None,
    }


def _parse_h3_row(r: dict) -> dict:
    return {
        "h3_index_res7": r.get("h3_index_res7"),
        "window_start": r.get("window_start"),
        "window_end": r.get("window_end"),
        "mean_speed_mph": float(r["mean_speed_mph"]) if r.get("mean_speed_mph") else None,
        "total_flow": int(r["total_flow"]) if r.get("total_flow") else None,
        "sensor_count": int(r.get("sensor_count", 0)),
    }
