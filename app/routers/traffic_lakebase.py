"""Traffic endpoints backed by Lakebase (RTM path) — sub-50ms queries."""

from __future__ import annotations

from fastapi import APIRouter, Query

from models import TrafficReading

router = APIRouter(prefix="/api/v1/lakebase", tags=["lakebase"])


@router.get("/traffic/current", response_model=list[TrafficReading])
async def get_current_traffic(
    road: str | None = Query(None, description="Filter by road name"),
    limit: int = Query(100, ge=1, le=1000),
) -> list[TrafficReading]:
    """Get current traffic from Lakebase (RTM pipeline, sub-second latency)."""
    from lakebase import query

    conditions = ["1=1"]
    if road:
        conditions.append(f"road_name = '{road}'")
    where = " AND ".join(conditions)

    sql = (
        f"SELECT DISTINCT ON (site_id) "
        f"site_id, site_name, road_name, direction, reading_ts, "
        f"avg_speed, total_volume, latitude, longitude, h3_index_res7, h3_index_res10 "
        f"FROM realtime.gold_current_traffic "
        f"WHERE {where} "
        f"ORDER BY site_id, reading_ts DESC "
        f"LIMIT {limit}"
    )
    rows = await query(sql)
    return [TrafficReading(**_parse(r)) for r in rows]


@router.get("/traffic/current/{site_id}", response_model=TrafficReading | None)
async def get_site_traffic(site_id: str) -> TrafficReading | None:
    """Get latest reading for a site from Lakebase."""
    from lakebase import query_one

    sql = (
        f"SELECT site_id, site_name, road_name, direction, reading_ts, "
        f"avg_speed, total_volume, latitude, longitude, h3_index_res7, h3_index_res10 "
        f"FROM realtime.gold_current_traffic "
        f"WHERE site_id = '{site_id}' "
        f"ORDER BY reading_ts DESC LIMIT 1"
    )
    row = await query_one(sql)
    if not row:
        return None
    return TrafficReading(**_parse(row))


@router.get("/traffic/count")
async def get_record_count():
    """Get total record count in Lakebase RTM table."""
    from lakebase import query_one

    row = await query_one("SELECT COUNT(*) as cnt FROM realtime.gold_current_traffic")
    return {"count": row["cnt"] if row else 0, "source": "lakebase"}


def _parse(r: dict) -> dict:
    return {
        "site_id": r.get("site_id"),
        "site_name": r.get("site_name"),
        "road_name": r.get("road_name"),
        "direction": r.get("direction"),
        "reading_ts": r.get("reading_ts"),
        "avg_speed": float(r["avg_speed"]) if r.get("avg_speed") is not None else None,
        "total_volume": int(r["total_volume"]) if r.get("total_volume") is not None else None,
        "latitude": float(r["latitude"]) if r.get("latitude") is not None else None,
        "longitude": float(r["longitude"]) if r.get("longitude") is not None else None,
        "h3_index_res7": r.get("h3_index_res7"),
        "h3_index_res10": r.get("h3_index_res10"),
    }
