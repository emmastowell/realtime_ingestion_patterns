"""Asset/site reference data endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Query

from models import SiteInfo
from sql_warehouse import CATALOG_SCHEMA, query

router = APIRouter(prefix="/api/v1/assets", tags=["assets"])


@router.get("/sites", response_model=list[SiteInfo])
async def list_sites(
    road: str | None = Query(None, description="Filter by road name, e.g. M25"),
    limit: int = Query(100, ge=1, le=1000),
) -> list[SiteInfo]:
    """List MIDAS sensor sites with optional filters."""
    conditions = ["1=1"]
    if road:
        conditions.append(f"description LIKE '%{road}%'")

    where = " AND ".join(conditions)
    sql = (
        f"SELECT site_id, site_name, description, latitude, longitude "
        f"FROM {CATALOG_SCHEMA}.ref_midas_sites "
        f"WHERE {where} ORDER BY site_id LIMIT {limit}"
    )
    rows = query(sql)
    return [SiteInfo(**_parse_site_row(r)) for r in rows]


@router.get("/sites/{site_id}", response_model=SiteInfo | None)
async def get_site(site_id: str) -> SiteInfo | None:
    """Get details for a specific MIDAS site."""
    sql = (
        f"SELECT site_id, site_name, description, latitude, longitude "
        f"FROM {CATALOG_SCHEMA}.ref_midas_sites "
        f"WHERE site_id = '{site_id}'"
    )
    rows = query(sql)
    if not rows:
        return None
    return SiteInfo(**_parse_site_row(rows[0]))


def _parse_site_row(r: dict) -> dict:
    return {
        "site_id": r.get("site_id"),
        "site_name": r.get("site_name"),
        "description": r.get("description"),
        "latitude": float(r["latitude"]) if r.get("latitude") else None,
        "longitude": float(r["longitude"]) if r.get("longitude") else None,
    }
