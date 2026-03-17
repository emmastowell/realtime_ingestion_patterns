"""Typed httpx client for the WebTRIS REST API."""

from __future__ import annotations

from datetime import date

import httpx

from nh_dataflows.config import WEBTRIS_BASE_URL
from nh_dataflows.ingestion.schemas import (
    AreasResponse,
    DailyReportResponse,
    SitesResponse,
)


class WebTRISClient:
    """Async client for the WebTRIS traffic data API.

    Endpoints reference: https://webtris.nationalhighways.co.uk/api/swagger/ui/index
    """

    def __init__(self, base_url: str = WEBTRIS_BASE_URL, timeout: float = 30.0) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    async def __aenter__(self) -> WebTRISClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def get_sites(self) -> SitesResponse:
        """Fetch all MIDAS sensor sites."""
        resp = await self._client.get("/sites")
        resp.raise_for_status()
        return SitesResponse.model_validate(resp.json())

    async def get_sites_by_area(self, area_id: str) -> SitesResponse:
        """Fetch sites within a specific area."""
        resp = await self._client.get(f"/sites/{area_id}")
        resp.raise_for_status()
        return SitesResponse.model_validate(resp.json())

    async def get_areas(self) -> AreasResponse:
        """Fetch all geographic areas."""
        resp = await self._client.get("/areas")
        resp.raise_for_status()
        return AreasResponse.model_validate(resp.json())

    async def get_daily_report(
        self,
        site_ids: list[str],
        start_date: date,
        end_date: date,
        page: int = 1,
        page_size: int = 40000,
    ) -> DailyReportResponse:
        """Fetch daily traffic report for given sites and date range.

        Args:
            site_ids: List of MIDAS site IDs (e.g. ["1", "2", "3"]).
            start_date: Report start date (inclusive).
            end_date: Report end date (inclusive).
            page: Page number for paginated results.
            page_size: Number of rows per page.
        """
        sites_csv = ",".join(site_ids)
        date_fmt = "%d%m%Y"
        resp = await self._client.get(
            f"/reports/{sites_csv}/daily",
            params={
                "start_date": start_date.strftime(date_fmt),
                "end_date": end_date.strftime(date_fmt),
                "page": page,
                "page_size": page_size,
            },
        )
        resp.raise_for_status()
        return DailyReportResponse.model_validate(resp.json())
