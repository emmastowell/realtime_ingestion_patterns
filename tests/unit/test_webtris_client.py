"""Tests for the WebTRIS API client using respx for HTTP mocking."""

from __future__ import annotations

from datetime import date

import httpx
import pytest
import respx

from nh_dataflows.ingestion.webtris_client import WebTRISClient


@pytest.fixture
def mock_api() -> respx.MockRouter:
    with respx.mock(base_url="https://webtris.nationalhighways.co.uk/api/v1.0") as router:
        yield router


@pytest.fixture
def client() -> WebTRISClient:
    return WebTRISClient()


SITES_RESPONSE = {
    "sites": [
        {
            "Id": "1",
            "Name": "M25/2259B",
            "Description": "M25 J5-6 Clockwise",
            "Longitude": -0.1234,
            "Latitude": 51.3456,
            "Status": "Active",
        },
        {
            "Id": "2",
            "Name": "M1/4567A",
            "Description": "M1 J6-7 Northbound",
            "Longitude": -0.3456,
            "Latitude": 51.7890,
            "Status": "Active",
        },
    ],
    "row_count": 2,
}

AREAS_RESPONSE = {
    "areas": [
        {
            "Id": "1",
            "Name": "South East",
            "Description": "South East England",
            "XLongitude": -0.5,
            "YLatitude": 51.2,
        }
    ],
    "row_count": 1,
}

DAILY_REPORT_RESPONSE = {
    "Header": {"report_date": "12/03/2026"},
    "Rows": [
        {
            "Site Name": "M25/2259B",
            "Report Date": "12/03/2026",
            "Time Period Ending": "08:15",
            "Time Interval": 900,
            "0 - Total Volume": 1842,
            "0 - Avg mph": 58.3,
            "Total Link Length Km": 1.2,
            "Total Link Length Miles": 0.75,
        }
    ],
}


@pytest.mark.asyncio
async def test_get_sites(mock_api: respx.MockRouter, client: WebTRISClient) -> None:
    mock_api.get("/sites").mock(return_value=httpx.Response(200, json=SITES_RESPONSE))

    async with client:
        result = await client.get_sites()

    assert result.row_count == 2
    assert len(result.sites) == 2
    assert result.sites[0].name == "M25/2259B"


@pytest.mark.asyncio
async def test_get_areas(mock_api: respx.MockRouter, client: WebTRISClient) -> None:
    mock_api.get("/areas").mock(return_value=httpx.Response(200, json=AREAS_RESPONSE))

    async with client:
        result = await client.get_areas()

    assert result.row_count == 1
    assert result.areas[0].name == "South East"


@pytest.mark.asyncio
async def test_get_daily_report(mock_api: respx.MockRouter, client: WebTRISClient) -> None:
    mock_api.get("/reports/1,2/daily").mock(
        return_value=httpx.Response(200, json=DAILY_REPORT_RESPONSE)
    )

    async with client:
        result = await client.get_daily_report(
            site_ids=["1", "2"],
            start_date=date(2026, 3, 12),
            end_date=date(2026, 3, 12),
        )

    assert len(result.rows) == 1
    assert result.rows[0].site_id == "M25/2259B"
    assert result.rows[0].avg_speed == 58.3


@pytest.mark.asyncio
async def test_client_raises_on_error(mock_api: respx.MockRouter, client: WebTRISClient) -> None:
    mock_api.get("/sites").mock(return_value=httpx.Response(500))

    async with client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_sites()
