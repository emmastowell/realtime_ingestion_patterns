"""Tests for WebTRIS Pydantic models."""

from __future__ import annotations

import json
from pathlib import Path

from nh_dataflows.ingestion.schemas import (
    Area,
    MidasSite,
    TrafficReading,
)

SAMPLE_DIR = Path(__file__).parent.parent.parent / "data" / "sample"


class TestMidasSite:
    def test_parse_from_api_format(self) -> None:
        data = {
            "Id": "1234",
            "Name": "M25/2259B",
            "Description": "M25 J5-6 Clockwise",
            "Longitude": -0.1234,
            "Latitude": 51.3456,
            "Status": "Active",
        }
        site = MidasSite.model_validate(data)
        assert site.id == "1234"
        assert site.name == "M25/2259B"
        assert site.longitude == -0.1234
        assert site.latitude == 51.3456
        assert site.status == "Active"

    def test_populate_by_name(self) -> None:
        site = MidasSite(
            id="1", name="test", description="desc",
            longitude=0.0, latitude=51.0, status="Active",
        )
        assert site.id == "1"


class TestArea:
    def test_parse_from_api_format(self) -> None:
        data = {
            "Id": "1",
            "Name": "South East",
            "Description": "South East England",
            "XLongitude": -0.5,
            "YLatitude": 51.2,
        }
        area = Area.model_validate(data)
        assert area.id == "1"
        assert area.name == "South East"
        assert area.x_lon == -0.5


class TestTrafficReading:
    def test_parse_from_api_format(self) -> None:
        data = {
            "Site Name": "M25/2259B",
            "Report Date": "12/03/2026",
            "Time Period Ending": "08:15",
            "Time Interval": 900,
            "0 - Total Volume": 1842,
            "0 - Avg mph": 58.3,
            "Total Link Length Km": 1.2,
            "Total Link Length Miles": 0.75,
        }
        reading = TrafficReading.model_validate(data)
        assert reading.site_id == "M25/2259B"
        assert reading.report_date == "12/03/2026"
        assert reading.total_volume == 1842
        assert reading.avg_speed == 58.3

    def test_null_values_allowed(self) -> None:
        data = {
            "Site Name": "M25/2260A",
            "Report Date": "12/03/2026",
            "Time Period Ending": "08:15",
            "Time Interval": 900,
            "0 - Total Volume": None,
            "0 - Avg mph": None,
            "Total Link Length Km": 1.1,
            "Total Link Length Miles": 0.68,
        }
        reading = TrafficReading.model_validate(data)
        assert reading.total_volume is None
        assert reading.avg_speed is None


class TestSampleData:
    def test_load_sample_file(self) -> None:
        sample_path = SAMPLE_DIR / "webtris_daily_sample.json"
        data = json.loads(sample_path.read_text())
        readings = [TrafficReading.model_validate(r) for r in data]
        assert len(readings) == 5
        assert readings[0].site_id == "M25/2259B"
        assert readings[4].total_volume is None
