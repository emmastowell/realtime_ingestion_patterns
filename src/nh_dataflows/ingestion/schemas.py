"""Pydantic v2 models for WebTRIS API responses."""

from __future__ import annotations

from pydantic import BaseModel, Field


class MidasSite(BaseModel):
    """A single MIDAS sensor site from the WebTRIS sites endpoint."""

    id: str = Field(alias="Id")
    name: str = Field(alias="Name")
    description: str = Field(alias="Description")
    longitude: float = Field(alias="Longitude")
    latitude: float = Field(alias="Latitude")
    status: str = Field(alias="Status")

    model_config = {"populate_by_name": True}


class SitesResponse(BaseModel):
    """Response from GET /sites."""

    sites: list[MidasSite] = Field(alias="sites")
    row_count: int = Field(alias="row_count")


class Area(BaseModel):
    """A WebTRIS area (geographic grouping of sites)."""

    id: str = Field(alias="Id")
    name: str = Field(alias="Name")
    description: str = Field(alias="Description")
    x_lon: float = Field(alias="XLongitude")
    y_lat: float = Field(alias="YLatitude")

    model_config = {"populate_by_name": True}


class AreasResponse(BaseModel):
    """Response from GET /areas."""

    areas: list[Area] = Field(alias="areas")
    row_count: int = Field(alias="row_count")


class TrafficReading(BaseModel):
    """A single 15-minute traffic reading from the daily report endpoint."""

    site_id: str = Field(alias="Site Name")
    report_date: str = Field(alias="Report Date")
    time_period_end: str = Field(alias="Time Period Ending")
    interval: int = Field(alias="Time Interval")
    total_volume: int | None = Field(default=None, alias="0 - Total Volume")
    avg_speed: float | None = Field(default=None, alias="0 - Avg mph")
    link_length_km: float | None = Field(default=None, alias="Total Link Length Km")
    link_length_miles: float | None = Field(default=None, alias="Total Link Length Miles")

    model_config = {"populate_by_name": True}


class DailyReportResponse(BaseModel):
    """Response from GET /reports/daily."""

    header: dict = Field(alias="Header")
    rows: list[TrafficReading] = Field(alias="Rows")
