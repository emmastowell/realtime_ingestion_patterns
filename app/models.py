"""Pydantic response models for the traffic API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    sql_warehouse_connected: bool
    latest_reading_ts: datetime | None = None
    pipeline_lag_seconds: float | None = None


class TrafficReading(BaseModel):
    site_id: str
    site_name: str | None = None
    road_name: str | None = None
    direction: str | None = None
    reading_ts: datetime | None = None
    avg_speed: float | None = None
    total_volume: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    h3_index_res7: str | None = None
    h3_index_res10: str | None = None


class RoadTraffic(BaseModel):
    road_name: str
    direction: str | None = None
    window_start: datetime
    window_end: datetime
    mean_speed_mph: float | None = None
    total_flow: int | None = None
    sensor_count: int = 0
    min_speed_mph: float | None = None
    max_speed_mph: float | None = None


class H3Traffic(BaseModel):
    h3_index_res7: str
    window_start: datetime
    window_end: datetime
    mean_speed_mph: float | None = None
    total_flow: int | None = None
    sensor_count: int = 0


class SiteInfo(BaseModel):
    site_id: str
    site_name: str | None = None
    description: str | None = None
    road_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    status: str | None = None
    h3_index_res7: str | None = None
    h3_index_res10: str | None = None
