"""FastAPI entry point for the Roadways Traffic API."""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.responses import FileResponse

from routers import assets, demo, health, traffic, traffic_lakebase


app = FastAPI(
    title="Roadways Traffic API",
    description=(
        "Real-time traffic data from MIDAS sensors. "
        "Powered by Zerobus Ingest, Spark Structured Streaming, and Databricks SQL Warehouse."
    ),
    version="0.1.0",
)

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@app.get("/", include_in_schema=False)
async def root():
    """Serve the demo UI."""
    return FileResponse(os.path.join(_STATIC_DIR, "demo.html"), media_type="text/html")


app.include_router(health.router)
app.include_router(traffic.router)
app.include_router(traffic_lakebase.router)
app.include_router(assets.router)
app.include_router(demo.router)
