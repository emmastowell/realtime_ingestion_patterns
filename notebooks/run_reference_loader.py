# Databricks notebook source
# MAGIC %md
# MAGIC # Load Reference Data (MIDAS Sites + Areas)
# MAGIC
# MAGIC Fetches all 19,518 MIDAS sensor sites and 25 areas from the WebTRIS API,
# MAGIC pre-computes H3 geospatial indexes, and writes to Delta tables.

# COMMAND ----------

# MAGIC %pip install httpx pydantic nest_asyncio -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import asyncio
import nest_asyncio
import httpx
from pydantic import BaseModel, Field
from pyspark.sql import functions as F

nest_asyncio.apply()

# --- Config ---
CATALOG = os.environ.get("NH_CATALOG", "transport")
SCHEMA = os.environ.get("NH_SCHEMA", "realtime")
TABLE_REF_SITES = f"{CATALOG}.{SCHEMA}.ref_midas_sites"
TABLE_REF_AREAS = f"{CATALOG}.{SCHEMA}.ref_areas"
WEBTRIS_BASE_URL = "https://webtris.nationalhighways.co.uk/api/v1.0"

print(f"Catalog: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# --- Pydantic models ---
class MidasSite(BaseModel):
    id: str = Field(alias="Id")
    name: str = Field(alias="Name")
    description: str = Field(alias="Description")
    longitude: float = Field(alias="Longitude")
    latitude: float = Field(alias="Latitude")
    status: str = Field(alias="Status")
    model_config = {"populate_by_name": True}

class SitesResponse(BaseModel):
    sites: list[MidasSite] = Field(alias="sites")
    row_count: int = Field(alias="row_count")

class Area(BaseModel):
    id: str = Field(alias="Id")
    name: str = Field(alias="Name")
    description: str = Field(alias="Description")
    x_lon: float = Field(alias="XLongitude")
    y_lat: float = Field(alias="YLatitude")
    model_config = {"populate_by_name": True}

class AreasResponse(BaseModel):
    areas: list[Area] = Field(alias="areas")
    row_count: int = Field(alias="row_count")

# COMMAND ----------

# --- Fetch from WebTRIS API ---
async def fetch_sites_and_areas():
    async with httpx.AsyncClient(base_url=WEBTRIS_BASE_URL, timeout=30.0, headers={"Accept": "application/json"}) as client:
        sites_resp = await client.get("/sites")
        sites_resp.raise_for_status()
        sites = SitesResponse.model_validate(sites_resp.json())

        areas_resp = await client.get("/areas")
        areas_resp.raise_for_status()
        areas = AreasResponse.model_validate(areas_resp.json())

    return [s.model_dump() for s in sites.sites], [a.model_dump() for a in areas.areas]

sites_data, areas_data = asyncio.run(fetch_sites_and_areas())
print(f"Fetched {len(sites_data)} sites and {len(areas_data)} areas")

# COMMAND ----------

# --- Load sites with H3 indexes ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

df = spark.createDataFrame(sites_data)
df_sites = df.select(
    F.col("id").cast("string").alias("site_id"),
    F.col("name").alias("site_name"),
    F.col("description"),
    F.col("longitude").cast("double"),
    F.col("latitude").cast("double"),
    F.col("status"),
    F.expr("CAST(h3_longlatash3(longitude, latitude, 10) AS STRING)").alias("h3_index_res10"),
    F.expr("CAST(h3_longlatash3(longitude, latitude, 7) AS STRING)").alias("h3_index_res7"),
    F.expr("regexp_extract(description, '(M\\\\d+|A\\\\d+)', 1)").alias("road_name"),
)
df_sites.write.mode("overwrite").saveAsTable(TABLE_REF_SITES)
print(f"Loaded {spark.table(TABLE_REF_SITES).count()} sites to {TABLE_REF_SITES}")

# COMMAND ----------

# --- Load areas ---
df_a = spark.createDataFrame(areas_data)
df_areas = df_a.select(
    F.col("id").cast("string").alias("area_id"),
    F.col("name").alias("area_name"),
    F.col("description"),
    F.col("x_lon").cast("double").alias("longitude"),
    F.col("y_lat").cast("double").alias("latitude"),
)
df_areas.write.mode("overwrite").saveAsTable(TABLE_REF_AREAS)
print(f"Loaded {spark.table(TABLE_REF_AREAS).count()} areas to {TABLE_REF_AREAS}")
