"""Shared configuration constants for the traffic data pipeline."""

# Unity Catalog locations (overridable via env vars for multi-workspace deploy)
import os
CATALOG = os.environ.get("NH_CATALOG", "transport")
SCHEMA = os.environ.get("NH_SCHEMA", "realtime")
VOLUME_RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw_traffic"
VOLUME_REFERENCE = f"/Volumes/{CATALOG}/{SCHEMA}/reference"

# WebTRIS API
WEBTRIS_BASE_URL = "https://webtris.nationalhighways.co.uk/api/v1.0"

# Focus motorways — high-coverage MIDAS sites on strategic routes
FOCUS_ROADS: list[str] = ["M25", "M1", "M6"]

# H3 geospatial resolutions
H3_RESOLUTION_FINE = 10  # ~66m — sensor-level precision
H3_RESOLUTION_COARSE = 7  # ~1.2km — area-level aggregation

# Pipeline settings
WATERMARK_DELAY = "1 hour"
FETCH_INTERVAL_MINUTES = 5

# Reference table names
TABLE_REF_SITES = f"{CATALOG}.{SCHEMA}.ref_midas_sites"
TABLE_REF_AREAS = f"{CATALOG}.{SCHEMA}.ref_areas"

# Streaming table names (raw Structured Streaming path — low-latency demo)
TABLE_BRONZE_TRAFFIC = f"{CATALOG}.{SCHEMA}.bronze_traffic_readings"
TABLE_SILVER_TRAFFIC = f"{CATALOG}.{SCHEMA}.silver_traffic_readings"
TABLE_GOLD_CURRENT = f"{CATALOG}.{SCHEMA}.gold_current_traffic"
TABLE_GOLD_BY_ROAD = f"{CATALOG}.{SCHEMA}.gold_traffic_by_road"
TABLE_GOLD_BY_H3 = f"{CATALOG}.{SCHEMA}.gold_traffic_by_h3"

# Zerobus target table (Zerobus writes directly here, streaming reads from it)
TABLE_ZEROBUS_INGEST = f"{CATALOG}.{SCHEMA}.zerobus_traffic_ingest"

# Event Hub simulation table (fallback: XML payloads via Delta)
TABLE_EVENTHUB_INGEST = f"{CATALOG}.{SCHEMA}.eventhub_xml_ingest"

# Azure Event Hub (Kafka-compatible) — real streaming path
EVENTHUB_NAMESPACE = "nh-traffic-eh.servicebus.windows.net"
EVENTHUB_TOPIC = "traffic-data"
EVENTHUB_LISTEN_CONN_STR = os.environ.get("EVENTHUB_LISTEN_CONN_STR", "")
EVENTHUB_SEND_CONN_STR = os.environ.get("EVENTHUB_SEND_CONN_STR", "")

# Checkpoint locations for raw Structured Streaming
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# Streaming trigger interval for low-latency demo
STREAMING_TRIGGER_INTERVAL = "1 second"

# Lakebase credentials (set via environment variables or Databricks secrets)
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "")
LAKEBASE_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
LAKEBASE_DATABASE = os.environ.get("LAKEBASE_DATABASE", "traffic")
LAKEBASE_USER = os.environ.get("LAKEBASE_USER", "")
LAKEBASE_PASSWORD = os.environ.get("LAKEBASE_PASSWORD", "")
LAKEBASE_SCHEMA = os.environ.get("LAKEBASE_SCHEMA", "realtime")
