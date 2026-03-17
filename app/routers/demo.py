"""Demo endpoints for live latency demonstration.

/api/v1/demo/inject — Push a test traffic record via Zerobus (Approach 1)
/api/v1/demo/inject-eventhub — Push a test XML record to real Azure Event Hub (Approach 2)
/api/v1/demo/inject-xml — Push a test XML record to Delta sim table (Approach 2 fallback)
/api/v1/demo/poll/{trace_id} — Poll until the record appears in the gold table
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from datetime import UTC, datetime

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from sql_warehouse import CATALOG_SCHEMA, query_one

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/demo", tags=["demo"])


class InjectRequest(BaseModel):
    site_id: str = "5138"
    avg_speed: float = 55.0
    total_volume: int = 1500
    link_length_km: float = 1.2


class InjectResponse(BaseModel):
    trace_id: str
    injected_at: str
    approach: str
    message: str
    xml_payload: str | None = None


class BatchInjectRequest(BaseModel):
    count: int = 10
    site_ids: list[str] | None = None
    speed_min: float = 30.0
    speed_max: float = 80.0
    volume_min: int = 500
    volume_max: int = 3000


class BatchInjectResponse(BaseModel):
    count: int
    approach: str
    first_trace_id: str
    last_trace_id: str
    elapsed_seconds: float
    message: str


class PollResponse(BaseModel):
    trace_id: str
    found: bool
    elapsed_seconds: float | None = None
    injected_at: str | None = None
    appeared_at: str | None = None
    ingestion_ts: str | None = None
    processed_ts: str | None = None
    ingest_method: str | None = None
    site_id: str | None = None
    road_name: str | None = None
    avg_speed: float | None = None


def _build_xml(req: InjectRequest, trace_id: str, now: datetime) -> str:
    """Build an XML traffic payload matching the DfC XML format."""
    return (
        "<TrafficData>"
        f"<SiteId>{req.site_id}</SiteId>"
        f"<ReportDate>{now.strftime('%d/%m/%Y')}</ReportDate>"
        f"<TimePeriodEnd>{now.strftime('%H:%M')}</TimePeriodEnd>"
        f"<Interval>900</Interval>"
        f"<TotalVolume>{req.total_volume}</TotalVolume>"
        f"<AvgSpeed>{req.avg_speed}</AvgSpeed>"
        f"<LinkLengthKm>{req.link_length_km}</LinkLengthKm>"
        f"<LinkLengthMiles>{round(req.link_length_km * 0.621371, 2)}</LinkLengthMiles>"
        f"<TraceId>{trace_id}</TraceId>"
        "</TrafficData>"
    )


def _get_workspace_client() -> WorkspaceClient:
    if os.environ.get("DATABRICKS_APP_NAME"):
        return WorkspaceClient()
    return WorkspaceClient(
        host=os.environ.get("DATABRICKS_HOST", ""),
    )


@router.post("/inject", response_model=InjectResponse)
async def inject_record(req: InjectRequest) -> InjectResponse:
    """Inject a test traffic record via Zerobus (Approach 1).

    Returns a trace_id that can be used with /poll to track when
    the record appears in the gold table.
    """
    try:
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk
    except ImportError:
        raise HTTPException(status_code=400, detail="Zerobus SDK not available on this workspace.")

    server_endpoint = os.environ.get("ZEROBUS_SERVER_ENDPOINT")
    if not server_endpoint:
        raise HTTPException(status_code=400, detail="Zerobus not configured on this workspace. Use Event Hub inject instead.")
    workspace_url = os.environ["DATABRICKS_WORKSPACE_URL"]
    table_name = os.environ.get(
        "ZEROBUS_TABLE_NAME", "transport.realtime.zerobus_traffic_ingest"
    )
    client_id = os.environ["ZEROBUS_CLIENT_ID"]
    client_secret = os.environ["ZEROBUS_CLIENT_SECRET"]

    trace_id = str(uuid.uuid4())
    now = datetime.now(UTC)

    record = {
        "site_id": req.site_id,
        "report_date": now.strftime("%d/%m/%Y"),
        "time_period_end": now.strftime("%H:%M"),
        "interval": 900,
        "total_volume": req.total_volume,
        "avg_speed": req.avg_speed,
        "link_length_km": req.link_length_km,
        "link_length_miles": round(req.link_length_km * 0.621371, 2),
        "trace_id": trace_id,
        "ingestion_ts": int(now.timestamp() * 1_000_000),
    }

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    table_props = TableProperties(table_name)
    stream = sdk.create_stream(client_id, client_secret, table_props, options)

    try:
        ack = stream.ingest_record(record)
        ack.wait_for_ack()
    finally:
        stream.close()

    import json as _json
    return InjectResponse(
        trace_id=trace_id,
        injected_at=now.isoformat(),
        approach="zerobus",
        xml_payload=_json.dumps(record, indent=2),
        message="Record injected via Zerobus (Approach 1). Use /poll/{trace_id} to track.",
    )


@router.post("/inject-eventhub", response_model=InjectResponse)
async def inject_eventhub_record(req: InjectRequest) -> InjectResponse:
    """Inject a test XML traffic record into the real Azure Event Hub (Approach 2).

    Publishes an XML payload to Azure Event Hub using the native SDK.
    The streaming pipeline reads from Event Hub via Kafka protocol,
    parses the XML natively in Spark, and writes to gold tables.

    Returns a trace_id that can be used with /poll to track when
    the record appears in the gold table.
    """
    try:
        from azure.eventhub import EventData, EventHubProducerClient
    except ImportError:
        raise HTTPException(status_code=400, detail="azure-eventhub SDK not installed.")

    trace_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    xml_payload = _build_xml(req, trace_id, now)

    eh_conn_str = os.environ.get("EVENTHUB_SEND_CONN_STR", "")
    if not eh_conn_str:
        raise HTTPException(status_code=400, detail="Event Hub not configured. Set EVENTHUB_SEND_CONN_STR.")
    eh_topic = os.environ.get("EVENTHUB_TOPIC", "traffic-data")

    try:
        producer = EventHubProducerClient.from_connection_string(
            eh_conn_str, eventhub_name=eh_topic
        )
        with producer:
            batch = producer.create_batch()
            batch.add(EventData(xml_payload))
            producer.send_batch(batch)

        return InjectResponse(
            trace_id=trace_id,
            injected_at=now.isoformat(),
            approach="eventhub",
            message=(
                f"XML record published to Azure Event Hub. "
                f"Use /poll/{trace_id} to track."
            ),
        )
    except Exception as e:
        # Fall back to Delta simulation table if Event Hub is unreachable
        logger.warning("Event Hub publish failed (%s), falling back to Delta sim: %s", type(e).__name__, e)
        table_name = os.environ.get(
            "EVENTHUB_TABLE_NAME", "transport.realtime.eventhub_xml_ingest"
        )
        warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
        w = _get_workspace_client()
        escaped_xml = xml_payload.replace("'", "''")
        sql = (
            f"INSERT INTO {table_name} (value, ingestion_ts, trace_id) "
            f"VALUES ('{escaped_xml}', '{now.isoformat()}', '{trace_id}')"
        )
        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, wait_timeout="30s",
        )
        return InjectResponse(
            trace_id=trace_id,
            injected_at=now.isoformat(),
            approach="eventhub_xml",
            message=(
                f"Event Hub unreachable from app — fell back to Delta simulation table. "
                f"Record will still be picked up by the streaming pipeline. "
                f"Use /poll/{trace_id} to track."
            ),
        )


@router.post("/inject-xml", response_model=InjectResponse)
async def inject_xml_record(req: InjectRequest) -> InjectResponse:
    """Inject a test XML record into the Delta simulation table (Approach 2 fallback).

    Writes a raw XML payload to the Unity Catalog Delta table
    transport.realtime.eventhub_xml_ingest via the SQL Statement API.

    Returns a trace_id that can be used with /poll to track when
    the record appears in the gold table.
    """
    trace_id = str(uuid.uuid4())
    now = datetime.now(UTC)

    xml_payload = _build_xml(req, trace_id, now)
    table_name = os.environ.get(
        "EVENTHUB_TABLE_NAME", "transport.realtime.eventhub_xml_ingest"
    )
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")

    w = _get_workspace_client()

    # Escape single quotes in XML payload for SQL
    escaped_xml = xml_payload.replace("'", "''")
    sql = (
        f"INSERT INTO {table_name} (value, ingestion_ts, trace_id) "
        f"VALUES ('{escaped_xml}', '{now.isoformat()}', '{trace_id}')"
    )

    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )
    if result.status and result.status.state.value not in ("SUCCEEDED", "RUNNING"):
        logger.error("SQL Statement API error: %s", result.status)

    return InjectResponse(
        trace_id=trace_id,
        injected_at=now.isoformat(),
        approach="eventhub_xml",
        xml_payload=xml_payload,
        message=(
            f"XML record injected into UC Delta table via SQL Statement API. "
            f"Use /poll/{trace_id} to track."
        ),
    )


@router.post("/inject-batch", response_model=BatchInjectResponse)
async def inject_batch(req: BatchInjectRequest) -> BatchInjectResponse:
    """Inject multiple test records via the XML Delta simulation path.

    Generates randomised traffic readings and inserts them as a single
    multi-row INSERT for efficiency.
    """
    import random

    try:
        return await _do_inject_batch(req, random)
    except Exception as e:
        logger.exception("Batch inject failed")
        raise HTTPException(status_code=500, detail=f"Batch inject failed: {type(e).__name__}: {e}")


async def _do_inject_batch(req, random):
    # Default site IDs — real MIDAS sites on M25, M1, M6
    site_ids = req.site_ids or [
        "5138", "5139", "5140", "5141", "5142", "5148", "5149",
        "5688", "5689", "5690", "5691", "5692", "5693", "5694",
        "6248", "6249", "6250", "6251", "6252", "6253", "6254",
    ]

    start = time.monotonic()
    first_trace = last_trace = None

    # Build all XML payloads
    payloads = []
    for i in range(req.count):
        trace_id = str(uuid.uuid4())
        if first_trace is None:
            first_trace = trace_id
        last_trace = trace_id

        now = datetime.now(UTC)
        site_id = random.choice(site_ids)
        speed = round(random.uniform(req.speed_min, req.speed_max), 1)
        volume = random.randint(req.volume_min, req.volume_max)
        link_km = round(random.uniform(0.5, 2.5), 2)

        inject_req = InjectRequest(
            site_id=site_id, avg_speed=speed,
            total_volume=volume, link_length_km=link_km,
        )
        payloads.append(_build_xml(inject_req, trace_id, now))

    # Try Event Hub first, fall back to Delta simulation
    eh_conn_str = os.environ.get("EVENTHUB_SEND_CONN_STR", "")
    eh_topic = os.environ.get("EVENTHUB_TOPIC", "traffic-data")
    approach = "eventhub_batch"

    if eh_conn_str:
        try:
            from azure.eventhub import EventData, EventHubProducerClient

            producer = EventHubProducerClient.from_connection_string(
                eh_conn_str, eventhub_name=eh_topic
            )
            with producer:
                # Send in batches (Event Hub batch size limit)
                for chunk_start in range(0, len(payloads), 100):
                    chunk = payloads[chunk_start:chunk_start + 100]
                    batch = producer.create_batch()
                    for xml in chunk:
                        try:
                            batch.add(EventData(xml))
                        except ValueError:
                            # Batch full, send and start new
                            producer.send_batch(batch)
                            batch = producer.create_batch()
                            batch.add(EventData(xml))
                    producer.send_batch(batch)
        except Exception as e:
            logger.warning("Event Hub batch failed (%s), falling back to Delta sim", e)
            approach = "eventhub_xml_batch"
            await _batch_to_delta(payloads, first_trace, last_trace)
    else:
        approach = "eventhub_xml_batch"
        await _batch_to_delta(payloads, first_trace, last_trace)

    elapsed = time.monotonic() - start

    return BatchInjectResponse(
        count=req.count,
        approach=approach,
        first_trace_id=first_trace,
        last_trace_id=last_trace,
        elapsed_seconds=round(elapsed, 2),
        message=f"Injected {req.count} records via {approach} in {elapsed:.1f}s.",
    )


async def _batch_to_delta(payloads, first_trace, last_trace):
    """Fallback: insert XML payloads into Delta simulation table."""
    table_name = os.environ.get(
        "EVENTHUB_TABLE_NAME", "transport.realtime.eventhub_xml_ingest"
    )
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    w = _get_workspace_client()

    # Re-extract trace_id and ingestion_ts from the XML payloads
    import re
    values_parts = []
    for xml in payloads:
        trace_match = re.search(r"<TraceId>(.*?)</TraceId>", xml)
        trace_id = trace_match.group(1) if trace_match else ""
        now = datetime.now(UTC)
        escaped = xml.replace("'", "''")
        values_parts.append(f"('{escaped}', '{now.isoformat()}', '{trace_id}')")

    for chunk_start in range(0, len(values_parts), 100):
        chunk = values_parts[chunk_start:chunk_start + 100]
        sql = (
            f"INSERT INTO {table_name} (value, ingestion_ts, trace_id) VALUES "
            + ", ".join(chunk)
        )
        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, wait_timeout="50s",
        )


@router.get("/poll/{trace_id}", response_model=PollResponse)
async def poll_record(
    trace_id: str,
    timeout: float = Query(30.0, ge=1, le=60, description="Max seconds to poll"),
) -> PollResponse:
    """Poll for a record to appear in the gold_current_traffic table.

    Queries the UC Delta gold table directly via SQL Warehouse.
    Returns immediately if found, otherwise polls every 2s up to timeout.
    Reports the end-to-end latency from injection to gold table appearance.
    """
    start = time.monotonic()

    while (time.monotonic() - start) < timeout:
        row = query_one(
            f"SELECT site_id, road_name, avg_speed, ingest_method, "
            f"CAST(ingestion_ts AS STRING) AS ingestion_ts, "
            f"CAST(processed_ts AS STRING) AS processed_ts "
            f"FROM {CATALOG_SCHEMA}.gold_current_traffic "
            f"WHERE trace_id = '{trace_id}'"
        )
        if row is not None:
            elapsed = time.monotonic() - start
            return PollResponse(
                trace_id=trace_id,
                found=True,
                elapsed_seconds=round(elapsed, 2),
                appeared_at=datetime.now(UTC).isoformat(),
                ingestion_ts=row.get("ingestion_ts"),
                processed_ts=row.get("processed_ts"),
                ingest_method=row.get("ingest_method"),
                site_id=row.get("site_id"),
                road_name=row.get("road_name"),
                avg_speed=float(row["avg_speed"]) if row.get("avg_speed") else None,
            )

        await asyncio.sleep(2)

    return PollResponse(
        trace_id=trace_id,
        found=False,
        elapsed_seconds=round(time.monotonic() - start, 2),
    )


@router.get("/poll-lakebase/{trace_id}", response_model=PollResponse)
async def poll_lakebase_record(
    trace_id: str,
    timeout: float = Query(30.0, ge=1, le=60, description="Max seconds to poll"),
) -> PollResponse:
    """Poll Lakebase for a record written by the RTM pipeline.

    Queries the Lakebase PostgreSQL gold table directly via asyncpg.
    """
    from lakebase import query_one as lb_query_one

    start = time.monotonic()

    while (time.monotonic() - start) < timeout:
        row = await lb_query_one(
            f"SELECT site_id, road_name, avg_speed, ingest_method, "
            f"CAST(ingestion_ts AS TEXT) AS ingestion_ts, "
            f"CAST(processed_ts AS TEXT) AS processed_ts "
            f"FROM realtime.gold_current_traffic "
            f"WHERE trace_id = '{trace_id}'"
        )
        if row is not None:
            elapsed = time.monotonic() - start
            return PollResponse(
                trace_id=trace_id,
                found=True,
                elapsed_seconds=round(elapsed, 2),
                appeared_at=datetime.now(UTC).isoformat(),
                ingestion_ts=str(row.get("ingestion_ts", "")),
                processed_ts=str(row.get("processed_ts", "")),
                ingest_method=str(row.get("ingest_method", "")),
                site_id=str(row.get("site_id", "")),
                road_name=str(row.get("road_name", "")),
                avg_speed=float(row["avg_speed"]) if row.get("avg_speed") is not None else None,
            )

        await asyncio.sleep(0.5)  # Poll faster for RTM (sub-second expected)

    return PollResponse(
        trace_id=trace_id,
        found=False,
        elapsed_seconds=round(time.monotonic() - start, 2),
    )
