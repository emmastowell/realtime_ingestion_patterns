"""SQL Warehouse query client for the Traffic API.

Uses the Databricks SQL Statement API via the Python SDK to query
Unity Catalog Delta tables directly — no intermediate database needed.
"""

from __future__ import annotations

import logging
import os

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_client: WorkspaceClient | None = None

WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
CATALOG_SCHEMA = os.environ.get("GOLD_TABLE_PREFIX", "transport.realtime")


def _get_client() -> WorkspaceClient:
    """Get or create the WorkspaceClient (reused across requests).

    On Databricks Apps, WorkspaceClient() with no args auto-detects the
    app's service principal. Only pass host explicitly if DATABRICKS_HOST
    is set AND we're not running inside a Databricks App.
    """
    global _client
    if _client is None:
        host = os.environ.get("DATABRICKS_HOST", "")
        # If running inside a Databricks App, let the SDK auto-detect
        if os.environ.get("DATABRICKS_APP_NAME"):
            _client = WorkspaceClient()
        elif host:
            _client = WorkspaceClient(host=host)
        else:
            _client = WorkspaceClient()
    return _client


def query(sql: str, timeout: str = "15s") -> list[dict]:
    """Execute a SQL query and return results as a list of dicts.

    Args:
        sql: SQL statement (can reference tables in transport.realtime).
        timeout: Max wait time for the query to complete.

    Returns:
        List of row dicts with column names as keys.
    """
    w = _get_client()
    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout=timeout,
    )

    if not result.status or result.status.state.value != "SUCCEEDED":
        error_msg = ""
        if result.status and result.status.error:
            error_msg = result.status.error.message
        logger.warning("SQL query failed: %s — %s", sql[:100], error_msg)
        return []

    if not result.result or not result.result.data_array:
        return []

    columns = [col.name for col in result.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in result.result.data_array]


def query_one(sql: str, timeout: str = "15s") -> dict | None:
    """Execute a SQL query and return the first row as a dict, or None."""
    rows = query(sql, timeout)
    return rows[0] if rows else None
