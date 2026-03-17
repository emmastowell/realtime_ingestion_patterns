"""Lakebase (autoscaling PostgreSQL) query client.

Uses native PostgreSQL login for the app's service principal.
"""

from __future__ import annotations

import logging
import os
import ssl

logger = logging.getLogger(__name__)

LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "")
LAKEBASE_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
LAKEBASE_DATABASE = os.environ.get("LAKEBASE_DATABASE", "traffic")
LAKEBASE_USER = os.environ.get("LAKEBASE_USER", "")
LAKEBASE_PASSWORD = os.environ.get("LAKEBASE_PASSWORD", "")
LAKEBASE_SCHEMA = os.environ.get("LAKEBASE_SCHEMA", "realtime")
LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "nh-traffic")

_pool = None


async def get_pool():
    global _pool
    if _pool is not None:
        return _pool

    if not LAKEBASE_PASSWORD:
        logger.warning("LAKEBASE_PASSWORD not set")
        return None

    try:
        import asyncpg

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        _pool = await asyncpg.create_pool(
            host=LAKEBASE_HOST,
            port=LAKEBASE_PORT,
            database=LAKEBASE_DATABASE,
            user=LAKEBASE_USER,
            password=LAKEBASE_PASSWORD,
            ssl=ssl_ctx,
            min_size=1,
            max_size=5,
            server_settings={"search_path": f"{LAKEBASE_SCHEMA},public"},
        )
        logger.info("Lakebase pool created: %s@%s/%s", LAKEBASE_USER, LAKEBASE_HOST, LAKEBASE_DATABASE)
    except Exception:
        logger.exception("Failed to create Lakebase pool")
        _pool = None
        return None
    return _pool


async def query(sql: str) -> list[dict]:
    pool = await get_pool()
    if pool is None:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("Lakebase query failed: %s", e)
        global _pool
        _pool = None
        return []


async def query_one(sql: str) -> dict | None:
    rows = await query(sql)
    return rows[0] if rows else None
