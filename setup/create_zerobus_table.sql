-- Create the Zerobus ingest target table.
-- Zerobus does NOT create or alter tables — this must exist before ingestion starts.
-- The table must be a managed Delta table in Unity Catalog.

CREATE SCHEMA IF NOT EXISTS transport.realtime;

CREATE TABLE IF NOT EXISTS transport.realtime.zerobus_traffic_ingest (
    site_id            STRING      NOT NULL,
    report_date        STRING      NOT NULL,
    time_period_end    STRING      NOT NULL,
    interval           INT,
    total_volume       INT,
    avg_speed          DOUBLE,
    link_length_km     DOUBLE,
    link_length_miles  DOUBLE,
    trace_id           STRING,
    ingestion_ts       LONG        -- Microsecond epoch (Zerobus requirement)
);

-- Create the gold tables that the streaming pipeline MERGEs/appends into.
-- These must exist before the foreachBatch writers run.

CREATE TABLE IF NOT EXISTS transport.realtime.gold_current_traffic (
    site_id          STRING,
    site_name        STRING,
    report_date      STRING,
    time_period_end  STRING,
    reading_ts       TIMESTAMP,
    total_volume     INT,
    avg_speed        DOUBLE,
    link_length_km   DOUBLE,
    latitude         DOUBLE,
    longitude        DOUBLE,
    h3_index_res10   STRING,
    h3_index_res7    STRING,
    road_name        STRING,
    direction        STRING,
    trace_id         STRING,
    ingest_method    STRING,
    ingestion_ts     TIMESTAMP,
    processed_ts     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transport.realtime.gold_traffic_by_road (
    road_name       STRING,
    direction       STRING,
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    mean_speed_mph  DOUBLE,
    total_flow      BIGINT,
    sensor_count    BIGINT,
    min_speed_mph   DOUBLE,
    max_speed_mph   DOUBLE
);

CREATE TABLE IF NOT EXISTS transport.realtime.gold_traffic_by_h3 (
    h3_index_res7   STRING,
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    mean_speed_mph  DOUBLE,
    total_flow      BIGINT,
    sensor_count    BIGINT
);

-- Grant service principal access for Zerobus writes
-- Replace <service-principal-uuid> with your actual SP UUID
-- GRANT USE CATALOG ON CATALOG transport TO `<service-principal-uuid>`;
-- GRANT USE SCHEMA ON SCHEMA transport.realtime TO `<service-principal-uuid>`;
-- GRANT MODIFY, SELECT ON TABLE transport.realtime.zerobus_traffic_ingest TO `<service-principal-uuid>`;
