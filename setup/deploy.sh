#!/bin/bash
# =============================================================================
# Real-Time Traffic Pipeline — Full Deployment Script
#
# Deploys the complete demo to a Databricks Azure workspace including:
#   - Unity Catalog schema and tables
#   - Azure Event Hub (namespace, topic, SAS policies)
#   - Lakebase Autoscaling (PostgreSQL for RTM path)
#   - Zerobus service principal
#   - Databricks App with environment variables
#   - All streaming jobs
#   - Reference data load
#
# Prerequisites:
#   - Databricks CLI v0.285+ authenticated (databricks auth login)
#   - Azure CLI authenticated (az login)
#   - Azure subscription with Event Hub and resource group permissions
#
# Usage:
#   ./setup/deploy.sh --profile <databricks-profile> \
#                     --subscription <azure-subscription-id> \
#                     --region <azure-region>
# =============================================================================

set -euo pipefail

# --- Parse arguments ---
PROFILE=""
SUBSCRIPTION=""
REGION="eastus"
CATALOG="highways_realtime_pipeline"
SCHEMA="realtime"
EH_NAMESPACE="nh-traffic-eh"
EH_TOPIC="traffic-data"
EH_RG="nh-traffic-demo-rg"
LAKEBASE_PROJECT="nh-traffic"
SKIP_EVENTHUB=false
SKIP_LAKEBASE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --profile) PROFILE="$2"; shift 2 ;;
    --subscription) SUBSCRIPTION="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --catalog) CATALOG="$2"; shift 2 ;;
    --skip-eventhub) SKIP_EVENTHUB=true; shift ;;
    --skip-lakebase) SKIP_LAKEBASE=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [ -z "$PROFILE" ]; then
  echo "Usage: ./setup/deploy.sh --profile <databricks-profile> [--subscription <azure-sub>] [--region <region>]"
  exit 1
fi

echo "=== Traffic Pipeline Deployment ==="
echo "Profile:      $PROFILE"
echo "Region:       $REGION"
echo "Catalog:      $CATALOG.$SCHEMA"
echo ""

# --- Step 1: Get workspace info ---
echo "--- Step 1: Workspace info ---"
WS_HOST=$(databricks auth env --profile "$PROFILE" 2>/dev/null | grep DATABRICKS_HOST | cut -d= -f2 || true)
if [ -z "$WS_HOST" ]; then
  WS_HOST=$(grep -A1 "\[$PROFILE\]" ~/.databrickscfg | grep host | awk '{print $3}')
fi
echo "Workspace: $WS_HOST"

# Get first available SQL warehouse
WH_ID=$(databricks warehouses list --profile "$PROFILE" --output json 2>/dev/null | \
  python3 -c "import json,sys; whs=json.load(sys.stdin); print(whs[0]['id'] if whs else '')" 2>/dev/null || true)
echo "SQL Warehouse: $WH_ID"

# --- Step 2: Create Unity Catalog schema and tables ---
echo ""
echo "--- Step 2: Create UC schema and tables ---"

python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile='$PROFILE')

statements = [
    'CREATE SCHEMA IF NOT EXISTS $CATALOG.$SCHEMA',
    '''CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.zerobus_traffic_ingest (
        site_id STRING NOT NULL, report_date STRING NOT NULL, time_period_end STRING NOT NULL,
        interval INT, total_volume INT, avg_speed DOUBLE, link_length_km DOUBLE,
        link_length_miles DOUBLE, trace_id STRING, ingestion_ts LONG
    )''',
    '''CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.eventhub_xml_ingest (
        value STRING, ingestion_ts TIMESTAMP, trace_id STRING
    )''',
    '''CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.gold_current_traffic (
        site_id STRING, site_name STRING, report_date STRING, time_period_end STRING,
        reading_ts TIMESTAMP, total_volume INT, avg_speed DOUBLE, link_length_km DOUBLE,
        latitude DOUBLE, longitude DOUBLE, h3_index_res10 STRING, h3_index_res7 STRING,
        road_name STRING, direction STRING, trace_id STRING, ingest_method STRING,
        ingestion_ts TIMESTAMP, processed_ts TIMESTAMP
    )''',
    '''CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.bronze_traffic (
        value STRING, ingestion_ts TIMESTAMP, trace_id STRING,
        kafka_timestamp TIMESTAMP, bronze_ts TIMESTAMP
    )''',
    '''CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.silver_traffic (
        site_id STRING, site_name STRING, report_date STRING, time_period_end STRING,
        reading_ts TIMESTAMP, total_volume INT, avg_speed DOUBLE, link_length_km DOUBLE,
        latitude DOUBLE, longitude DOUBLE, h3_index_res10 STRING, h3_index_res7 STRING,
        road_name STRING, direction STRING, trace_id STRING, ingest_method STRING,
        ingestion_ts TIMESTAMP, bronze_ts TIMESTAMP, silver_ts TIMESTAMP
    )''',
    'CREATE VOLUME IF NOT EXISTS $CATALOG.$SCHEMA.checkpoints',
    'CREATE VOLUME IF NOT EXISTS $CATALOG.$SCHEMA.raw_traffic',
    '''CREATE OR REPLACE VIEW $CATALOG.$SCHEMA.v_traffic_by_road AS
    SELECT road_name, direction,
        window(reading_ts, '15 minutes').start AS window_start,
        window(reading_ts, '15 minutes').end AS window_end,
        AVG(avg_speed) AS mean_speed_mph, SUM(total_volume) AS total_flow,
        COUNT(DISTINCT site_id) AS sensor_count, MIN(avg_speed) AS min_speed_mph,
        MAX(avg_speed) AS max_speed_mph
    FROM $CATALOG.$SCHEMA.gold_current_traffic
    WHERE road_name IS NOT NULL AND road_name != ''
    GROUP BY road_name, direction, window(reading_ts, '15 minutes')''',
    '''CREATE OR REPLACE VIEW $CATALOG.$SCHEMA.v_traffic_by_h3 AS
    SELECT h3_index_res7,
        window(reading_ts, '15 minutes').start AS window_start,
        window(reading_ts, '15 minutes').end AS window_end,
        AVG(avg_speed) AS mean_speed_mph, SUM(total_volume) AS total_flow,
        COUNT(DISTINCT site_id) AS sensor_count
    FROM $CATALOG.$SCHEMA.gold_current_traffic
    WHERE h3_index_res7 IS NOT NULL
    GROUP BY h3_index_res7, window(reading_ts, '15 minutes')''',
]

for sql in statements:
    r = w.statement_execution.execute_statement(warehouse_id='$WH_ID', statement=sql, wait_timeout='30s')
    label = sql.strip().split('(')[0].strip()[:60]
    print(f'  {r.status.state.value}: {label}')
"

# --- Step 3: Create Azure Event Hub ---
if [ "$SKIP_EVENTHUB" = false ] && [ -n "$SUBSCRIPTION" ]; then
  echo ""
  echo "--- Step 3: Create Azure Event Hub ---"
  az account set --subscription "$SUBSCRIPTION"

  az group create --name "$EH_RG" --location "$REGION" --output table 2>/dev/null

  echo "  Creating namespace (Standard, Kafka enabled)..."
  az eventhubs namespace create \
    --name "$EH_NAMESPACE" \
    --resource-group "$EH_RG" \
    --location "$REGION" \
    --sku Standard \
    --enable-kafka true \
    --output none 2>/dev/null

  echo "  Creating topic: $EH_TOPIC"
  az eventhubs eventhub create \
    --name "$EH_TOPIC" \
    --namespace-name "$EH_NAMESPACE" \
    --resource-group "$EH_RG" \
    --partition-count 1 \
    --cleanup-policy Delete \
    --retention-time 168 \
    --output none 2>/dev/null

  echo "  Creating SAS policies..."
  az eventhubs eventhub authorization-rule create \
    --name send-policy --eventhub-name "$EH_TOPIC" \
    --namespace-name "$EH_NAMESPACE" --resource-group "$EH_RG" \
    --rights Send --output none 2>/dev/null

  az eventhubs eventhub authorization-rule create \
    --name listen-policy --eventhub-name "$EH_TOPIC" \
    --namespace-name "$EH_NAMESPACE" --resource-group "$EH_RG" \
    --rights Listen --output none 2>/dev/null

  # Get connection strings
  EH_SEND_CONN=$(az eventhubs eventhub authorization-rule keys list \
    --name send-policy --eventhub-name "$EH_TOPIC" \
    --namespace-name "$EH_NAMESPACE" --resource-group "$EH_RG" \
    --query primaryConnectionString -o tsv)

  EH_LISTEN_CONN=$(az eventhubs eventhub authorization-rule keys list \
    --name listen-policy --eventhub-name "$EH_TOPIC" \
    --namespace-name "$EH_NAMESPACE" --resource-group "$EH_RG" \
    --query primaryConnectionString -o tsv)

  echo "  Send:   ${EH_SEND_CONN:0:60}..."
  echo "  Listen: ${EH_LISTEN_CONN:0:60}..."
else
  echo ""
  echo "--- Step 3: Skipping Event Hub (use --subscription to create) ---"
  EH_SEND_CONN="${EVENTHUB_SEND_CONN_STR:-}"
  EH_LISTEN_CONN="${EVENTHUB_LISTEN_CONN_STR:-}"
fi

# --- Step 4: Create Lakebase Autoscaling ---
if [ "$SKIP_LAKEBASE" = false ]; then
  echo ""
  echo "--- Step 4: Create Lakebase Autoscaling ---"

  # Check if project already exists
  EXISTING=$(databricks postgres list-projects --profile "$PROFILE" --output json 2>/dev/null | \
    python3 -c "import json,sys; ps=json.load(sys.stdin); print('yes' if any(p.get('name','').endswith('/$LAKEBASE_PROJECT') for p in ps) else 'no')" 2>/dev/null || echo "no")

  if [ "$EXISTING" = "no" ]; then
    echo "  Creating Lakebase project: $LAKEBASE_PROJECT"
    databricks postgres create-project "$LAKEBASE_PROJECT" \
      --json '{"spec": {"display_name": "Traffic Demo"}}' \
      --profile "$PROFILE" --no-wait 2>/dev/null

    echo "  Waiting for endpoint to be ready..."
    sleep 60
  else
    echo "  Lakebase project already exists"
  fi

  # Get endpoint host
  LB_HOST=$(databricks postgres list-endpoints "projects/$LAKEBASE_PROJECT/branches/production" \
    --profile "$PROFILE" --output json 2>/dev/null | \
    python3 -c "import json,sys; eps=json.load(sys.stdin); print(eps[0]['status']['hosts']['host'] if eps else '')" 2>/dev/null || true)
  echo "  Lakebase host: $LB_HOST"

  if [ -n "$LB_HOST" ]; then
    # Generate credential and create database + table
    LB_TOKEN=$(databricks postgres generate-database-credential \
      "projects/$LAKEBASE_PROJECT/branches/production/endpoints/primary" \
      --profile "$PROFILE" --output json 2>/dev/null | \
      python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")

    LB_USER=$(databricks current-user me --profile "$PROFILE" --output json 2>/dev/null | \
      python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")

    echo "  Creating database and tables..."
    PGPASSWORD=$LB_TOKEN psql "host=$LB_HOST port=5432 dbname=postgres user=$LB_USER sslmode=require" \
      -c "CREATE DATABASE traffic;" 2>/dev/null || true

    PGPASSWORD=$LB_TOKEN psql "host=$LB_HOST port=5432 dbname=traffic user=$LB_USER sslmode=require" -c "
    CREATE SCHEMA IF NOT EXISTS realtime;
    CREATE TABLE IF NOT EXISTS realtime.gold_current_traffic (
        site_id VARCHAR(20), site_name VARCHAR(200), report_date VARCHAR(20),
        time_period_end VARCHAR(10), reading_ts TIMESTAMP, total_volume INT,
        avg_speed DOUBLE PRECISION, link_length_km DOUBLE PRECISION,
        latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
        h3_index_res10 VARCHAR(30), h3_index_res7 VARCHAR(30),
        road_name VARCHAR(20), direction VARCHAR(30), trace_id VARCHAR(50),
        ingest_method VARCHAR(20), ingestion_ts TIMESTAMP, processed_ts TIMESTAMP
    );
    " 2>/dev/null
    echo "  Lakebase tables created"
  fi
else
  echo ""
  echo "--- Step 4: Skipping Lakebase ---"
fi

# --- Step 5: Create Zerobus service principal ---
echo ""
echo "--- Step 5: Create Zerobus service principal ---"

SP_INFO=$(python3 -c "
from databricks.sdk import WorkspaceClient
import json, subprocess
w = WorkspaceClient(profile='$PROFILE')

# Create SP
sp = w.service_principals.create(display_name='nh-traffic-zerobus')

# Create OAuth secret via REST
import requests
token = subprocess.run(['databricks', 'auth', 'token', '--profile', '$PROFILE'],
    capture_output=True, text=True).stdout
token = json.loads(token)['access_token']
host = w.config.host.rstrip('/')

resp = requests.post(f'{host}/api/2.0/accounts/servicePrincipals/{sp.id}/credentials/secrets',
    headers={'Authorization': f'Bearer {token}'})
secret_data = resp.json()

# Grant permissions
WH = '$WH_ID'
for sql in [
    f'GRANT USE CATALOG ON CATALOG $CATALOG TO \`{sp.application_id}\`',
    f'GRANT USE SCHEMA ON SCHEMA $CATALOG.$SCHEMA TO \`{sp.application_id}\`',
    f'GRANT MODIFY ON TABLE $CATALOG.$SCHEMA.zerobus_traffic_ingest TO \`{sp.application_id}\`',
    f'GRANT SELECT ON TABLE $CATALOG.$SCHEMA.zerobus_traffic_ingest TO \`{sp.application_id}\`',
]:
    w.statement_execution.execute_statement(warehouse_id=WH, statement=sql, wait_timeout='30s')

print(json.dumps({
    'client_id': sp.application_id,
    'client_secret': secret_data.get('secret', ''),
}))
" 2>/dev/null || echo '{"client_id":"","client_secret":""}')

ZB_CLIENT_ID=$(echo "$SP_INFO" | python3 -c "import json,sys; print(json.load(sys.stdin)['client_id'])")
ZB_CLIENT_SECRET=$(echo "$SP_INFO" | python3 -c "import json,sys; print(json.load(sys.stdin)['client_secret'])")
echo "  Zerobus SP Client ID: $ZB_CLIENT_ID"

# --- Step 6: Deploy bundle ---
echo ""
echo "--- Step 6: Deploy Databricks Asset Bundle ---"
databricks bundle deploy --target azure --profile "$PROFILE"

# --- Step 7: Create and deploy app ---
echo ""
echo "--- Step 7: Deploy Databricks App ---"

databricks apps create --json '{"name": "nh-traffic-api", "description": "Traffic Real-Time Pipeline Demo"}' \
  --profile "$PROFILE" --no-wait 2>/dev/null || true

echo "  Waiting for app compute..."
sleep 60

# Grant app SP permissions
APP_SP=$(databricks apps get nh-traffic-api --profile "$PROFILE" --output json 2>/dev/null | \
  python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('service_principal_client_id',''))")

python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile='$PROFILE')
for sql in [
    f'GRANT USE CATALOG ON CATALOG $CATALOG TO \`$APP_SP\`',
    f'GRANT USE SCHEMA ON SCHEMA $CATALOG.$SCHEMA TO \`$APP_SP\`',
    f'GRANT ALL PRIVILEGES ON SCHEMA $CATALOG.$SCHEMA TO \`$APP_SP\`',
]:
    w.statement_execution.execute_statement(warehouse_id='$WH_ID', statement=sql, wait_timeout='30s')
print('  App SP permissions granted')
" 2>/dev/null

# Deploy app code
databricks apps deploy nh-traffic-api --profile "$PROFILE" \
  --source-code-path "/Workspace/Users/$(databricks current-user me --profile $PROFILE --output json | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")/.bundle/nh_realtime_dataflows/azure/files/app"

# --- Step 8: Load reference data ---
echo ""
echo "--- Step 8: Load reference data ---"
REF_JOB=$(databricks jobs list --profile "$PROFILE" --output json | \
  python3 -c "import json,sys; jobs=json.load(sys.stdin); [print(j['job_id']) for j in jobs if 'reference-ingest' in j['settings']['name']]" | head -1)
if [ -n "$REF_JOB" ]; then
  databricks jobs run-now "$REF_JOB" --profile "$PROFILE" --no-wait
  echo "  Reference loader started (job $REF_JOB)"
fi

# --- Step 9: Start streaming jobs ---
echo ""
echo "--- Step 9: Start streaming jobs ---"
for name in "eventhub-streaming" "rtm-streaming" "medallion-streaming"; do
  JOB_ID=$(databricks jobs list --profile "$PROFILE" --output json | \
    python3 -c "import json,sys; jobs=json.load(sys.stdin); [print(j['job_id']) for j in jobs if '$name' in j['settings']['name']]" | head -1)
  if [ -n "$JOB_ID" ]; then
    databricks jobs run-now "$JOB_ID" --profile "$PROFILE" --no-wait 2>/dev/null || true
    echo "  Started $name (job $JOB_ID)"
  fi
done

# --- Summary ---
echo ""
echo "==========================================="
echo "  Deployment Complete!"
echo "==========================================="
echo ""
echo "App URL: $(databricks apps get nh-traffic-api --profile $PROFILE --output json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('url',''))")"
echo ""
echo "Environment variables to set in app.yaml:"
echo "  DATABRICKS_SQL_WAREHOUSE_ID=$WH_ID"
echo "  EVENTHUB_SEND_CONN_STR=$EH_SEND_CONN"
echo "  ZEROBUS_CLIENT_ID=$ZB_CLIENT_ID"
echo "  ZEROBUS_CLIENT_SECRET=$ZB_CLIENT_SECRET"
echo "  LAKEBASE_HOST=${LB_HOST:-not configured}"
echo ""
echo "Next steps:"
echo "  1. Update app/app-azure.yaml with the values above"
echo "  2. Redeploy: databricks apps deploy nh-traffic-api --profile $PROFILE ..."
echo "  3. Wait for reference data to load (~5 min)"
echo "  4. Open the app URL and test all four approaches"
