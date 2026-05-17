#!/bin/bash
# Sets up the Atlas Database Trigger via the App Services Admin API.
# Uses Atlas programmatic API keys for authentication (no session expiry).
#
# Prerequisites:
#   - Atlas API Key with Project Owner role on the target project
#   - Create at: Atlas UI > Access Manager > API Keys
#
# Required env vars:
#   ATLAS_PUBLIC_KEY    - Atlas API public key
#   ATLAS_PRIVATE_KEY   - Atlas API private key
#   ATLAS_PROJECT_ID    - Atlas project ID (default: 6775fae8bc0e793431ff8dd0)
#   MLFLOW_ENDPOINT     - MLflow inference URL (auto-detected from CloudFormation if not set)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ATLAS_PROJECT_ID="${ATLAS_PROJECT_ID:-6775fae8bc0e793431ff8dd0}"
ATLAS_CLUSTER_NAME="${ATLAS_CLUSTER_NAME:-DemoCluster}"
APP_SERVICES_BASE="https://services.cloud.mongodb.com/api/admin/v3.0"

echo "============================================================"
echo "Telco ODS - Atlas Trigger Setup"
echo "============================================================"
echo ""

# Check required vars
if [ -z "$ATLAS_PUBLIC_KEY" ] || [ -z "$ATLAS_PRIVATE_KEY" ]; then
  echo "Error: Set ATLAS_PUBLIC_KEY and ATLAS_PRIVATE_KEY"
  echo ""
  echo "  export ATLAS_PUBLIC_KEY=<your-public-key>"
  echo "  export ATLAS_PRIVATE_KEY=<your-private-key>"
  echo ""
  echo "Create an API key at: Atlas UI > Access Manager > API Keys"
  echo "Required role: Project Owner on project $ATLAS_PROJECT_ID"
  echo ""
  echo "--- MANUAL SETUP (alternative) ---"
  echo "1. Atlas UI > App Services > Create Application (name: TelcoODS)"
  echo "2. Linked Data Source: ${ATLAS_CLUSTER_NAME}"
  echo "3. Triggers > Add Trigger:"
  echo "   - Name: network_health_inference"
  echo "   - Type: Database"
  echo "   - Cluster: ${ATLAS_CLUSTER_NAME}"
  echo "   - Database: ods_demo_db"
  echo "   - Collection: windowed_network_metrics"
  echo "   - Operation: Insert"
  echo "   - Full Document: ON"
  echo "   - Function: paste contents of trigger_function.js"
  echo "4. Values > Add Value:"
  echo "   - Name: MLFLOW_ENDPOINT"
  echo "   - Value: ${MLFLOW_ENDPOINT:-http://<mlflow-ip>:5003/invocations}"
  exit 1
fi

# Auto-detect MLflow IP from CloudFormation if not set
if [ -z "$MLFLOW_ENDPOINT" ]; then
  MLFLOW_IP=$(aws cloudformation describe-stacks \
    --stack-name telco-ods-demo \
    --region ap-southeast-2 \
    --query "Stacks[0].Outputs[?OutputKey=='MLflowPublicIP'].OutputValue" \
    --output text 2>/dev/null)
  if [ -n "$MLFLOW_IP" ] && [ "$MLFLOW_IP" != "None" ]; then
    MLFLOW_ENDPOINT="http://${MLFLOW_IP}:5003/invocations"
  else
    echo "Error: Cannot determine MLflow endpoint. Set MLFLOW_ENDPOINT env var."
    exit 1
  fi
fi

echo "  Project ID: $ATLAS_PROJECT_ID"
echo "  Cluster:    $ATLAS_CLUSTER_NAME"
echo "  MLflow:     $MLFLOW_ENDPOINT"
echo ""

# Step 1: Authenticate to App Services Admin API
echo "[1/5] Authenticating to App Services Admin API..."
AUTH_RESPONSE=$(curl -s -X POST "${APP_SERVICES_BASE}/auth/providers/mongodb-cloud/login" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"${ATLAS_PUBLIC_KEY}\",
    \"password\": \"${ATLAS_PRIVATE_KEY}\"
  }")

ACCESS_TOKEN=$(echo "$AUTH_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)

if [ -z "$ACCESS_TOKEN" ]; then
  echo "  Error: Authentication failed."
  echo "  Response: $AUTH_RESPONSE"
  exit 1
fi
echo "  Authenticated successfully."

AUTH_HEADER="Authorization: Bearer ${ACCESS_TOKEN}"

# Step 2: Check for existing app or create one
echo "[2/5] Setting up App Services application..."
EXISTING_APPS=$(curl -s -H "$AUTH_HEADER" \
  "${APP_SERVICES_BASE}/groups/${ATLAS_PROJECT_ID}/apps")

APP_ID=$(echo "$EXISTING_APPS" | python3 -c "
import sys, json
apps = json.load(sys.stdin)
for app in apps:
    if 'TelcoODS' in app.get('name', '') or 'telco' in app.get('name', '').lower():
        print(app['_id'])
        break
" 2>/dev/null)

if [ -z "$APP_ID" ]; then
  echo "  Creating new App Services app: TelcoODS..."
  CREATE_RESPONSE=$(curl -s -X POST -H "$AUTH_HEADER" \
    -H "Content-Type: application/json" \
    "${APP_SERVICES_BASE}/groups/${ATLAS_PROJECT_ID}/apps" \
    -d "{
      \"name\": \"TelcoODS\",
      \"data_source\": {
        \"name\": \"mongodb-atlas\",
        \"type\": \"mongodb-atlas\",
        \"config\": {
          \"clusterName\": \"${ATLAS_CLUSTER_NAME}\"
        }
      }
    }")
  APP_ID=$(echo "$CREATE_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('_id',''))" 2>/dev/null)
  if [ -z "$APP_ID" ]; then
    echo "  Error creating app: $CREATE_RESPONSE"
    exit 1
  fi
  echo "  Created app: $APP_ID"
else
  echo "  Using existing app: $APP_ID"
fi

APP_BASE="${APP_SERVICES_BASE}/groups/${ATLAS_PROJECT_ID}/apps/${APP_ID}"

# Step 3: Create the MLFLOW_ENDPOINT value
echo "[3/5] Creating MLFLOW_ENDPOINT value..."
# Delete existing value if present
EXISTING_VALUES=$(curl -s -H "$AUTH_HEADER" "${APP_BASE}/values")
EXISTING_VALUE_ID=$(echo "$EXISTING_VALUES" | python3 -c "
import sys, json
values = json.load(sys.stdin)
for v in values:
    if v.get('name') == 'MLFLOW_ENDPOINT':
        print(v['_id'])
        break
" 2>/dev/null)

if [ -n "$EXISTING_VALUE_ID" ]; then
  curl -s -X DELETE -H "$AUTH_HEADER" "${APP_BASE}/values/${EXISTING_VALUE_ID}" > /dev/null
fi

curl -s -X POST -H "$AUTH_HEADER" -H "Content-Type: application/json" \
  "${APP_BASE}/values" \
  -d "{
    \"name\": \"MLFLOW_ENDPOINT\",
    \"value\": \"${MLFLOW_ENDPOINT}\",
    \"private\": false
  }" > /dev/null
echo "  Set MLFLOW_ENDPOINT = ${MLFLOW_ENDPOINT}"

# Step 4: Create the trigger function
echo "[4/5] Creating trigger function..."
FUNCTION_SOURCE=$(cat "$SCRIPT_DIR/trigger_function.js")
FUNCTION_SOURCE_ESCAPED=$(python3 -c "import json,sys; print(json.dumps(open(sys.argv[1]).read()))" "$SCRIPT_DIR/trigger_function.js")

# Check if function exists
EXISTING_FUNCTIONS=$(curl -s -H "$AUTH_HEADER" "${APP_BASE}/functions")
EXISTING_FUNC_ID=$(echo "$EXISTING_FUNCTIONS" | python3 -c "
import sys, json
funcs = json.load(sys.stdin)
for f in funcs:
    if f.get('name') == 'networkHealthInference':
        print(f['_id'])
        break
" 2>/dev/null)

if [ -n "$EXISTING_FUNC_ID" ]; then
  # Update existing
  curl -s -X PUT -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    "${APP_BASE}/functions/${EXISTING_FUNC_ID}" \
    -d "{
      \"name\": \"networkHealthInference\",
      \"private\": false,
      \"source\": ${FUNCTION_SOURCE_ESCAPED}
    }" > /dev/null
  FUNC_ID="$EXISTING_FUNC_ID"
else
  # Create new
  FUNC_RESPONSE=$(curl -s -X POST -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    "${APP_BASE}/functions" \
    -d "{
      \"name\": \"networkHealthInference\",
      \"private\": false,
      \"source\": ${FUNCTION_SOURCE_ESCAPED}
    }")
  FUNC_ID=$(echo "$FUNC_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('_id',''))" 2>/dev/null)
fi

if [ -z "$FUNC_ID" ]; then
  echo "  Error: Could not create/update function"
  exit 1
fi
echo "  Function ready: networkHealthInference ($FUNC_ID)"

# Step 5: Create the database trigger
echo "[5/5] Creating database trigger..."
# Check for existing trigger
EXISTING_TRIGGERS=$(curl -s -H "$AUTH_HEADER" "${APP_BASE}/triggers")
EXISTING_TRIGGER_ID=$(echo "$EXISTING_TRIGGERS" | python3 -c "
import sys, json
triggers = json.load(sys.stdin)
for t in triggers:
    if t.get('name') == 'network_health_inference':
        print(t['_id'])
        break
" 2>/dev/null)

TRIGGER_CONFIG="{
  \"name\": \"network_health_inference\",
  \"type\": \"DATABASE\",
  \"config\": {
    \"operation_types\": [\"INSERT\"],
    \"database\": \"ods_demo_db\",
    \"collection\": \"windowed_network_metrics\",
    \"service_id\": \"\",
    \"match\": {},
    \"full_document\": true,
    \"full_document_before_change\": false,
    \"unordered\": false
  },
  \"function_id\": \"${FUNC_ID}\",
  \"disabled\": false
}"

if [ -n "$EXISTING_TRIGGER_ID" ]; then
  curl -s -X PUT -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    "${APP_BASE}/triggers/${EXISTING_TRIGGER_ID}" \
    -d "$TRIGGER_CONFIG" > /dev/null
  echo "  Updated trigger: network_health_inference"
else
  TRIGGER_RESPONSE=$(curl -s -X POST -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    "${APP_BASE}/triggers" \
    -d "$TRIGGER_CONFIG")
  echo "  Created trigger: network_health_inference"
fi

echo ""
echo "============================================================"
echo "TRIGGER SETUP COMPLETE"
echo "============================================================"
echo ""
echo "The trigger will fire on every insert to:"
echo "  ods_demo_db.windowed_network_metrics"
echo ""
echo "It calls MLflow at:"
echo "  $MLFLOW_ENDPOINT"
echo ""
echo "Predictions written to:"
echo "  ods_demo_db.network_health_predictions"
echo ""
echo "Verify: Atlas UI > App Services > TelcoODS > Triggers"
